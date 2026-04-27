#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bilibili 每周必看 采集器（单文件版）

用法：
  export BILI_COOKIE="SESSDATA=xxx; bili_jct=yyy; buvid3=zzz; ..."

  python crawler.py init                       # 拉取全部期号 + 每期视频列表
  python crawler.py init --week 250            # 只拉第 250 期
  python crawler.py init --from 200 --to 250   # 拉第 200~250 期
  python crawler.py run                        # 采集评论+弹幕（断点续传）
  python crawler.py status                     # 查看进度
  python crawler.py retry                      # 重置失败任务

边跑边查（另开终端）：
  sqlite3 bili.db "SELECT COUNT(*) FROM comments;"
  sqlite3 bili.db "SELECT week_no, COUNT(*) FROM videos GROUP BY week_no;"
"""

import argparse, json, logging, os, random, signal, sqlite3, sys, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests


# ============================================================
# 配置
# ============================================================

@dataclass
class Config:
    db_path: str = os.environ.get("BILI_DB", "bili.db")
    cookie: str = os.environ.get("BILI_COOKIE", "")
    user_agent: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36")
    qps: float = float(os.environ.get("BILI_QPS", "1.5"))
    burst: int = int(os.environ.get("BILI_BURST", "3"))
    workers: int = int(os.environ.get("BILI_WORKERS", "2"))
    http_timeout: int = 20
    http_retries: int = 3
    cooldown_352: int = 600
    cooldown_412: int = 300
    cooldown_max: int = 3600
    log_file: str = os.environ.get("BILI_LOG", "bili.log")

CFG = Config()


# ============================================================
# 日志 & 优雅退出
# ============================================================

def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("bili")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stderr); sh.setFormatter(fmt); logger.addHandler(sh)
    if CFG.log_file:
        fh = logging.FileHandler(CFG.log_file, encoding="utf-8"); fh.setFormatter(fmt); logger.addHandler(fh)
    return logger

log = _setup_logging()
STOP = threading.Event()

def _on_signal(sig, frame):
    if STOP.is_set():
        log.warning("再次收到 %s，强制退出", sig); os._exit(1)
    log.warning("收到信号 %s，准备优雅退出（再按一次强退）", sig)
    STOP.set()

signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


# ============================================================
# WBI 签名
# ============================================================

_MIXIN_TAB = [
    46,47,18,2,53,8,23,32,15,50,10,31,58,3,45,35,
    27,43,5,49,33,9,42,19,29,28,14,39,12,38,41,13,
    37,48,7,16,24,55,40,61,26,17,0,1,60,51,30,4,
    22,25,54,21,56,59,6,63,57,62,11,36,20,34,44,52,
]

class WBI:
    def __init__(self, sess: "Session"):
        self.s = sess
        self._key: Optional[str] = None
        self._key_ts: float = 0.0
        self._lock = threading.Lock()

    def _refresh_locked(self):
        if self._key and time.time() - self._key_ts < 3600:
            return
        r = self.s.raw_get("https://api.bilibili.com/x/web-interface/nav")
        data = r.json()
        wbi = (data.get("data") or {}).get("wbi_img") or {}
        img = wbi.get("img_url", "").rsplit("/", 1)[-1].split(".")[0]
        sub = wbi.get("sub_url", "").rsplit("/", 1)[-1].split(".")[0]
        if not img or not sub:
            raise RuntimeError(f"WBI key 获取失败：{data}")
        raw = img + sub
        self._key = "".join(raw[i] for i in _MIXIN_TAB)[:32]
        self._key_ts = time.time()
        log.info("WBI key 已刷新")

    def sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self._refresh_locked()
            key = self._key
        params = dict(params)
        params["wts"] = int(time.time())
        clean = {}
        for k, v in params.items():
            sv = str(v)
            for c in "!'()*":
                sv = sv.replace(c, "")
            clean[k] = sv
        q = urlencode(sorted(clean.items()))
        params["w_rid"] = md5((q + key).encode()).hexdigest()
        return params


# ============================================================
# 令牌桶 + 风控熔断
# ============================================================

class RateLimiter:
    def __init__(self, qps: float, burst: int):
        self.qps = qps; self.burst = burst
        self.tokens = float(burst); self.last = time.time()
        self.lock = threading.Lock()
        self.blocked_until: float = 0.0
        self.consecutive_risks = 0

    def acquire(self):
        while True:
            if STOP.is_set():
                raise KeyboardInterrupt()
            with self.lock:
                now = time.time()
                if now < self.blocked_until:
                    wait = self.blocked_until - now
                else:
                    self.tokens = min(self.burst, self.tokens + (now - self.last) * self.qps)
                    self.last = now
                    if self.tokens >= 1:
                        self.tokens -= 1; return
                    wait = (1 - self.tokens) / self.qps
            STOP.wait(min(wait, 5.0))

    def on_success(self):
        with self.lock:
            if self.consecutive_risks > 0:
                self.consecutive_risks -= 1

    def on_risk(self, code: int):
        with self.lock:
            self.consecutive_risks += 1
            base = CFG.cooldown_352 if code == -352 else CFG.cooldown_412
            cool = min(CFG.cooldown_max, base * (2 ** (self.consecutive_risks - 1)))
            until = time.time() + cool
            if until > self.blocked_until:
                self.blocked_until = until
            log.warning("风控触发 code=%s，熔断 %ds（连续 %d 次）",
                        code, cool, self.consecutive_risks)


# ============================================================
# HTTP Session
# ============================================================

class Session:
    RISK_CODES = {-352, -412, -509, -799}
    AUTH_CODES = {-101, -111}

    def __init__(self, limiter: RateLimiter):
        self.limiter = limiter
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": CFG.user_agent,
            "Referer": "https://www.bilibili.com",
            "Accept": "application/json, text/plain, */*",
        })
        if CFG.cookie:
            self.s.headers["Cookie"] = CFG.cookie
        else:
            log.warning("未设置 BILI_COOKIE，部分接口会返回 -101")

    def raw_get(self, url: str, params=None) -> requests.Response:
        return self.s.get(url, params=params, timeout=CFG.http_timeout)

    def _do(self, url: str, params: Dict[str, Any], want_bytes: bool):
        last_exc: Optional[Exception] = None
        for attempt in range(CFG.http_retries + 1):
            self.limiter.acquire()
            try:
                r = self.s.get(url, params=params, timeout=CFG.http_timeout)
                if r.status_code == 412:
                    self.limiter.on_risk(-412); continue
                if r.status_code in (429, 503):
                    self.limiter.on_risk(-352); continue
                r.raise_for_status()

                if want_bytes:
                    self.limiter.on_success(); return r.content

                data = r.json()
                code = data.get("code", 0)
                if code == 0:
                    self.limiter.on_success(); return data
                if code in self.RISK_CODES:
                    self.limiter.on_risk(code); continue
                if code in self.AUTH_CODES:
                    raise RuntimeError(f"Cookie 失效或未登录 (code={code})：{data.get('message')}")
                self.limiter.on_success(); return data
            except requests.RequestException as e:
                last_exc = e
                wait = 2 ** attempt + random.random()
                log.warning("HTTP 错误 %s（重试 %d/%d，%.1fs 后）", e, attempt + 1, CFG.http_retries, wait)
                STOP.wait(wait)
        raise RuntimeError(f"请求失败超过重试上限：{url}（最后错误：{last_exc}）")

    def get_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._do(url, params, want_bytes=False)

    def get_bytes(self, url: str, params: Dict[str, Any]) -> bytes:
        return self._do(url, params, want_bytes=True)


# ============================================================
# 数据库
# ============================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS weeks (
    number      INTEGER PRIMARY KEY,
    name        TEXT,
    subject     TEXT,
    status      INTEGER,
    list_done   INTEGER DEFAULT 0,
    updated_at  INTEGER
);

CREATE TABLE IF NOT EXISTS videos (
    bvid                TEXT PRIMARY KEY,
    aid                 INTEGER,
    cid                 INTEGER,
    week_no             INTEGER,
    title               TEXT,
    owner_mid           INTEGER,
    owner_name          TEXT,
    pubdate             INTEGER,
    duration            INTEGER,
    view                INTEGER,
    danmaku             INTEGER,
    reply               INTEGER,
    like                INTEGER,
    coin                INTEGER,
    favorite            INTEGER,
    share               INTEGER,
    rcmd_reason         TEXT,
    meta_done           INTEGER DEFAULT 1,   -- 每周必看接口已含 cid，默认 1
    comment_cursor      TEXT,
    danmaku_done        INTEGER DEFAULT 0,
    danmaku_total_seg   INTEGER,
    status              TEXT DEFAULT 'pending',
    last_error          TEXT,
    updated_at          INTEGER
);
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_week   ON videos(week_no);

CREATE TABLE IF NOT EXISTS comments (
    rpid     INTEGER PRIMARY KEY,
    bvid     TEXT NOT NULL,
    parent   INTEGER DEFAULT 0,
    root     INTEGER DEFAULT 0,
    mid      INTEGER,
    uname    TEXT,
    ctime    INTEGER,
    likes    INTEGER,
    content  TEXT,
    raw      TEXT
);
CREATE INDEX IF NOT EXISTS idx_comments_bvid ON comments(bvid);
CREATE INDEX IF NOT EXISTS idx_comments_root ON comments(root);

CREATE TABLE IF NOT EXISTS danmaku (
    id         INTEGER PRIMARY KEY,
    bvid       TEXT NOT NULL,
    cid        INTEGER NOT NULL,
    progress   INTEGER,
    mode       INTEGER,
    fontsize   INTEGER,
    color      INTEGER,
    mid_hash   TEXT,
    content    TEXT,
    ctime      INTEGER,
    pool       INTEGER,
    weight     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_danmaku_bvid ON danmaku(bvid);
"""


class DB:
    def __init__(self, path: str):
        self.path = path
        self.local = threading.local()
        c = self.connect()
        c.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        c = getattr(self.local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=60, isolation_level=None,
                                check_same_thread=False)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL;")
            c.execute("PRAGMA synchronous=NORMAL;")
            c.execute("PRAGMA temp_store=MEMORY;")
            c.execute("PRAGMA mmap_size=268435456;")
            c.execute("PRAGMA busy_timeout=30000;")
            self.local.conn = c
        return c

    @contextmanager
    def tx(self):
        c = self.connect()
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK"); raise

    # ---- 期号 ----
    def upsert_weeks(self, rows: List[Dict[str, Any]]):
        if not rows: return
        now = int(time.time())
        with self.tx() as c:
            for r in rows:
                c.execute("""
                    INSERT INTO weeks(number, name, subject, status, updated_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(number) DO UPDATE SET
                        name=excluded.name, subject=excluded.subject,
                        status=excluded.status, updated_at=excluded.updated_at
                """, (r["number"], r.get("name"), r.get("subject"),
                      r.get("status", 0), now))

    def mark_week_done(self, number: int):
        with self.tx() as c:
            c.execute("UPDATE weeks SET list_done=1, updated_at=? WHERE number=?",
                      (int(time.time()), number))

    def list_pending_weeks(self, only: Optional[List[int]] = None) -> List[sqlite3.Row]:
        c = self.connect()
        if only:
            qs = ",".join("?" * len(only))
            return list(c.execute(f"SELECT * FROM weeks WHERE number IN ({qs}) AND list_done=0", only))
        return list(c.execute("SELECT * FROM weeks WHERE list_done=0 ORDER BY number"))

    # ---- 视频 ----
    def upsert_week_videos(self, week_no: int, vids: List[Dict[str, Any]]):
        if not vids: return
        now = int(time.time())
        with self.tx() as c:
            for v in vids:
                stat = v.get("stat") or {}
                owner = v.get("owner") or {}
                c.execute("""
                    INSERT INTO videos(bvid, aid, cid, week_no, title,
                        owner_mid, owner_name, pubdate, duration,
                        view, danmaku, reply, like, coin, favorite, share,
                        rcmd_reason, updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(bvid) DO UPDATE SET
                        cid=excluded.cid,
                        week_no=COALESCE(videos.week_no, excluded.week_no),
                        view=excluded.view, danmaku=excluded.danmaku,
                        reply=excluded.reply, like=excluded.like,
                        updated_at=excluded.updated_at
                """, (
                    v["bvid"], v.get("aid"), v.get("cid"), week_no, v.get("title"),
                    owner.get("mid"), owner.get("name"),
                    v.get("pubdate"), v.get("duration"),
                    stat.get("view", 0), stat.get("danmaku", 0),
                    stat.get("reply", 0), stat.get("like", 0),
                    stat.get("coin", 0), stat.get("favorite", 0), stat.get("share", 0),
                    (v.get("rcmd_reason") or {}).get("content") if isinstance(v.get("rcmd_reason"), dict) else None,
                    now,
                ))
                # 计算弹幕段数
                dur = v.get("duration") or 0
                total_seg = max(1, (dur + 359) // 360)
                c.execute("UPDATE videos SET danmaku_total_seg=? WHERE bvid=? AND danmaku_total_seg IS NULL",
                          (total_seg, v["bvid"]))

    def update_video(self, bvid: str, **fields):
        if not fields: return
        fields["updated_at"] = int(time.time())
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [bvid]
        with self.tx() as c:
            c.execute(f"UPDATE videos SET {cols} WHERE bvid=?", vals)

    def list_pending(self) -> List[sqlite3.Row]:
        c = self.connect()
        return list(c.execute(
            "SELECT * FROM videos WHERE status NOT IN ('done') ORDER BY week_no DESC, bvid"))

    def get_video(self, bvid: str) -> Optional[sqlite3.Row]:
        c = self.connect()
        return c.execute("SELECT * FROM videos WHERE bvid=?", (bvid,)).fetchone()

    def insert_comments(self, rows: List[Tuple]):
        if not rows: return
        with self.tx() as c:
            c.executemany("""INSERT OR IGNORE INTO comments
                (rpid, bvid, parent, root, mid, uname, ctime, likes, content, raw)
                VALUES (?,?,?,?,?,?,?,?,?,?)""", rows)

    def insert_danmaku(self, rows: List[Tuple]):
        if not rows: return
        with self.tx() as c:
            c.executemany("""INSERT OR IGNORE INTO danmaku
                (id, bvid, cid, progress, mode, fontsize, color, mid_hash, content, ctime, pool, weight)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

    def stats(self) -> Dict[str, int]:
        c = self.connect()
        return {
            "weeks_total":   c.execute("SELECT COUNT(*) FROM weeks").fetchone()[0],
            "weeks_done":    c.execute("SELECT COUNT(*) FROM weeks WHERE list_done=1").fetchone()[0],
            "videos_total":  c.execute("SELECT COUNT(*) FROM videos").fetchone()[0],
            "videos_done":   c.execute("SELECT COUNT(*) FROM videos WHERE status='done'").fetchone()[0],
            "videos_failed": c.execute("SELECT COUNT(*) FROM videos WHERE status='failed'").fetchone()[0],
            "videos_running":c.execute("SELECT COUNT(*) FROM videos WHERE status='running'").fetchone()[0],
            "comments":      c.execute("SELECT COUNT(*) FROM comments").fetchone()[0],
            "danmaku":       c.execute("SELECT COUNT(*) FROM danmaku").fetchone()[0],
        }


# ============================================================
# 弹幕 protobuf 解析
# ============================================================

def _read_varint(buf: bytes, i: int) -> Tuple[int, int]:
    n = 0; shift = 0
    while True:
        b = buf[i]; i += 1
        n |= (b & 0x7F) << shift
        if not (b & 0x80): return n, i
        shift += 7

def _parse_dm_elem(buf: bytes) -> Dict[str, Any]:
    out: Dict[str, Any] = {}; i = 0; n = len(buf)
    while i < n:
        tag, i = _read_varint(buf, i)
        field_no = tag >> 3; wt = tag & 7
        if wt == 0:
            v, i = _read_varint(buf, i)
            if   field_no == 1:  out["id"] = v
            elif field_no == 2:  out["progress"] = v
            elif field_no == 3:  out["mode"] = v
            elif field_no == 4:  out["fontsize"] = v
            elif field_no == 5:  out["color"] = v
            elif field_no == 8:  out["ctime"] = v
            elif field_no == 9:  out["weight"] = v
            elif field_no == 11: out["pool"] = v
            elif field_no == 13: out["attr"] = v
        elif wt == 2:
            ln, i = _read_varint(buf, i); chunk = buf[i:i+ln]; i += ln
            if   field_no == 6:  out["mid_hash"] = chunk.decode("utf-8", "replace")
            elif field_no == 7:  out["content"]  = chunk.decode("utf-8", "replace")
            elif field_no == 10: out["action"]   = chunk.decode("utf-8", "replace")
            elif field_no == 12: out["id_str"]   = chunk.decode("utf-8", "replace")
        elif wt == 1: i += 8
        elif wt == 5: i += 4
        else: break
    return out

def parse_dm_seg(buf: bytes) -> List[Dict[str, Any]]:
    if not buf: return []
    elems: List[Dict[str, Any]] = []; i = 0; n = len(buf)
    while i < n:
        tag, i = _read_varint(buf, i)
        field_no = tag >> 3; wt = tag & 7
        if field_no == 1 and wt == 2:
            ln, i = _read_varint(buf, i)
            elems.append(_parse_dm_elem(buf[i:i+ln])); i += ln
        elif wt == 0: _, i = _read_varint(buf, i)
        elif wt == 2: ln, i = _read_varint(buf, i); i += ln
        elif wt == 1: i += 8
        elif wt == 5: i += 4
        else: break
    return elems


# ============================================================
# B 站 API（每周必看）
# ============================================================

class BiliAPI:
    def __init__(self, sess: Session, wbi: WBI):
        self.s = sess; self.wbi = wbi

    def list_all_weeks(self) -> List[Dict[str, Any]]:
        """获取所有期号列表"""
        data = self.s.get_json(
            "https://api.bilibili.com/x/web-interface/popular/series/list", {})
        if data.get("code") != 0:
            raise RuntimeError(f"期号列表失败：{data}")
        return (data.get("data") or {}).get("list") or []

    def get_week(self, number: int) -> Dict[str, Any]:
        """获取某一期内容（含视频列表，每条已含 cid）"""
        data = self.s.get_json(
            "https://api.bilibili.com/x/web-interface/popular/series/one",
            {"number": number})
        if data.get("code") != 0:
            raise RuntimeError(f"第 {number} 期失败：{data}")
        return data["data"]

    def comments_main(self, oid: int, pagination: Dict[str, Any]) -> Dict[str, Any]:
        params = {
            "oid": oid, "type": 1, "mode": 3,
            "pagination_str": json.dumps(pagination, separators=(",", ":")),
            "plat": 1, "web_location": "1315875",
        }
        params = self.wbi.sign(params)
        return self.s.get_json("https://api.bilibili.com/x/v2/reply/wbi/main", params)

    def comment_replies(self, oid: int, root: int, pn: int) -> Dict[str, Any]:
        return self.s.get_json(
            "https://api.bilibili.com/x/v2/reply/reply",
            {"oid": oid, "type": 1, "root": root, "ps": 20, "pn": pn})

    def danmaku_seg(self, cid: int, aid: int, segment_index: int) -> bytes:
        return self.s.get_bytes(
            "https://api.bilibili.com/x/v2/dm/web/seg.so",
            {"type": 1, "oid": cid, "pid": aid, "segment_index": segment_index})


# ============================================================
# 采集主流程
# ============================================================

class Crawler:
    def __init__(self, db: DB, api: BiliAPI):
        self.db = db; self.api = api

    # ---- 期号 + 视频列表 ----
    def init_weeks(self, only: Optional[List[int]] = None):
        log.info("拉取每周必看期号列表...")
        weeks = self.api.list_all_weeks()
        log.info("共 %d 期", len(weeks))
        self.db.upsert_weeks(weeks)

        pending = self.db.list_pending_weeks(only=only)
        log.info("本次需要拉取 %d 期视频列表", len(pending))
        for w in pending:
            if STOP.is_set(): break
            number = w["number"]
            try:
                d = self.api.get_week(number)
                vids = d.get("list") or []
                self.db.upsert_week_videos(number, vids)
                self.db.mark_week_done(number)
                log.info("✓ 第 %d 期：%s（%d 个视频）",
                         number, (d.get("config") or {}).get("name", ""), len(vids))
            except Exception as e:
                log.exception("✗ 第 %d 期失败：%s", number, e)

    # ---- 单视频 ----
    def crawl_one(self, bvid: str):
        v = self.db.get_video(bvid)
        if v is None or v["status"] == "done": return
        try:
            self.db.update_video(bvid, status="running", last_error=None)

            if v["comment_cursor"] != "":
                self._crawl_comments(v)

            v = self.db.get_video(bvid)
            done = v["danmaku_done"] or 0
            total = v["danmaku_total_seg"] or 1
            if done < total:
                self._crawl_danmaku(v)

            self.db.update_video(bvid, status="done")
            log.info("✓ %s 完成", bvid)

        except KeyboardInterrupt:
            self.db.update_video(bvid, status="pending"); raise
        except Exception as e:
            log.exception("✗ %s 失败：%s", bvid, e)
            self.db.update_video(bvid, status="failed", last_error=str(e)[:500])

    def _crawl_comments(self, v: sqlite3.Row):
        bvid = v["bvid"]; aid = v["aid"]
        cur = v["comment_cursor"]
        try:
            pagination = json.loads(cur) if cur else {"offset": ""}
        except Exception:
            pagination = {"offset": ""}

        page_no = 0
        while not STOP.is_set():
            page_no += 1
            data = self.api.comments_main(aid, pagination)
            code = data.get("code", 0)
            if code == 12022:
                log.info("[%s] 评论已关闭", bvid)
                self.db.update_video(bvid, comment_cursor=""); return
            if code != 0:
                raise RuntimeError(f"comments 失败 {bvid}: {data}")
            d = data["data"]
            replies = d.get("replies") or []

            rows: List[Tuple] = []
            for r in replies:
                rows.append(self._reply_row(bvid, r, root=0))
                got = 0
                for sub in (r.get("replies") or []):
                    rows.append(self._reply_row(bvid, sub, root=r["rpid"])); got += 1
                rcount = r.get("rcount", 0)
                if rcount > got:
                    rows.extend(self._fetch_all_replies(bvid, aid, r["rpid"]))
            self.db.insert_comments(rows)

            cursor = d.get("cursor") or {}
            if cursor.get("is_end"):
                self.db.update_video(bvid, comment_cursor="")
                log.info("[%s] 评论完成（%d 页）", bvid, page_no); return
            pag = cursor.get("pagination_reply") or {}
            next_offset = pag.get("next_offset")
            if not next_offset:
                self.db.update_video(bvid, comment_cursor=""); return
            pagination = {"offset": next_offset}
            self.db.update_video(bvid, comment_cursor=json.dumps(pagination))

    def _fetch_all_replies(self, bvid: str, aid: int, root: int) -> List[Tuple]:
        rows: List[Tuple] = []; pn = 1
        while not STOP.is_set():
            data = self.api.comment_replies(aid, root, pn)
            if data.get("code") != 0:
                log.warning("[%s] 子评论失败 root=%s: %s", bvid, root, data); return rows
            d = data["data"] or {}
            subs = d.get("replies") or []
            if not subs: return rows
            for sub in subs:
                rows.append(self._reply_row(bvid, sub, root=root))
            page = d.get("page") or {}
            size = page.get("size") or 20; count = page.get("count") or 0
            if pn * size >= count: return rows
            pn += 1
        return rows

    @staticmethod
    def _reply_row(bvid: str, r: Dict[str, Any], root: int) -> Tuple:
        m = r.get("member") or {}; c = r.get("content") or {}
        mid = m.get("mid")
        try: mid = int(mid) if mid is not None else None
        except (TypeError, ValueError): mid = None
        return (r["rpid"], bvid, r.get("parent", 0), root,
                mid, m.get("uname"), r.get("ctime"), r.get("like", 0),
                c.get("message", ""),
                json.dumps(r, ensure_ascii=False, separators=(",", ":")))

    def _crawl_danmaku(self, v: sqlite3.Row):
        bvid = v["bvid"]; cid = v["cid"]; aid = v["aid"]
        if not cid:
            log.warning("[%s] 缺 cid，跳过弹幕", bvid); return
        total = v["danmaku_total_seg"] or 1
        start = (v["danmaku_done"] or 0) + 1
        for seg in range(start, total + 1):
            if STOP.is_set(): return
            try:
                buf = self.api.danmaku_seg(cid, aid, seg)
                elems = parse_dm_seg(buf)
            except Exception as e:
                log.warning("[%s] 弹幕段 %d 失败：%s（跳过）", bvid, seg, e)
                self.db.update_video(bvid, danmaku_done=seg); continue
            rows = [(e.get("id"), bvid, cid, e.get("progress", 0),
                     e.get("mode", 0), e.get("fontsize", 0), e.get("color", 0),
                     e.get("mid_hash"), e.get("content", ""),
                     e.get("ctime", 0), e.get("pool", 0), e.get("weight", 0))
                    for e in elems if e.get("id")]
            self.db.insert_danmaku(rows)
            self.db.update_video(bvid, danmaku_done=seg)
        log.info("[%s] 弹幕完成（%d 段）", bvid, total)

    def run(self):
        pending = self.db.list_pending()
        log.info("待处理 %d 个视频，并发 %d，限速 QPS=%.2f",
                 len(pending), CFG.workers, CFG.qps)
        if not pending: return
        if CFG.workers <= 1:
            for v in pending:
                if STOP.is_set(): break
                self.crawl_one(v["bvid"])
            return
        with ThreadPoolExecutor(max_workers=CFG.workers, thread_name_prefix="w") as ex:
            futs = {ex.submit(self.crawl_one, v["bvid"]): v["bvid"] for v in pending}
            try:
                for f in as_completed(futs):
                    if STOP.is_set(): break
                    try: f.result()
                    except KeyboardInterrupt: STOP.set(); break
                    except Exception as e: log.error("任务异常 %s: %s", futs[f], e)
            except KeyboardInterrupt: STOP.set()


# ============================================================
# CLI
# ============================================================

def _build():
    db = DB(CFG.db_path)
    limiter = RateLimiter(CFG.qps, CFG.burst)
    sess = Session(limiter)
    wbi = WBI(sess)
    api = BiliAPI(sess, wbi)
    return db, Crawler(db, api)


def cmd_init(args):
    _, cr = _build()
    only = None
    if args.week:
        only = [args.week]
    elif args.from_ is not None and args.to is not None:
        only = list(range(args.from_, args.to + 1))
    cr.init_weeks(only=only)


def cmd_run(args):
    _, cr = _build()
    cr.run()


def cmd_status(args):
    db = DB(CFG.db_path)
    s = db.stats()
    pending = s["videos_total"] - s["videos_done"] - s["videos_failed"] - s["videos_running"]
    print(f"期号：         {s['weeks_done']}/{s['weeks_total']}")
    print(f"视频总数：     {s['videos_total']}")
    print(f"  已完成：     {s['videos_done']}")
    print(f"  运行中：     {s['videos_running']}")
    print(f"  失败：       {s['videos_failed']}")
    print(f"  待处理：     {pending}")
    print(f"评论条数：     {s['comments']:,}")
    print(f"弹幕条数：     {s['danmaku']:,}")


def cmd_retry(args):
    db = DB(CFG.db_path)
    with db.tx() as c:
        c.execute("UPDATE videos SET status='pending', last_error=NULL "
                  "WHERE status IN ('failed','running')")
    print("已重置 failed/running -> pending")


def main():
    p = argparse.ArgumentParser(description="Bilibili 每周必看 采集器")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("init", help="拉取期号 + 视频列表")
    pi.add_argument("--week", type=int, help="只拉某一期")
    pi.add_argument("--from", dest="from_", type=int, help="起始期号")
    pi.add_argument("--to", type=int, help="结束期号（含）")
    pi.set_defaults(func=cmd_init)

    sub.add_parser("run", help="采集评论+弹幕（断点续传）").set_defaults(func=cmd_run)
    sub.add_parser("status", help="查看进度").set_defaults(func=cmd_status)
    sub.add_parser("retry", help="重置失败任务").set_defaults(func=cmd_retry)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()