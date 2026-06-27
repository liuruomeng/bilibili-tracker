#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bilibili 每周必看 采集器 v3




━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
反爬策略（v3 新增 ★）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 ★ bili_ticket   HMAC-SHA256 JWT，放入 Cookie 可降低风控概率，有效期3天自动续期
 ★ v_voucher     检测 WBI 签名失效的静默失败（code=0 但 data 含 v_voucher）
 ★ x-bili-trace-id  随机请求追踪头，缺失时更易被识别为爬虫
 curl_cffi       TLS/JA3/HTTP2 指纹伪装（优先），回退 requests
 WBI 签名        每小时自动刷新，带3次重试
 dm_img_*        画布指纹参数（评论+弹幕接口均携带）
 令牌桶限速      单次熔断上限300s，防止指数爆炸卡死
 STOP.wait()     替代 time.sleep()，Ctrl+C 即时响应


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
鲁棒性改进（v3 新增 ★）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 ★ run() 持续循环  不重启也能处理 recrawl 新加入的 pending 视频
 ★ 启动自动清理    僵死 running 状态在启动时重置为 pending
 ★ danmaku_total_seg NULL 修复  从时长重新估算，防止静默丢失弹幕
 ★ 评论分页上限    可配置保护，防止无限循环（默认不限制）
 ★ WBI 刷新重试    网络抖动不再崩溃整个进程
 ★ 子评论触顶缓存  max offset exceeded 不再反复重试同一 root
 comment_cursor   NULL/空字符串统一处理


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
安装
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 pip install requests python-dotenv curl_cffi   # curl_cffi 强烈推荐


用法：
 export BILI_COOKIE="SESSDATA=xxx; bili_jct=yyy; buvid3=zzz; ..."


 python crawler.py init                       # 拉取全部期号 + 每期视频列表
 python crawler.py init --week 250            # 只拉第 250 期
 python crawler.py init --from 200 --to 250   # 拉第 200~250 期
 python crawler.py run                        # 持续采集（断点续传，Ctrl+C 安全退出）
 python crawler.py meta                       # 补充分区等元信息
 python crawler.py status                     # 查看进度 + 覆盖率
 python crawler.py retry                      # 重置失败任务
 python crawler.py recrawl                    # 重置"已完成但实际缺数据"的视频
 python crawler.py recrawl --comments-only
 python crawler.py recrawl --danmaku-only
 python crawler.py recrawl --batch 500
 python crawler.py recrawl --min-expected 50
 python crawler.py recrawl --force-all
 python crawler.py recrawl --dry-run
"""


import argparse, hashlib, hmac, json, logging, os, random, signal
import sqlite3, sys, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode


# ── HTTP 客户端：优先 curl_cffi（TLS/JA3指纹伪装），回退 requests ──────────────
try:
    from curl_cffi import requests as _curl_requests
    _USE_CURL_CFFI = True
except ImportError:
    import requests as _curl_requests          # type: ignore
    _USE_CURL_CFFI = False


import requests as _std_requests               # 始终可用，用于异常捕获


from dotenv import load_dotenv
load_dotenv()




# ============================================================
# 配置
# ============================================================


@dataclass
class Config:
    db_path:      str   = os.environ.get("BILI_DB",      "bili.db")
    cookie:       str   = os.environ.get("BILI_COOKIE",  "")
    user_agent:   str   = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    qps:          float = float(os.environ.get("BILI_QPS",    "0.8"))
    burst:        int   = int(os.environ.get("BILI_BURST",   "2"))
    workers:      int   = int(os.environ.get("BILI_WORKERS", "1"))
    http_timeout: int   = 25
    http_retries: int   = 3
    cooldown_single_max: int = 300   # 单次熔断上限（防卡死）
    cooldown_max:        int = 1800  # 全局上限
    idle_poll_secs: int  = 60        # run() 空闲轮询间隔
    max_comment_pages: int = 0       # ★ 0 = 不限制（靠 cursor.is_end 自然结束）
    # ★ 楼中楼（子评论）采集开关，默认关闭：只爬主楼，跨期口径一致 + 提速 + 省空间
    collect_sub_replies: bool = os.environ.get("BILI_SUB_REPLIES", "0") == "1"
    log_file:     str   = os.environ.get("BILI_LOG", "bili.log")


CFG = Config()


# dm_img 画布指纹（固定值；缺失时 B 站静默返回空或 -352）
_DM_IMG_LIST       = "[]"
_DM_IMG_STR        = "V2ViR0wgMS"
_DM_COVER_IMG_STR  = (
    "QU5HTEUgKEludGVsLCBJbnRlbChSKSBIRCBHcmFwaGljcyBEaXJl"
    "Y3QzRDExIHZzXzVfMCBwc181XzApR29vZ2xlIEluYy4gKEludGVsKQ"
)
_DM_IMG_INTER_STR  = "0"


# bili_ticket HMAC 密钥（公开）
_BILI_TICKET_KEY   = "XgwSnGZ1p"
_BILI_TICKET_URL   = "https://api.bilibili.com/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket"




# ============================================================
# 日志 & 优雅退出
# ============================================================


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("bili")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    if CFG.log_file:
        fh = logging.FileHandler(CFG.log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


log = _setup_logging()


if _USE_CURL_CFFI:
    log.info("HTTP 客户端：curl_cffi（TLS/JA3 指纹伪装已启用）")
else:
    log.warning("HTTP 客户端：requests（建议 pip install curl_cffi 以启用 TLS 指纹伪装）")


STOP = threading.Event()


def _on_signal(sig, frame):
    if STOP.is_set():
        log.warning("再次收到 %s，强制退出", sig)
        os._exit(1)
    log.warning("收到信号 %s，准备优雅退出（再按一次强退）", sig)
    STOP.set()


signal.signal(signal.SIGINT,  _on_signal)
signal.signal(signal.SIGTERM, _on_signal)




# ============================================================
# WBI 签名
# ============================================================


_MIXIN_TAB = [
    46, 47, 18,  2, 53,  8, 23, 32, 15, 50, 10, 31, 58,  3, 45, 35,
    27, 43,  5, 49, 33,  9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48,  7, 16, 24, 55, 40, 61, 26, 17,  0,  1, 60, 51, 30,  4,
    22, 25, 54, 21, 56, 59,  6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]


class WBI:
    def __init__(self, sess: "Session"):
        self.s     = sess
        self._key: Optional[str] = None
        self._key_ts: float = 0.0
        self._lock = threading.Lock()


    def _fetch_key_from_nav(self) -> Tuple[str, str]:
        """从 nav 接口获取 WBI img/sub key。"""
        r    = self.s.raw_get("https://api.bilibili.com/x/web-interface/nav")
        data = r.json()
        wbi  = (data.get("data") or {}).get("wbi_img") or {}
        img  = wbi.get("img_url", "").rsplit("/", 1)[-1].split(".")[0]
        sub  = wbi.get("sub_url", "").rsplit("/", 1)[-1].split(".")[0]
        if not img or not sub:
            raise RuntimeError(f"WBI key 字段为空：{data}")
        return img, sub


    def _refresh_locked(self):
        """★ 带3次重试的 WBI key 刷新，防止网络抖动崩溃。"""
        if self._key and time.time() - self._key_ts < 3600:
            return
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                img, sub = self._fetch_key_from_nav()
                raw        = img + sub
                self._key    = "".join(raw[i] for i in _MIXIN_TAB)[:32]
                self._key_ts = time.time()
                log.info("WBI key 已刷新")
                return
            except Exception as e:
                last_exc = e
                if attempt < 2:
                    wait = 5 * (attempt + 1)
                    log.warning("WBI key 刷新失败 %d/3，%ds 后重试：%s", attempt + 1, wait, e)
                    STOP.wait(wait)
        raise RuntimeError(f"WBI key 刷新失败（3次）：{last_exc}")


    def sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self._refresh_locked()
            key = self._key
        params = dict(params)
        params["wts"] = int(time.time())
        clean: Dict[str, str] = {}
        for k, v in params.items():
            sv = str(v)
            for c in "!'()*":
                sv = sv.replace(c, "")
            clean[k] = sv
        q = urlencode(sorted(clean.items()))
        params["w_rid"] = md5((q + key).encode()).hexdigest()
        return params




# ============================================================
# bili_ticket（★ v3 新增）
# HMAC-SHA256 签名的 JWT，放入 Cookie 可降低风控概率，有效期3天
# ============================================================


class BiliTicket:
    def __init__(self, sess: "Session"):
        self.s       = sess
        self._ticket: Optional[str] = None
        self._ticket_ts: float = 0.0
        self._lock   = threading.Lock()


    @staticmethod
    def _hmac_sha256(key: str, message: str) -> str:
        return hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest()


    def _refresh_locked(self):
        # TTL 259200s（3天），提前1小时续期
        if self._ticket and time.time() - self._ticket_ts < 259200 - 3600:
            return
        ts      = int(time.time())
        hexsign = self._hmac_sha256(_BILI_TICKET_KEY, f"ts{ts}")
        # 从 cookie 中提取 bili_jct（csrf），没有也行
        csrf = ""
        for part in CFG.cookie.split(";"):
            part = part.strip()
            if part.startswith("bili_jct="):
                csrf = part[len("bili_jct="):]
                break
        try:
            r = self.s.raw_post(_BILI_TICKET_URL, params={
                "key_id":      "ec02",
                "hexsign":     hexsign,
                "context[ts]": str(ts),
                "csrf":        csrf,
            })
            data = r.json()
            ticket = (data.get("data") or {}).get("ticket")
            if ticket:
                self._ticket    = ticket
                self._ticket_ts = time.time()
                log.info("bili_ticket 已刷新（有效期3天）")
            else:
                log.warning("bili_ticket 获取失败（忽略）：%s", data)
        except Exception as e:
            log.warning("bili_ticket 刷新异常（忽略）：%s", e)


    def get(self) -> Optional[str]:
        with self._lock:
            self._refresh_locked()
            return self._ticket




# ============================================================
# 令牌桶 + 风控熔断
# ★ 单次退避上限 300s，on_success 快速重置，防无限卡死
# ============================================================


class RateLimiter:
    def __init__(self, qps: float, burst: int):
        self.qps    = qps
        self.burst  = burst
        self.tokens = float(burst)
        self.last   = time.time()
        self.lock   = threading.Lock()
        self.blocked_until:     float = 0.0
        self.consecutive_risks: int   = 0


    def acquire(self):
        while True:
            if STOP.is_set():
                raise KeyboardInterrupt()
            with self.lock:
                now = time.time()
                if now < self.blocked_until:
                    wait = self.blocked_until - now
                else:
                    self.tokens = min(
                        self.burst,
                        self.tokens + (now - self.last) * self.qps,
                    )
                    self.last = now
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                    wait = (1 - self.tokens) / self.qps
            STOP.wait(min(wait, 5.0))


    def on_success(self):
        with self.lock:
            if self.consecutive_risks > 0:
                self.consecutive_risks = max(0, self.consecutive_risks - 1)


    def on_risk(self, code: int):
        with self.lock:
            self.consecutive_risks += 1
            base  = 300 if code == -352 else 200
            exp   = min(self.consecutive_risks - 1, 3)
            cool  = min(CFG.cooldown_single_max, base * (2 ** exp))
            cool  = min(cool, CFG.cooldown_max)
            until = time.time() + cool
            if until > self.blocked_until:
                self.blocked_until = until
            log.warning(
                "风控触发 code=%s，熔断 %ds（连续 %d 次）",
                code, cool, self.consecutive_risks,
            )




# ============================================================
# HTTP Session
# ★ curl_cffi impersonate chrome131
# ★ bili_ticket 注入 Cookie
# ★ x-bili-trace-id 随机追踪头
# ★ v_voucher 静默失败检测
# ★ get_bytes JSON 风控响应检测
# ============================================================


class Session:
    RISK_CODES = {-352, -412, -509, -799}
    AUTH_CODES = {-101, -111}


    def __init__(self, limiter: RateLimiter):
        self.limiter = limiter
        self._bt: Optional["BiliTicket"] = None  # 延迟注入，避免循环依赖


        if _USE_CURL_CFFI:
            self._s = _curl_requests.Session(impersonate="chrome131")
        else:
            self._s = _std_requests.Session()


        self._s.headers.update({
            "User-Agent":      CFG.user_agent,
            "Referer":         "https://www.bilibili.com",
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin":          "https://www.bilibili.com",
        })
        if CFG.cookie:
            self._s.headers["Cookie"] = CFG.cookie
        else:
            log.warning("未设置 BILI_COOKIE，部分接口会返回 -101")


    def set_bili_ticket(self, bt: "BiliTicket"):
        """注入 BiliTicket（构造后调用一次）。"""
        self._bt = bt


    def _build_cookie_header(self) -> str:
        """将 bili_ticket 追加到 Cookie 字符串。"""
        base = CFG.cookie
        if self._bt:
            ticket = self._bt.get()
            if ticket and "bili_ticket=" not in base:
                base = base.rstrip("; ") + f"; bili_ticket={ticket}"
        return base


    @staticmethod
    def _trace_id() -> str:
        """★ 生成随机 x-bili-trace-id（32位十六进制）。"""
        return "%032x" % random.getrandbits(128)


    def raw_get(self, url: str, params=None):
        """不经过限速，直接 GET（用于 WBI key / bili_ticket）。"""
        return self._s.get(url, params=params, timeout=CFG.http_timeout)


    def raw_post(self, url: str, params=None):
        """不经过限速，直接 POST（用于 bili_ticket）。"""
        return self._s.post(url, params=params, timeout=CFG.http_timeout)


    def _do(self, url: str, params: Dict[str, Any], want_bytes: bool):
        last_exc: Optional[Exception] = None
        for attempt in range(CFG.http_retries + 1):
            self.limiter.acquire()
            try:
                # ★ 每次请求动态注入 bili_ticket + trace-id
                hdrs = {
                    "Cookie":           self._build_cookie_header(),
                    "x-bili-trace-id":  self._trace_id(),
                }
                r = self._s.get(
                    url, params=params,
                    headers=hdrs, timeout=CFG.http_timeout,
                )


                if r.status_code == 412:
                    self.limiter.on_risk(-412); continue
                if r.status_code in (429, 503):
                    self.limiter.on_risk(-352); continue
                r.raise_for_status()


                if want_bytes:
                    # ★ 检测弹幕接口返回 JSON 风控响应
                    ct  = r.headers.get("Content-Type", "")
                    raw = r.content
                    if "json" in ct or (len(raw) < 300 and raw.lstrip().startswith(b"{")):
                        try:
                            djson = r.json()
                            code  = djson.get("code", 0)
                            if code in self.RISK_CODES:
                                self.limiter.on_risk(code); continue
                            if code in self.AUTH_CODES:
                                raise RuntimeError(
                                    f"Cookie 失效 (code={code})：{djson.get('message')}"
                                )
                            if code != 0:
                                raise RuntimeError(
                                    f"弹幕请求失败 code={code}: {djson.get('message')}"
                                )
                        except (ValueError, KeyError):
                            pass
                    self.limiter.on_success()
                    return raw


                data = r.json()
                code = data.get("code", 0)


                # ★ v_voucher 检测：code=0 但 WBI 实际已失效
                if code == 0 and isinstance(data.get("data"), dict):
                    if "v_voucher" in (data["data"] or {}):
                        log.warning("检测到 v_voucher，WBI key 可能已失效，触发重刷")
                        self.limiter.on_risk(-352)
                        continue


                if code == 0:
                    self.limiter.on_success(); return data
                if code in self.RISK_CODES:
                    self.limiter.on_risk(code); continue
                if code in self.AUTH_CODES:
                    raise RuntimeError(
                        f"Cookie 失效或未登录 (code={code})：{data.get('message')}"
                    )
                self.limiter.on_success(); return data


            except Exception as e:
                if isinstance(e, RuntimeError):
                    raise
                last_exc = e
                wait = 2 ** attempt + random.random()
                log.warning(
                    "HTTP 错误 %s（重试 %d/%d，%.1fs 后）",
                    e, attempt + 1, CFG.http_retries, wait,
                )
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
    tid                 INTEGER,
    tname               TEXT,
    description         TEXT,
    pic                 TEXT,
    meta_done           INTEGER DEFAULT 0,
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
        self.path  = path
        self.local = threading.local()
        c = self.connect()
        c.executescript(SCHEMA)
        self._migrate()


    def _migrate(self):
        c = self.connect()
        new_cols = [
            ("videos", "tid",         "INTEGER"),
            ("videos", "tname",       "TEXT"),
            ("videos", "description", "TEXT"),
            ("videos", "pic",         "TEXT"),
        ]
        added = 0
        for table, col, typ in new_cols:
            try:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
                added += 1
                log.info("迁移：%s 新增列 %s", table, col)
            except sqlite3.OperationalError:
                pass
        if added:
            log.info("迁移：共新增 %d 列", added)
        rows = c.execute(
            "UPDATE videos SET meta_done=0 WHERE meta_done=1 AND tid IS NULL"
        ).rowcount
        if rows:
            log.info("迁移：重置 %d 条老数据 meta_done → 0", rows)


    def connect(self) -> sqlite3.Connection:
        c = getattr(self.local, "conn", None)
        if c is None:
            c = sqlite3.connect(
                self.path, timeout=60,
                isolation_level=None, check_same_thread=False,
            )
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
                c.execute(
                    """INSERT INTO weeks(number, name, subject, status, updated_at)
                       VALUES(?,?,?,?,?)
                       ON CONFLICT(number) DO UPDATE SET
                           name=excluded.name, subject=excluded.subject,
                           status=excluded.status, updated_at=excluded.updated_at""",
                    (r["number"], r.get("name"), r.get("subject"),
                     r.get("status", 0), now),
                )


    def mark_week_done(self, number: int):
        with self.tx() as c:
            c.execute(
                "UPDATE weeks SET list_done=1, updated_at=? WHERE number=?",
                (int(time.time()), number),
            )


    def list_pending_weeks(self, only: Optional[List[int]] = None) -> List[sqlite3.Row]:
        c = self.connect()
        if only:
            qs = ",".join("?" * len(only))
            return list(c.execute(
                f"SELECT * FROM weeks WHERE number IN ({qs}) AND list_done=0", only
            ))
        return list(c.execute("SELECT * FROM weeks WHERE list_done=0 ORDER BY number"))


    # ---- 视频 ----
    def upsert_week_videos(self, week_no: int, vids: List[Dict[str, Any]]):
        if not vids: return
        now = int(time.time())
        with self.tx() as c:
            for v in vids:
                stat  = v.get("stat")  or {}
                owner = v.get("owner") or {}
                c.execute(
                    """INSERT INTO videos(
                           bvid, aid, cid, week_no, title,
                           owner_mid, owner_name, pubdate, duration,
                           view, danmaku, reply, like, coin, favorite, share,
                           rcmd_reason, tid, tname, description, pic, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(bvid) DO UPDATE SET
                           cid=excluded.cid,
                           week_no=COALESCE(videos.week_no, excluded.week_no),
                           view=excluded.view, danmaku=excluded.danmaku,
                           reply=excluded.reply, like=excluded.like,
                           tid=COALESCE(videos.tid, excluded.tid),
                           tname=COALESCE(videos.tname, excluded.tname),
                           description=COALESCE(videos.description, excluded.description),
                           pic=COALESCE(videos.pic, excluded.pic),
                           updated_at=excluded.updated_at""",
                    (
                        v["bvid"], v.get("aid"), v.get("cid"), week_no, v.get("title"),
                        owner.get("mid"), owner.get("name"),
                        v.get("pubdate"), v.get("duration"),
                        stat.get("view", 0), stat.get("danmaku", 0),
                        stat.get("reply", 0), stat.get("like", 0),
                        stat.get("coin", 0), stat.get("favorite", 0),
                        stat.get("share", 0),
                        (v.get("rcmd_reason") or {}).get("content")
                            if isinstance(v.get("rcmd_reason"), dict) else None,
                        v.get("tid"), v.get("tname"),
                        v.get("desc"), v.get("pic"), now,
                    ),
                )
                dur       = v.get("duration") or 0
                total_seg = max(1, (dur + 359) // 360)
                c.execute(
                    "UPDATE videos SET danmaku_total_seg=? "
                    "WHERE bvid=? AND danmaku_total_seg IS NULL",
                    (total_seg, v["bvid"]),
                )


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
            "SELECT * FROM videos WHERE status NOT IN ('done') "
            "ORDER BY week_no DESC, bvid"
        ))


    def list_need_meta(self) -> List[sqlite3.Row]:
        c = self.connect()
        return list(c.execute(
            "SELECT * FROM videos WHERE meta_done=0 ORDER BY week_no DESC, bvid"
        ))


    def get_video(self, bvid: str) -> Optional[sqlite3.Row]:
        c = self.connect()
        return c.execute("SELECT * FROM videos WHERE bvid=?", (bvid,)).fetchone()


    def insert_comments(self, rows: List[Tuple]):
        if not rows: return
        with self.tx() as c:
            c.executemany(
                """INSERT OR IGNORE INTO comments
                   (rpid, bvid, parent, root, mid, uname, ctime, likes, content)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )


    def insert_danmaku(self, rows: List[Tuple]):
        if not rows: return
        with self.tx() as c:
            c.executemany(
                """INSERT OR IGNORE INTO danmaku
                   (id, bvid, cid, progress, mode, fontsize, color,
                    mid_hash, content, ctime, pool, weight)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )


    def stats(self) -> Dict[str, Any]:
        c = self.connect()
        s: Dict[str, Any] = {
            "weeks_total":    c.execute("SELECT COUNT(*) FROM weeks").fetchone()[0],
            "weeks_done":     c.execute("SELECT COUNT(*) FROM weeks WHERE list_done=1").fetchone()[0],
            "videos_total":   c.execute("SELECT COUNT(*) FROM videos").fetchone()[0],
            "videos_done":    c.execute("SELECT COUNT(*) FROM videos WHERE status='done'").fetchone()[0],
            "videos_failed":  c.execute("SELECT COUNT(*) FROM videos WHERE status='failed'").fetchone()[0],
            "videos_running": c.execute("SELECT COUNT(*) FROM videos WHERE status='running'").fetchone()[0],
            "comments":       c.execute("SELECT COUNT(*) FROM comments").fetchone()[0],
            "danmaku":        c.execute("SELECT COUNT(*) FROM danmaku").fetchone()[0],
        }
        s["vids_with_comments"]    = c.execute("SELECT COUNT(DISTINCT bvid) FROM comments").fetchone()[0]
        s["vids_with_danmaku"]     = c.execute("SELECT COUNT(DISTINCT bvid) FROM danmaku").fetchone()[0]
        s["vids_expect_comments"]  = c.execute("SELECT COUNT(*) FROM videos WHERE reply > 10").fetchone()[0]
        s["vids_expect_danmaku"]   = c.execute("SELECT COUNT(*) FROM videos WHERE danmaku > 10").fetchone()[0]
        s["vids_missing_comments"] = c.execute("""
            SELECT COUNT(*) FROM videos v
            WHERE v.status='done' AND v.reply > 10
              AND NOT EXISTS (SELECT 1 FROM comments c WHERE c.bvid = v.bvid)
        """).fetchone()[0]
        s["vids_missing_danmaku"]  = c.execute("""
            SELECT COUNT(*) FROM videos v
            WHERE v.status='done' AND v.danmaku > 10
              AND NOT EXISTS (SELECT 1 FROM danmaku d WHERE d.bvid = v.bvid)
        """).fetchone()[0]
        return s




# ============================================================
# 弹幕 protobuf 解析（纯 Python，无需 protobuf 库）
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
            if   field_no == 1:  out["id"]      = v
            elif field_no == 2:  out["progress"] = v
            elif field_no == 3:  out["mode"]     = v
            elif field_no == 4:  out["fontsize"] = v
            elif field_no == 5:  out["color"]    = v
            elif field_no == 8:  out["ctime"]    = v
            elif field_no == 9:  out["weight"]   = v
            elif field_no == 11: out["pool"]     = v
            elif field_no == 13: out["attr"]     = v
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
# B 站 API
# ============================================================


class BiliAPI:
    def __init__(self, sess: Session, wbi: WBI):
        self.s   = sess
        self.wbi = wbi


    def list_all_weeks(self) -> List[Dict[str, Any]]:
        data = self.s.get_json(
            "https://api.bilibili.com/x/web-interface/popular/series/list", {}
        )
        if data.get("code") != 0:
            raise RuntimeError(f"期号列表失败：{data}")
        return (data.get("data") or {}).get("list") or []


    def get_week(self, number: int) -> Dict[str, Any]:
        data = self.s.get_json(
            "https://api.bilibili.com/x/web-interface/popular/series/one",
            {"number": number},
        )
        if data.get("code") != 0:
            raise RuntimeError(f"第 {number} 期失败：{data}")
        return data["data"]


    def comments_main(self, oid: int, pagination: Dict[str, Any]) -> Dict[str, Any]:
        """评论主接口，携带 WBI 签名 + dm_img_* 画布指纹。"""
        params = {
            "oid":            oid,
            "type":           1,
            "mode":           3,
            "pagination_str": json.dumps(pagination, separators=(",", ":")),
            "plat":           1,
            "web_location":   "1315875",
            "dm_img_list":       _DM_IMG_LIST,
            "dm_img_str":        _DM_IMG_STR,
            "dm_cover_img_str":  _DM_COVER_IMG_STR,
            "dm_img_inter_str":  _DM_IMG_INTER_STR,
        }
        params = self.wbi.sign(params)
        return self.s.get_json(
            "https://api.bilibili.com/x/v2/reply/wbi/main", params
        )


    def comment_replies(self, oid: int, root: int, pn: int) -> Dict[str, Any]:
        return self.s.get_json(
            "https://api.bilibili.com/x/v2/reply/reply",
            {"oid": oid, "type": 1, "root": root, "ps": 20, "pn": pn},
        )


    def danmaku_seg(self, cid: int, aid: int, segment_index: int) -> bytes:
        """WBI 签名版弹幕接口，含 dm_img_* 参数。"""
        params = self.wbi.sign({
            "type":             1,
            "oid":              cid,
            "pid":              aid,
            "segment_index":    segment_index,
            "dm_img_list":       _DM_IMG_LIST,
            "dm_img_str":        _DM_IMG_STR,
            "dm_cover_img_str":  _DM_COVER_IMG_STR,
            "dm_img_inter_str":  _DM_IMG_INTER_STR,
        })
        return self.s.get_bytes(
            "https://api.bilibili.com/x/v2/dm/wbi/web/seg.so", params
        )


    def video_info(self, bvid: str) -> Dict[str, Any]:
        params = self.wbi.sign({"bvid": bvid})
        data   = self.s.get_json(
            "https://api.bilibili.com/x/web-interface/view", params
        )
        if data.get("code") != 0:
            raise RuntimeError(f"视频详情失败 {bvid}: {data}")
        return data["data"]




# ============================================================
# 采集主流程
# ============================================================


class Crawler:
    def __init__(self, db: DB, api: BiliAPI):
        self.db  = db
        self.api = api


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
                d    = self.api.get_week(number)
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
        if v is None or v["status"] == "done":
            return
        try:
            self.db.update_video(bvid, status="running", last_error=None)


            # comment_cursor 为 NULL 或 "" 都表示"已完成"，跳过
            if v["comment_cursor"] != "":
                self._crawl_comments(v)


            v     = self.db.get_video(bvid)
            done  = v["danmaku_done"]      or 0
            total = v["danmaku_total_seg"] or 0
            # ★ danmaku_total_seg 为 NULL/0 时从时长重新估算
            if not total:
                dur   = v["duration"] or 360
                total = max(1, (dur + 359) // 360)
                self.db.update_video(bvid, danmaku_total_seg=total)
                log.warning("[%s] danmaku_total_seg 为空，从时长估算：%d 段", bvid, total)
            if done < total:
                self._crawl_danmaku(v)


            self.db.update_video(bvid, status="done")
            log.info("✓ %s 完成", bvid)


        except KeyboardInterrupt:
            self.db.update_video(bvid, status="pending"); raise
        except Exception as e:
            log.exception("✗ %s 失败：%s", bvid, e)
            self.db.update_video(bvid, status="failed", last_error=str(e)[:500])


    # ★ 改进：failed_roots 缓存 + 进度日志
    def _crawl_comments(self, v: sqlite3.Row):
        bvid             = v["bvid"]
        aid              = v["aid"]
        expected_replies = v["reply"] or 0

        cur      = v["comment_cursor"] or ""
        is_fresh = (cur == "")
        try:
            pagination = json.loads(cur) if cur else {"offset": ""}
        except Exception:
            pagination = {"offset": ""}
            is_fresh   = True

        page_no         = 0
        total_collected = 0
        failed_roots: set = set()   # ★ 记住已触顶的 root，不再重复尝试

        while not STOP.is_set():
            if CFG.max_comment_pages and page_no >= CFG.max_comment_pages:
                log.warning("[%s] 评论页数已达上限 %d，强制结束",
                            bvid, CFG.max_comment_pages)
                self.db.update_video(bvid, comment_cursor="")
                return

            page_no += 1
            data = self.api.comments_main(aid, pagination)
            code = data.get("code", 0)

            if code in (12002, 12022):
                log.info("[%s] 评论已关闭", bvid)
                self.db.update_video(bvid, comment_cursor="")
                return
            if code != 0:
                raise RuntimeError(f"comments 失败 {bvid}: {data}")

            d       = data["data"]
            replies = d.get("replies") or []

            if is_fresh and page_no == 1 and not replies and expected_replies > 10:
                raise RuntimeError(
                    f"疑似静默限流：{bvid} 应有 ~{expected_replies} 条评论，"
                    f"但首页返回空（code=0）"
                )

            rows: List[Tuple] = []
            for r in replies:
                rows.append(self._reply_row(bvid, r, root=0))
                total_collected += 1
                # ★ 默认只爬主楼；BILI_SUB_REPLIES=1 时才采集楼中楼
                if CFG.collect_sub_replies:
                    got = 0
                    for sub in (r.get("replies") or []):
                        rows.append(self._reply_row(bvid, sub, root=r["rpid"]))
                        got += 1; total_collected += 1
                    rpid = r["rpid"]
                    # ★ 跳过已知触顶的 root
                    if r.get("rcount", 0) > got and rpid not in failed_roots:
                        extra, hit_limit = self._fetch_all_replies(bvid, aid, rpid)
                        rows.extend(extra); total_collected += len(extra)
                        if hit_limit:
                            failed_roots.add(rpid)

            self.db.insert_comments(rows)

            # ★ 每 200 页打印进度
            if page_no % 200 == 0:
                log.info("[%s] 评论翻页中 第%d页 已收集%d条（跳过%d个触顶root）",
                         bvid, page_no, total_collected, len(failed_roots))

            cursor = d.get("cursor") or {}
            if cursor.get("is_end"):
                self.db.update_video(bvid, comment_cursor="")
                log.info("[%s] 评论完成（%d 页，%d 条）",
                         bvid, page_no, total_collected)
                return

            pag         = cursor.get("pagination_reply") or {}
            next_offset = pag.get("next_offset")
            if not next_offset:
                self.db.update_video(bvid, comment_cursor="")
                return

            pagination = {"offset": next_offset}
            self.db.update_video(bvid, comment_cursor=json.dumps(pagination))
            if STOP.wait(random.uniform(0.8, 2.0)):
                return


    # ★ 改进：pn 上限 + max offset exceeded 识别为不可恢复 + 返回触顶标志
    def _fetch_all_replies(self, bvid: str, aid: int, root: int) -> Tuple[List[Tuple], bool]:
        """返回 (子评论列表, 是否触顶)。触顶 = max offset exceeded，属于服务端硬限制。"""
        MAX_SUB_PN = 50
        rows: List[Tuple] = []
        pn = 1
        hit_limit = False

        while not STOP.is_set():
            if pn > MAX_SUB_PN:
                log.info("[%s] 子评论 root=%s 翻页达安全上限 %d，截断（已收集%d条）",
                         bvid, root, MAX_SUB_PN, len(rows))
                hit_limit = True
                return rows, hit_limit

            data = self.api.comment_replies(aid, root, pn)
            if data.get("code") != 0:
                msg = data.get("message", "")
                if data.get("code") == -400 and "max offset" in msg.lower():
                    if not rows:
                        log.debug("[%s] 子评论 root=%s 首页即触顶", bvid, root)
                    else:
                        log.info("[%s] 子评论 root=%s pn=%d 触顶，已收集%d条",
                                 bvid, root, pn, len(rows))
                    hit_limit = True
                else:
                    log.warning("[%s] 子评论失败 root=%s: %s", bvid, root, data)
                return rows, hit_limit

            d    = data["data"] or {}
            subs = d.get("replies") or []
            if not subs:
                return rows, hit_limit
            for sub in subs:
                rows.append(self._reply_row(bvid, sub, root=root))

            page  = d.get("page") or {}
            size  = page.get("size")  or 20
            count = page.get("count") or 0
            if pn * size >= count:
                return rows, hit_limit
            pn += 1
            if STOP.wait(random.uniform(0.5, 1.2)):
                return rows, hit_limit

        return rows, hit_limit


    @staticmethod
    def _reply_row(bvid: str, r: Dict[str, Any], root: int) -> Tuple:
        m   = r.get("member")  or {}
        c   = r.get("content") or {}
        mid = m.get("mid")
        try:
            mid = int(mid) if mid is not None else None
        except (TypeError, ValueError):
            mid = None
        return (
            r["rpid"], bvid, r.get("parent", 0), root,
            mid, m.get("uname"), r.get("ctime"), r.get("like", 0),
            c.get("message", ""),
        )


    def _crawl_danmaku(self, v: sqlite3.Row):
        bvid             = v["bvid"]
        cid              = v["cid"]
        aid              = v["aid"]
        expected_danmaku = v["danmaku"] or 0
        if not cid:
            log.warning("[%s] 缺 cid，跳过弹幕", bvid); return


        total           = v["danmaku_total_seg"] or 1
        start           = (v["danmaku_done"] or 0) + 1
        is_fresh        = (start == 1)
        total_collected = 0
        had_errors      = False


        for seg in range(start, total + 1):
            if STOP.is_set(): return
            try:
                buf   = self.api.danmaku_seg(cid, aid, seg)
                elems = parse_dm_seg(buf)
            except Exception as e:
                had_errors = True
                log.warning("[%s] 弹幕段 %d 失败：%s（跳过）", bvid, seg, e)
                self.db.update_video(bvid, danmaku_done=seg); continue


            rows = [
                (e.get("id"), bvid, cid, e.get("progress", 0),
                 e.get("mode", 0), e.get("fontsize", 0), e.get("color", 0),
                 e.get("mid_hash"), e.get("content", ""),
                 e.get("ctime", 0), e.get("pool", 0), e.get("weight", 0))
                for e in elems if e.get("id")
            ]
            self.db.insert_danmaku(rows)
            self.db.update_video(bvid, danmaku_done=seg)
            total_collected += len(rows)


            if seg < total:
                if STOP.wait(random.uniform(0.5, 1.2)):
                    return


        # 全段返空且无错误 → 疑似弹幕限流，重置以便重试
        if is_fresh and total_collected == 0 and expected_danmaku > 10 and not had_errors:
            self.db.update_video(bvid, danmaku_done=0)
            raise RuntimeError(
                f"疑似弹幕限流：{bvid} 应有 ~{expected_danmaku} 条弹幕，"
                f"但全部 {total} 段返回空"
            )


        log.info("[%s] 弹幕完成（%d 段，%d 条）", bvid, total, total_collected)


    # ★ run() 持续循环，无需重启即可处理 recrawl 追加的新任务
    def run(self):
        log.info("采集器启动（持续模式），QPS=%.2f，并发=%d", CFG.qps, CFG.workers)
        while not STOP.is_set():
            pending = self.db.list_pending()
            if not pending:
                log.info("无待处理视频，%ds 后再检查...", CFG.idle_poll_secs)
                STOP.wait(CFG.idle_poll_secs)
                continue
            log.info("本轮待处理 %d 个视频", len(pending))
            self._run_batch(pending)


    def _run_batch(self, pending: List[sqlite3.Row]):
        if CFG.workers <= 1:
            for i, v in enumerate(pending, 1):
                if STOP.is_set(): break
                self.crawl_one(v["bvid"])
                if i % 50 == 0:
                    log.info("[总进度] %d/%d", i, len(pending))
            return
        with ThreadPoolExecutor(
            max_workers=CFG.workers, thread_name_prefix="w"
        ) as ex:
            futs = {ex.submit(self.crawl_one, v["bvid"]): v["bvid"] for v in pending}
            try:
                for f in as_completed(futs):
                    if STOP.is_set(): break
                    try:
                        f.result()
                    except KeyboardInterrupt:
                        STOP.set(); break
                    except Exception as e:
                        log.error("任务异常 %s: %s", futs[f], e)
            except KeyboardInterrupt:
                STOP.set()


    def crawl_meta(self):
        pending = self.db.list_need_meta()
        total   = len(pending)
        log.info("需补充元信息：%d 个视频", total)
        if not total: return
        done = 0; failed = 0
        for i, v in enumerate(pending, 1):
            if STOP.is_set(): break
            bvid = v["bvid"]
            try:
                info = self.api.video_info(bvid)
                self.db.update_video(bvid,
                    tid=info.get("tid"), tname=info.get("tname"),
                    description=info.get("desc"), pic=info.get("pic"),
                    meta_done=1,
                )
                done += 1
                if i % 50 == 0 or i == total:
                    log.info("[进度 %d/%d] %s → %s", i, total, bvid,
                             info.get("tname", "未知"))
            except KeyboardInterrupt:
                STOP.set(); break
            except Exception as e:
                failed += 1
                log.warning("[%d/%d] ✗ %s: %s", i, total, bvid, e)
        log.info("元信息补充结束：成功 %d，失败 %d，共 %d", done, failed, total)




# ============================================================
# CLI
# ============================================================


def _build():
    db = DB(CFG.db_path)


    # ★ 启动时清理僵死的 running 状态（上次异常退出残留）
    with db.tx() as c:
        n = c.execute(
            "UPDATE videos SET status='pending' WHERE status='running'"
        ).rowcount
    if n:
        log.info("启动清理：重置 %d 个僵死 running → pending", n)


    limiter = RateLimiter(CFG.qps, CFG.burst)
    sess    = Session(limiter)
    wbi     = WBI(sess)
    bt      = BiliTicket(sess)
    sess.set_bili_ticket(bt)        # 注入 bili_ticket
    api     = BiliAPI(sess, wbi)
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




def cmd_meta(args):
    _, cr = _build()
    cr.crawl_meta()




def cmd_status(args):
    db  = DB(CFG.db_path)
    s   = db.stats()
    pending = (s["videos_total"] - s["videos_done"]
               - s["videos_failed"] - s["videos_running"])
    tls_tag = "curl_cffi ✓" if _USE_CURL_CFFI else "requests（建议安装 curl_cffi）"
    print("=" * 54)
    print("  Bilibili 每周必看 采集进度  v3")
    print(f"  HTTP 客户端：{tls_tag}")
    print("=" * 54)
    print(f"期号：           {s['weeks_done']}/{s['weeks_total']}")
    print(f"视频总数：       {s['videos_total']}")
    print(f"  已完成：       {s['videos_done']}")
    print(f"  运行中：       {s['videos_running']}")
    print(f"  失败：         {s['videos_failed']}")
    print(f"  待处理：       {pending}")
    print(f"评论条数：       {s['comments']:,}")
    print(f"弹幕条数：       {s['danmaku']:,}")
    print("-" * 54)
    print("  数据覆盖率（预期数 > 10 的视频）")
    print("-" * 54)
    ec  = s["vids_expect_comments"];  wc  = s["vids_with_comments"]
    ed  = s["vids_expect_danmaku"];   wd  = s["vids_with_danmaku"]
    mc  = s["vids_missing_comments"]; md_ = s["vids_missing_danmaku"]
    print(f"有评论的视频：   {wc}/{ec}  ({100*wc/ec:.1f}%)" if ec else "有评论的视频：   0/0")
    print(f"有弹幕的视频：   {wd}/{ed}  ({100*wd/ed:.1f}%)" if ed else "有弹幕的视频：   0/0")
    if mc or md_:
        print(f"⚠ 已完成但缺评论：{mc}  缺弹幕：{md_}")
        print("  → 运行 python crawler.py recrawl 查看详情")




def cmd_retry(args):
    db = DB(CFG.db_path)
    with db.tx() as c:
        n = c.execute(
            "UPDATE videos SET status='pending', last_error=NULL "
            "WHERE status IN ('failed','running')"
        ).rowcount
    print(f"已重置 {n} 个 failed/running → pending")
    print("提示：建议降低 QPS 再运行，例如 BILI_QPS=0.5 python crawler.py run")




def cmd_recrawl(args):
    db      = DB(CFG.db_path)
    c       = db.connect()
    min_exp = args.min_expected
    batch   = args.batch
    dry     = args.dry_run


    if args.force_all:
        sql  = "SELECT bvid FROM videos WHERE status='done'"
        if batch: sql += f" LIMIT {batch}"
        rows = c.execute(sql).fetchall()
        if args.comments_only:
            if dry: print(f"[预览] 将重置 {len(rows)} 个视频的评论"); return
            with db.tx() as tc:
                for r in rows:
                    tc.execute("UPDATE videos SET status='pending', comment_cursor=NULL, "
                               "last_error=NULL WHERE bvid=?", (r["bvid"],))
            print(f"已重置 {len(rows)} 个视频（仅评论）")
        elif args.danmaku_only:
            if dry: print(f"[预览] 将重置 {len(rows)} 个视频的弹幕"); return
            with db.tx() as tc:
                for r in rows:
                    tc.execute("UPDATE videos SET status='pending', danmaku_done=0, "
                               "last_error=NULL WHERE bvid=?", (r["bvid"],))
            print(f"已重置 {len(rows)} 个视频（仅弹幕）")
        else:
            if dry: print(f"[预览] 将重置 {len(rows)} 个视频（评论+弹幕）"); return
            with db.tx() as tc:
                for r in rows:
                    tc.execute("UPDATE videos SET status='pending', comment_cursor=NULL, "
                               "danmaku_done=0, last_error=NULL WHERE bvid=?", (r["bvid"],))
            print(f"已重置 {len(rows)} 个视频（全部重爬）")
        return


    missing_comments: set = set()
    missing_danmaku:  set = set()


    if not args.danmaku_only:
        rows = c.execute(
            "SELECT v.bvid FROM videos v "
            "WHERE v.status='done' AND v.reply > ? "
            "AND NOT EXISTS (SELECT 1 FROM comments cm WHERE cm.bvid = v.bvid) "
            "ORDER BY v.reply DESC",
            (min_exp,),
        ).fetchall()
        missing_comments = {r["bvid"] for r in rows}


    if not args.comments_only:
        rows = c.execute(
            "SELECT v.bvid FROM videos v "
            "WHERE v.status='done' AND v.danmaku > ? "
            "AND NOT EXISTS (SELECT 1 FROM danmaku dm WHERE dm.bvid = v.bvid) "
            "ORDER BY v.danmaku DESC",
            (min_exp,),
        ).fetchall()
        missing_danmaku = {r["bvid"] for r in rows}


    all_missing = missing_comments | missing_danmaku


    if not all_missing:
        print("✓ 所有已完成视频数据完整，无需重置"); return


    if batch and len(all_missing) > batch:
        both    = missing_comments & missing_danmaku
        only_c  = missing_comments - missing_danmaku
        only_d  = missing_danmaku  - missing_comments
        ordered = list(both) + list(only_c) + list(only_d)
        all_missing      = set(ordered[:batch])
        missing_comments &= all_missing
        missing_danmaku  &= all_missing


    reset_both          = missing_comments & missing_danmaku
    reset_comments_only = missing_comments - missing_danmaku
    reset_danmaku_only  = missing_danmaku  - missing_comments


    print(f"发现 {len(all_missing)} 个视频需要重爬（预期数 > {min_exp}）：")
    print(f"  评论+弹幕均缺：{len(reset_both)}")
    print(f"  仅缺评论：     {len(reset_comments_only)}")
    print(f"  仅缺弹幕：     {len(reset_danmaku_only)}")


    if dry:
        print("\n[预览模式] 未实际修改，去掉 --dry-run 执行重置"); return


    with db.tx() as tc:
        for bvid in reset_both:
            tc.execute("UPDATE videos SET status='pending', comment_cursor=NULL, "
                       "danmaku_done=0, last_error=NULL WHERE bvid=?", (bvid,))
        for bvid in reset_comments_only:
            tc.execute("UPDATE videos SET status='pending', comment_cursor=NULL, "
                       "last_error=NULL WHERE bvid=?", (bvid,))
        for bvid in reset_danmaku_only:
            tc.execute("UPDATE videos SET status='pending', danmaku_done=0, "
                       "last_error=NULL WHERE bvid=?", (bvid,))


    print(f"\n✓ 已重置 {len(all_missing)} 个视频 → pending")
    print("  下一步：python crawler.py run   （或若已在运行，会在下一轮自动拾起）")




def main():
    p   = argparse.ArgumentParser(description="Bilibili 每周必看 采集器 v3")
    sub = p.add_subparsers(dest="cmd", required=True)


    pi = sub.add_parser("init", help="拉取期号 + 视频列表")
    pi.add_argument("--week",         type=int, help="只拉某一期")
    pi.add_argument("--from", dest="from_", type=int, help="起始期号")
    pi.add_argument("--to",           type=int, help="结束期号（含）")
    pi.set_defaults(func=cmd_init)


    sub.add_parser("run",    help="持续采集评论+弹幕（断点续传，自动轮询）").set_defaults(func=cmd_run)
    sub.add_parser("meta",   help="补充视频元信息（分区等）").set_defaults(func=cmd_meta)
    sub.add_parser("status", help="查看进度 + 数据覆盖率").set_defaults(func=cmd_status)
    sub.add_parser("retry",  help="重置失败任务").set_defaults(func=cmd_retry)


    pr = sub.add_parser("recrawl", help="重置已完成但缺数据的视频")
    pr.add_argument("--comments-only", action="store_true")
    pr.add_argument("--danmaku-only",  action="store_true")
    pr.add_argument("--min-expected",  type=int, default=10,
                    help="仅重置预期数 > N 的视频（默认 10）")
    pr.add_argument("--batch",         type=int, default=0,
                    help="每批最多重置数量（0 = 全部）")
    pr.add_argument("--force-all",     action="store_true",
                    help="强制重置所有已完成视频")
    pr.add_argument("--dry-run",       action="store_true",
                    help="仅预览，不实际修改")
    pr.set_defaults(func=cmd_recrawl)


    args = p.parse_args()
    args.func(args)




if __name__ == "__main__":
    main()


