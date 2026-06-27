#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载每周必看视频的封面图，用于 image-as-data 分析。

用法：
    python download_covers.py                  # 下载全部缺失的
    python download_covers.py --workers 16     # 并发更高
    python download_covers.py --limit 200      # 只下前 200 张（小样本验证）
    python download_covers.py --out covers/    # 自定义输出目录
"""

import argparse
import logging
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get("BILI_DB", "bili.db")
LOG_FILE = os.environ.get("BILI_COVER_LOG", "covers.log")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Referer": "https://www.bilibili.com",
    "Accept": "image/avif,image/webp,image/png,image/*,*/*;q=0.8",
}

ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("covers")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stderr); sh.setFormatter(fmt); logger.addHandler(sh)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8"); fh.setFormatter(fmt); logger.addHandler(fh)
    return logger


log = setup_logging()


def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    if not url.startswith("https://"):
        return None
    return url


def ext_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    for e in ALLOWED_EXTS:
        if path.endswith(e):
            return e
    return ".jpg"


def list_targets(out_dir: Path, limit: Optional[int]) -> List[Tuple[str, str, Path]]:
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    rows = conn.execute(
        "SELECT bvid, pic FROM videos WHERE pic IS NOT NULL AND pic != ''"
    ).fetchall()
    conn.close()

    have = {
        p.stem
        for p in out_dir.iterdir()
        if p.is_file() and p.suffix.lower() in ALLOWED_EXTS and p.stat().st_size > 0
    }
    log.info("已存在封面：%d 张", len(have))

    out: List[Tuple[str, str, Path]] = []
    for bvid, pic in rows:
        if bvid in have:
            continue
        url = normalize_url(pic)
        if url is None:
            continue
        ext = ext_from_url(url)
        dest = out_dir / f"{bvid}{ext}"
        out.append((bvid, url, dest))
    if limit is not None:
        out = out[:limit]
    return out


_session_local = threading.local()


def get_session() -> requests.Session:
    s = getattr(_session_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _session_local.s = s
    return s


def download_one(item: Tuple[str, str, Path],
                 retries: int = 3,
                 timeout: int = 20) -> Tuple[str, bool, Optional[str]]:
    bvid, url, dest = item
    s = get_session()
    last_err: Optional[str] = None
    for attempt in range(retries + 1):
        try:
            r = s.get(url, timeout=timeout)
            if r.status_code == 200 and r.content:
                tmp = dest.with_suffix(dest.suffix + ".part")
                tmp.write_bytes(r.content)
                tmp.rename(dest)
                return bvid, True, None
            last_err = f"HTTP {r.status_code} ({len(r.content)} B)"
        except Exception as e:
            last_err = repr(e)
        if attempt < retries:
            time.sleep(min(2 ** attempt, 8))
    return bvid, False, last_err


def main():
    p = argparse.ArgumentParser(description="下载 Bilibili 每周必看视频封面")
    p.add_argument("--out", default="covers", help="输出目录（默认 covers/）")
    p.add_argument("--workers", type=int, default=8, help="并发数（默认 8）")
    p.add_argument("--limit", type=int, default=None, help="只下前 N 张（冒烟测试）")
    p.add_argument("--report-every", type=int, default=200, help="每 N 张打一次进度")
    args = p.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    targets = list_targets(out_dir, args.limit)
    log.info("待下载：%d 张 | out=%s | workers=%d", len(targets), out_dir, args.workers)
    if not targets:
        log.info("无新增任务，退出")
        return

    ok = 0
    fail = 0
    t0 = time.time()
    failures: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="dl") as ex:
        futs = {ex.submit(download_one, t): t[0] for t in targets}
        for i, f in enumerate(as_completed(futs), 1):
            bvid, success, err = f.result()
            if success:
                ok += 1
            else:
                fail += 1
                failures.append((bvid, err or ""))
                log.warning("✗ %s: %s", bvid, err)
            if i % args.report_every == 0 or i == len(futs):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(futs) - i) / rate if rate > 0 else 0
                log.info("[进度 %d/%d] ok=%d fail=%d %.1f img/s ETA=%.0fs",
                         i, len(futs), ok, fail, rate, eta)

    log.info("完成：成功 %d，失败 %d，总计 %d", ok, fail, len(targets))
    if failures:
        fpath = Path("covers_failed.txt")
        with fpath.open("w", encoding="utf-8") as fh:
            for bvid, err in failures:
                fh.write(f"{bvid}\t{err}\n")
        log.info("失败列表已写入 %s（重跑同名命令即可断点续传）", fpath)


if __name__ == "__main__":
    main()
