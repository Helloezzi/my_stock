# core/auto_daily.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import os
import threading
import time

from core.downloader_daily import download_daily_all

LOCK_DIR = Path("data") / "_locks"
LOCK_DIR.mkdir(parents=True, exist_ok=True)

def _today_key() -> str:
    return datetime.now().strftime("%Y%m%d")

def _lock_path() -> Path:
    return LOCK_DIR / f"daily_{_today_key()}.lock"

def try_run_daily_once_async() -> bool:
    """
    Returns True if started, False if already done/locked.
    """
    lp = _lock_path()

    # 이미 성공했으면 스킵 (성공 마커)
    done = lp.with_suffix(".done")
    if done.exists():
        return False

    # 간단한 락: 원자적 생성
    try:
        fd = os.open(str(lp), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        return False

    def worker():
        try:
            # 실행
            res = download_daily_all()
            ok = all(v.ok for v in res.values())
            # 성공 마커
            if ok:
                done.write_text("ok", encoding="utf-8")
        finally:
            # 락 해제
            try:
                lp.unlink(missing_ok=True)
            except Exception:
                pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return True