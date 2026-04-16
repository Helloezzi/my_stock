from __future__ import annotations

import os
import threading
from datetime import datetime
from pathlib import Path

from core.daily_pipeline import run_daily_pipeline

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
    done = lp.with_suffix(".done")
    if done.exists():
        return False

    try:
        fd = os.open(str(lp), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        return False

    def worker():
        try:
            result = run_daily_pipeline()
            ok = bool(result.download_ok and result.published_ok)
            if ok:
                done.write_text("ok", encoding="utf-8")
        finally:
            try:
                lp.unlink(missing_ok=True)
            except Exception:
                pass

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return True
