from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

from core.downloader_daily import DownloadResult, download_daily_all
from core.published_picks import PublishedPicksConfig, publish_today_picks


@dataclass(frozen=True)
class DailyPipelineResult:
    download_results: Dict[str, DownloadResult]
    download_ok: bool
    published_ok: bool
    latest_json: Path | None = None
    history_json: Path | None = None
    history_csv: Path | None = None
    message: str = ""


def _to_yyyymmdd(value: str | date | None) -> str:
    if value is None:
        return datetime.now().strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return text
    return datetime.strptime(text, "%Y-%m-%d").strftime("%Y%m%d")


def _run_fdr_fallback(
    yyyymmdd: str | date | None,
    markets: Iterable[str],
) -> tuple[bool, str]:
    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "download_daily_fdr.py"
    if not script_path.exists():
        return False, f"fallback script not found: {script_path}"

    target = _to_yyyymmdd(yyyymmdd)
    market_values = {str(m).upper().strip() for m in markets}
    market_arg = "ALL" if market_values == {"KOSPI", "KOSDAQ"} else next(iter(market_values), "ALL")
    cmd = [
        sys.executable,
        str(script_path),
        "--start",
        target,
        "--end",
        target,
        "--market",
        market_arg,
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return True, proc.stdout.strip()

    detail = proc.stderr.strip() or proc.stdout.strip() or f"exit={proc.returncode}"
    return False, detail


def run_daily_pipeline(
    yyyymmdd: str | date | None = None,
    markets: Iterable[str] = ("KOSPI", "KOSDAQ"),
    out_dir: str | Path = "data/daily",
    force: bool = False,
    min_rows_by_market: Optional[Dict[str, int]] = None,
    published_config: PublishedPicksConfig | None = None,
) -> DailyPipelineResult:
    download_results = download_daily_all(
        yyyymmdd=yyyymmdd,
        markets=markets,
        out_dir=out_dir,
        force=force,
        min_rows_by_market=min_rows_by_market,
    )
    download_ok = all(result.ok for result in download_results.values())
    download_message = "ok"

    if not download_ok:
        fallback_ok, fallback_message = _run_fdr_fallback(yyyymmdd=yyyymmdd, markets=markets)
        if fallback_ok:
            download_ok = True
            download_message = "pykrx daily download failed; recovered via download_daily_fdr.py fallback"
        else:
            return DailyPipelineResult(
                download_results=download_results,
                download_ok=False,
                published_ok=False,
                message=f"download step failed; fallback also failed: {fallback_message}",
            )

    outputs = publish_today_picks(config=published_config or PublishedPicksConfig())
    return DailyPipelineResult(
        download_results=download_results,
        download_ok=True,
        published_ok=True,
        latest_json=outputs.get("latest_json"),
        history_json=outputs.get("history_json"),
        history_csv=outputs.get("history_csv"),
        message=download_message,
    )
