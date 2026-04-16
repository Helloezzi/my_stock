from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.email_delivery import send_today_picks_email


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Send today_picks.json by email using local SMTP config")
    parser.add_argument("--config", default="mail.config.local.json", help="Local mail config json path")
    parser.add_argument("--payload", default="data/picks/today_picks.json", help="today picks json path")
    args = parser.parse_args()

    ok, message = send_today_picks_email(config_path=args.config, payload_path=args.payload)
    print(message)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(_cli())
