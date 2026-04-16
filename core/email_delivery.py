from __future__ import annotations

import json
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from core.published_picks import TODAY_PICKS_PATH, load_published_picks

DEFAULT_MAIL_CONFIG_PATH = Path("mail.config.local.json")


def load_mail_config(path: str | Path = DEFAULT_MAIL_CONFIG_PATH) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if not bool(data.get("enabled", False)):
        return None
    return data


def _build_subject(payload: dict[str, Any], config: dict[str, Any]) -> str:
    prefix = str(config.get("subject_prefix", "[my_stock]")).strip() or "[my_stock]"
    trade_date = str(payload.get("trade_date", "-"))
    pick_count = int(payload.get("summary", {}).get("pick_count", 0) or 0)
    return f"{prefix} Today Picks {trade_date} ({pick_count})"


def _build_text_body(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    picks = payload.get("picks", []) if isinstance(payload, dict) else []
    lines = [
        f"Trade date: {payload.get('trade_date', '-')}",
        f"Generated at: {payload.get('generated_at', '-')}",
        f"Strategy: {payload.get('source', {}).get('strategy_label', '-')}",
        f"Pick count: {summary.get('pick_count', 0)}",
        f"Per market: {summary.get('per_market_counts', {})}",
        "",
    ]

    if not picks:
        lines.append("No picks available for today.")
    else:
        for item in picks:
            ticker = str(item.get("ticker", "")).zfill(6)
            lines.append(
                f"#{item.get('rank', '-')} {item.get('market', '-')} {ticker} "
                f"{item.get('name', ticker)} | stage={item.get('stage', '-')} | "
                f"entry={item.get('entry', '-')} stop={item.get('stop', '-')} "
                f"target={item.get('target', '-')} rr={item.get('rr', '-')}"
            )
    return "\n".join(lines)


def _build_html_body(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    source = payload.get("source", {}) if isinstance(payload, dict) else {}
    picks = payload.get("picks", []) if isinstance(payload, dict) else []
    rows = []
    for item in picks:
        ticker = str(item.get("ticker", "")).zfill(6)
        rows.append(
            "<tr>"
            f"<td>{item.get('rank', '-')}</td>"
            f"<td>{item.get('market', '-')}</td>"
            f"<td>{ticker}</td>"
            f"<td>{item.get('name', ticker)}</td>"
            f"<td>{item.get('stage', '-')}</td>"
            f"<td>{item.get('entry', '-')}</td>"
            f"<td>{item.get('stop', '-')}</td>"
            f"<td>{item.get('target', '-')}</td>"
            f"<td>{item.get('rr', '-')}</td>"
            "</tr>"
        )
    table_html = (
        "<p>No picks available for today.</p>"
        if not rows
        else (
            "<table border='1' cellpadding='6' cellspacing='0' "
            "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;'>"
            "<thead><tr>"
            "<th>Rank</th><th>Market</th><th>Ticker</th><th>Name</th><th>Stage</th>"
            "<th>Entry</th><th>Stop</th><th>Target</th><th>R/R</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )
    )
    return f"""
<html>
  <body style="font-family:Arial,sans-serif;">
    <h3>Today Picks</h3>
    <p><strong>Trade date:</strong> {payload.get('trade_date', '-')}<br>
    <strong>Generated at:</strong> {payload.get('generated_at', '-')}<br>
    <strong>Strategy:</strong> {source.get('strategy_label', '-')}<br>
    <strong>Pick count:</strong> {summary.get('pick_count', 0)}<br>
    <strong>Per market:</strong> {summary.get('per_market_counts', {})}</p>
    {table_html}
  </body>
</html>
""".strip()


def send_today_picks_email(
    *,
    config_path: str | Path = DEFAULT_MAIL_CONFIG_PATH,
    payload_path: str | Path = TODAY_PICKS_PATH,
) -> tuple[bool, str]:
    config = load_mail_config(config_path)
    if not config:
        return False, "mail config missing or disabled"

    payload = load_published_picks(payload_path)
    if not payload:
        return False, "today_picks payload missing or invalid"

    to_emails = [str(x).strip() for x in config.get("to_emails", []) if str(x).strip()]
    from_email = str(config.get("from_email", "")).strip()
    host = str(config.get("smtp_host", "")).strip()
    username = str(config.get("smtp_username", "")).strip()
    password = str(config.get("smtp_password", "")).strip()
    port = int(config.get("smtp_port", 465) or 465)
    security = str(config.get("security", "ssl")).strip().lower()

    if not (to_emails and from_email and host and username and password):
        return False, "mail config incomplete"

    msg = EmailMessage()
    msg["Subject"] = _build_subject(payload, config)
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg.set_content(_build_text_body(payload), subtype="plain")
    msg.add_alternative(_build_html_body(payload), subtype="html")

    if security == "ssl":
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context()) as server:
            server.login(username, password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(host, port) as server:
            server.ehlo()
            if security == "starttls":
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
            server.login(username, password)
            server.send_message(msg)

    return True, f"sent to {len(to_emails)} recipient(s)"
