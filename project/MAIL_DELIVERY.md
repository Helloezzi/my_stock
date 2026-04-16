# MAIL_DELIVERY

## Goal
- Send the published daily picks by email after the daily pipeline finishes.
- Keep mail setup local-only so SMTP credentials do not enter git.

## Current Status
- Optional SMTP mail sending is implemented.
- It uses `data/picks/today_picks.json` as the source.
- If local mail config is missing, the daily NAS script skips mail safely.

## Local Config Files
- Example file:
  - `mail.config.example.json`
- Real local file:
  - `mail.config.local.json`

## Setup
1. Copy `mail.config.example.json` to `mail.config.local.json`.
2. Fill in real SMTP values.
3. Keep `enabled` set to `true`.

## Supported Config Fields
- `enabled`
- `smtp_host`
- `smtp_port`
- `security`
  - `ssl`
  - `starttls`
  - `none`
- `smtp_username`
- `smtp_password`
- `from_email`
- `to_emails`
- `subject_prefix`

## Manual Test
```bash
python scripts/send_today_picks_email.py
```

## NAS Daily Behavior
- `scripts/run_nas_daily.sh` checks for `mail.config.local.json`.
- If the file exists, it runs:
```bash
python scripts/send_today_picks_email.py
```
- If the file does not exist, mail is skipped and the rest of the pipeline continues.

## Notes
- This setup is for simple SMTP delivery first.
- For Gmail or similar providers, an app password is usually required.
- The mail content is built from the already-published lightweight picks payload.
