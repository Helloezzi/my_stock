# NAS_SCHEDULING

## Goal
- Run the daily data pipeline automatically on the NAS.
- Keep the mobile app ready by refreshing daily files and published picks without manual commands.

## Recommended Schedule
- Time zone: `Asia/Seoul`
- Recommended time: `18:30`
- Frequency: every weekday

Why `18:30`:
- Market close has already passed.
- Upstream data sources have more time to settle.
- The app can be opened later without waiting for heavy work.

## Recommended Execution Target
- Use the NAS host scheduler to run:
  - `scripts/run_nas_daily.sh`

## Script Path
```bash
/your/deploy/path/my_stock/scripts/run_nas_daily.sh
```

## What The Script Does
1. Moves to the project directory.
2. Ensures the Docker app is up.
3. Runs one-day FDR collection for today.
4. Rebuilds `today_picks.json`.
5. Optionally sends email if `mail.config.local.json` exists.
6. Runs the daily output check.

## Synology Task Scheduler Example
- Task type:
  - User-defined script
- User:
  - `root`
- Schedule:
  - Weekly
  - Mon, Tue, Wed, Thu, Fri
  - 18:30
- Script:
```bash
cd /your/deploy/path/my_stock
sh scripts/run_nas_daily.sh
```

Important:
- The task should run as `root`.
- Running as a normal user such as `dasol` will usually fail on Docker socket access.
- If manual execution works only with `sudo sh scripts/run_nas_daily.sh`, the scheduler user is the first thing to check.

## If Manual Works But Scheduler Does Not
- Most common cause:
  - The task is not running as `root`.
- Other common causes:
  - The task is disabled or the weekday/time is wrong.
  - The script path is different from the real deploy path.
  - The scheduler command does not `cd` into the project directory first.
  - The scheduler ran, but stdout/stderr was not checked.

Recommended scheduler script:
```bash
cd /volume1/docker/my_stock
sh scripts/run_nas_daily.sh >> /volume1/docker/my_stock/data/_locks/nas_daily.log 2>&1
```

What to verify in Synology:
- Task is `Enabled`
- User is `root`
- Schedule is weekday `18:30` in NAS local time
- Working script path matches the real project path
- Task execution history shows a successful run

## Cron Example
```cron
30 18 * * 1-5 cd /your/deploy/path/my_stock && sh scripts/run_nas_daily.sh >> /your/deploy/path/my_stock/data/_locks/nas_daily.log 2>&1
```

## Manual Test Command
```bash
cd /your/deploy/path/my_stock
sudo sh scripts/run_nas_daily.sh
```

## Expected Result
- `data/daily/kospi/krx_ohlcv_YYYYMMDD.csv` updated
- `data/daily/kosdaq/krx_ohlcv_YYYYMMDD.csv` updated
- `data/picks/today_picks.json` updated
- `scripts/check_daily_outputs.py` reports fresh dates and non-broken picks payload

## Verified Status
- Verified on: `2026-04-16`
- Verification command:
```bash
sudo /usr/local/bin/docker exec -it my-stock python scripts/check_daily_outputs.py
```
- Observed healthy output:
  - `latest_daily_file`: `krx_ohlcv_20260416.csv` for both markets
  - `cache_max_date`: `2026-04-16` for both markets
  - `trade_date`: `2026-04-16`
  - `strategy`: `pullback_rr`
  - `pick_count`: `20`
  - `per_market_counts`: `KOSPI 10`, `KOSDAQ 10`

## If It Fails
- Check container status:
```bash
sudo /usr/local/bin/docker ps
sudo /usr/local/bin/docker logs --tail 200 my-stock
```
- Run the verification command:
```bash
sudo /usr/local/bin/docker exec -it my-stock python scripts/check_daily_outputs.py
```
- Re-run manually:
```bash
cd /your/deploy/path/my_stock
sudo sh scripts/run_nas_daily.sh
```
