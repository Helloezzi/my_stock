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
/volume1/docker/my_stock/scripts/run_nas_daily.sh
```

## What The Script Does
1. Moves to the project directory.
2. Ensures the Docker app is up.
3. Runs one-day FDR collection for today.
4. Rebuilds `today_picks.json`.
5. Runs the daily output check.

## Synology Task Scheduler Example
- Task type:
  - User-defined script
- User:
  - a user that can run Docker commands, or root if needed
- Schedule:
  - Weekly
  - Mon, Tue, Wed, Thu, Fri
  - 18:30
- Script:
```bash
cd /volume1/docker/my_stock
sh scripts/run_nas_daily.sh
```

## Cron Example
```cron
30 18 * * 1-5 cd /volume1/docker/my_stock && sh scripts/run_nas_daily.sh >> /volume1/docker/my_stock/data/_locks/nas_daily.log 2>&1
```

## Manual Test Command
```bash
cd /volume1/docker/my_stock
sh scripts/run_nas_daily.sh
```

## Expected Result
- `data/daily/kospi/krx_ohlcv_YYYYMMDD.csv` updated
- `data/daily/kosdaq/krx_ohlcv_YYYYMMDD.csv` updated
- `data/picks/today_picks.json` updated
- `scripts/check_daily_outputs.py` reports fresh dates and non-broken picks payload

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
cd /volume1/docker/my_stock
sh scripts/run_nas_daily.sh
```
