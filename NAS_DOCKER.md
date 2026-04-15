# NAS Docker Deploy

## Overview
- The app runs as a Streamlit container on port `8501`.
- Persistent data lives in the mounted host folder `./data` -> `/app/data`.
- As of April 15, 2026, daily market backfill was switched to `FinanceDataReader` using the `NAVER` source because the previous KRX auth flow is no longer usable.

## One-Time Setup On NAS
```sh
cd /volume1/docker
git clone https://github.com/Helloezzi/my_stock
cd my_stock
mkdir -p data
```

## Update App Code
```sh
cd /volume1/docker/my_stock
git pull
```

## Build And Run
### Docker Compose
```sh
cd /volume1/docker/my_stock
sudo docker compose up -d --build
```

### Plain Docker
```sh
cd /volume1/docker/my_stock
sudo docker build -t my-stock:latest .
sudo docker rm -f my-stock 2>/dev/null
sudo docker run -d \
  --name my-stock \
  -p 8501:8501 \
  -v /volume1/docker/my_stock/data:/app/data \
  --restart unless-stopped \
  my-stock:latest
```

## Check Status
```sh
sudo docker ps
sudo docker logs my-stock
```

## Backfill Daily Data
- Run this when the app data on NAS is behind the latest trading date.
- This script writes per-day CSV files under `/app/data/daily/...`.

### KOSPI + KOSDAQ from February 28, 2026 to April 15, 2026
```sh
cd /volume1/docker/my_stock
sudo docker exec -it my-stock python download_daily_fdr.py --start 2026-02-28 --end 2026-04-15
```

## Windows Batch Helper
- A Windows helper script is included at [deploy_nas.bat](D:/Dev/my_stock/deploy_nas.bat).
- It wraps the SSH flow from your old terminal log.

```bat
deploy_nas.bat deploy
deploy_nas.bat status
deploy_nas.bat logs
deploy_nas.bat backfill
deploy_nas.bat shell
```

## Notes
- The app reads from `/app/data`, so the NAS-mounted `data/` folder must stay attached.
- If you rebuild the image, data survives because it is stored in the mounted volume.
- `requirements.txt` now includes `finance-datareader`, so rebuilding the image is required to use the new backfill path.
- The terminal log you shared includes old auth tokens and keys. Those should be treated as exposed and rotated if they are still valid anywhere.
