# SSH_ACCESS

## NAS SSH Connection
- Host: `your-nas-host`
- Port: `22`
- User: `your-user`

## PowerShell Command
```powershell
ssh -p 22 your-user@your-nas-host
```

## Project Directory After Login
```bash
cd /your/deploy/path/my_stock
```

## Frequently Used Commands
### Check Git Status
```bash
git status
git rev-parse --short HEAD
```

### Check Docker Container
```bash
sudo /usr/local/bin/docker ps
sudo /usr/local/bin/docker logs --tail 200 my-stock
```

### Restart App
```bash
cd /your/deploy/path/my_stock
sudo /usr/local/bin/docker compose up -d --build
```

### Open Shell Inside Container
```bash
sudo /usr/local/bin/docker exec -it my-stock /bin/bash
```

### Run Published Picks Build
```bash
cd /your/deploy/path/my_stock
sudo /usr/local/bin/docker exec -it my-stock python scripts/build_today_picks.py --market ALL --limit 10
```

### Run Daily Output Check
```bash
cd /your/deploy/path/my_stock
sudo /usr/local/bin/docker exec -it my-stock python scripts/check_daily_outputs.py
```

### Run Scheduled Daily Pipeline Manually
```bash
cd /your/deploy/path/my_stock
sudo sh scripts/run_nas_daily.sh
```

## Windows Batch Shortcut
```powershell
.\deploy_nas.bat shell
```

## Notes
- Real NAS values should live in `deploy_nas.config.local.bat`, not in tracked files.
- Copy `deploy_nas.config.example.bat` to `deploy_nas.config.local.bat` and fill in your real values.
- SSH works on port `22`, not `2222`.
- Docker binary on the NAS is `/usr/local/bin/docker`.
- Python app scripts should be run inside the `my-stock` container, not on the NAS host OS.
