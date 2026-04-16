# SSH_ACCESS

## NAS SSH Connection
- Host: `192.168.124.101`
- Port: `22`
- User: `dasol`

## PowerShell Command
```powershell
ssh -p 22 dasol@192.168.124.101
```

## Project Directory After Login
```bash
cd /volume1/docker/my_stock
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
cd /volume1/docker/my_stock
sudo /usr/local/bin/docker compose up -d --build
```

### Open Shell Inside Container
```bash
sudo /usr/local/bin/docker exec -it my-stock /bin/bash
```

### Run Published Picks Build
```bash
cd /volume1/docker/my_stock
sudo /usr/local/bin/docker exec -it my-stock python scripts/build_today_picks.py --market ALL --limit 10
```

### Run Daily Output Check
```bash
cd /volume1/docker/my_stock
sudo /usr/local/bin/docker exec -it my-stock python scripts/check_daily_outputs.py
```

### Run Scheduled Daily Pipeline Manually
```bash
cd /volume1/docker/my_stock
sh scripts/run_nas_daily.sh
```

## Windows Batch Shortcut
```powershell
.\deploy_nas.bat shell
```

## Notes
- SSH works on port `22`, not `2222`.
- Docker binary on the NAS is `/usr/local/bin/docker`.
- Python app scripts should be run inside the `my-stock` container, not on the NAS host OS.
- If `git pull` is blocked by generated data before this cleanup is fully reflected on NAS, check `git status` first.
