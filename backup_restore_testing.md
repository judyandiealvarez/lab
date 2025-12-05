# Backup/Restore Testing Flow

## Steps
1. **Create backup**: `lab.py backup`
2. **Copy to Proxmox**: `scp backup-*.tar.gz root@10.11.3.4:/tmp/`
3. **Redeploy cluster**: `lab.py redeploy`
4. **Copy backup to backup container**: `scp root@10.11.3.4:/tmp/backup-*.tar.gz root@10.11.3.4:/backup/` (via pct push)
5. **Restore**: `lab.py restore --backup-name backup-YYYYMMDD_HHMMSS`
6. **Verify**: Check Rancher UI and `kubectl get nodes` shows all nodes Ready

## Notes
- Use latest backup file name from step 1
- Ensure all k3s services are stopped before restore
- Worker nodes should reconnect automatically after restore


