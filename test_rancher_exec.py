#!/usr/bin/env python3
"""Test kubectl exec in Rancher pod to set password"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from lab import get_config
from services.lxc import LXCService
from services.pct import PCTService

cfg = get_config()
if not cfg.kubernetes or not cfg.kubernetes.control:
    print('No kubernetes control node found')
    sys.exit(1)

control_id = cfg.kubernetes.control[0]
control_config = next((c for c in cfg.containers if c.id == control_id), None)
if not control_config:
    print(f'Control node {control_id} not found')
    sys.exit(1)

lxc_service = LXCService(cfg.proxmox_host, cfg.ssh)
if not lxc_service.connect():
    print('Failed to connect to LXC')
    sys.exit(1)

try:
    pct_service = PCTService(lxc_service)
    # Get Rancher pod name
    pod_cmd = "kubectl get pods -n cattle-system -l app=rancher --no-headers | head -1 | awk '{print $1}'"
    pod_output, pod_exit = pct_service.execute(str(control_id), pod_cmd, timeout=30)
    if pod_exit != 0 or not pod_output:
        print('Rancher pod not found')
        sys.exit(1)
    pod_name = pod_output.strip()
    print(f'Rancher pod: {pod_name}')
    
    # Check what password-related commands exist
    check_cmd = f"kubectl exec -n cattle-system {pod_name} -c rancher -- which reset-password ensure-default-admin 2>&1"
    check_output, check_exit = pct_service.execute(str(control_id), check_cmd, timeout=30)
    print(f'Available commands: {check_output}')
    
    # Try to see reset-password help or usage
    help_cmd = f"kubectl exec -n cattle-system {pod_name} -c rancher -- reset-password 2>&1 | head -10"
    help_output, help_exit = pct_service.execute(str(control_id), help_cmd, timeout=30)
    print(f'\nreset-password output:\n{help_output}')
    
    # Check if we can pass password as argument
    admin_password = cfg.services.rancher.password if cfg.services.rancher else "admin"
    test_cmd = f"kubectl exec -n cattle-system {pod_name} -c rancher -- reset-password {admin_password} 2>&1 || echo 'Command failed or password not accepted as arg'"
    test_output, test_exit = pct_service.execute(str(control_id), test_cmd, timeout=30)
    print(f'\nTrying to set password directly:\n{test_output}')
    
finally:
    lxc_service.disconnect()


