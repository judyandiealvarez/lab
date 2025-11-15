"""
HAProxy container type - creates an HAProxy load balancer container
"""
import sys
import time
import base64
import logging

# Import helper functions from libs
from libs import common, container
from libs.config import LabConfig, ContainerConfig
from cli import Apt, SystemCtl, Generic, CommandWrapper

# Get logger for this module
logger = logging.getLogger(__name__)

setup_container_base = container.setup_container_base
destroy_container = common.destroy_container
pct_exec = common.pct_exec


def create_container(container_cfg: ContainerConfig, cfg: LabConfig):
    """Create HAProxy load balancer container - method for type 'haproxy'"""
    proxmox_host = cfg.proxmox_host
    container_id = setup_container_base(container_cfg, cfg, privileged=True)
    
    if not container_id:
        logger.error(f"Failed to create container {container_cfg.id}")
        return False
    
    params = container_cfg.params
    http_port = params.get('http_port', 80)
    https_port = params.get('https_port', 443)
    stats_port = params.get('stats_port', 8404)
    
    # Get Swarm node IPs for backend
    swarm_nodes = cfg.swarm_managers + cfg.swarm_workers
    backend_servers = []
    for i, node in enumerate(swarm_nodes, 1):
        backend_servers.append(f"    server node{i} {node.ip_address}:80 check")
    
    # Install HAProxy - Ubuntu 25.04 may not have haproxy in main repo, try universe
    logger.info("Installing HAProxy...")
    # First, fix any dpkg issues and broken packages
    dpkg_cmd = "dpkg --configure -a 2>&1 || true"
    dpkg_output = pct_exec(proxmox_host, container_id, dpkg_cmd, check=False, capture_output=True, timeout=60, cfg=cfg)
    dpkg_result = CommandWrapper.parse_result(dpkg_output)
    if dpkg_result.has_error:
        logger.warning(f"dpkg configure had issues: {dpkg_result.error_type.value} - {dpkg_result.error_message}")
    
    # Fix broken packages
    fix_broken_cmd = Apt.fix_broken_cmd()
    fix_broken_output = pct_exec(proxmox_host, container_id, fix_broken_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    fix_broken_result = CommandWrapper.parse_result(fix_broken_output)
    if fix_broken_result.has_error:
        logger.warning(f"Fix broken packages had issues: {fix_broken_result.error_type.value} - {fix_broken_result.error_message}")
    
    # Update and install
    update_cmd = Apt.update_cmd(quiet=True)
    update_output = pct_exec(proxmox_host, container_id, update_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    update_result = CommandWrapper.parse_result(update_output)
    if not update_result:
        logger.error(f"Failed to update packages: {update_result.error_type.value} - {update_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    install_cmd = Apt.install_cmd(["haproxy"])
    install_output = pct_exec(proxmox_host, container_id, install_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    install_result = CommandWrapper.parse_result(install_output)
    
    # Verify installation
    check_cmd = Apt.command_exists_check_cmd("haproxy")
    check_output = pct_exec(proxmox_host, container_id, check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if not Apt.parse_command_exists(check_output):
        logger.warning("haproxy package not found, trying to install from universe...")
        # Fix dpkg and broken packages again before retry
        fix_broken_output = pct_exec(proxmox_host, container_id, fix_broken_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
        retry_cmd = Apt.install_cmd(["haproxy"], no_install_recommends=True)
        retry_output = pct_exec(proxmox_host, container_id, retry_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
        retry_result = CommandWrapper.parse_result(retry_output)
        if not retry_result:
            logger.error(f"Failed to install HAProxy: {retry_result.error_type.value} - {retry_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        # Check again
        check_output = pct_exec(proxmox_host, container_id, check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if not Apt.parse_command_exists(check_output):
            logger.error("Failed to install HAProxy - command not found")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    elif not install_result:
        logger.error(f"Failed to install HAProxy: {install_result.error_type.value} - {install_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Create HAProxy configuration
    logger.info("Configuring HAProxy...")
    backend_servers_str = "\n".join(backend_servers)
    haproxy_config = f"""global
    log /dev/log local0
    log /dev/log local1 notice
    chroot /var/lib/haproxy
    stats socket /run/haproxy/admin.sock mode 660 level admin
    stats timeout 30s
    user haproxy
    group haproxy
    daemon

defaults
    log global
    mode http
    option httplog
    option dontlognull
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms
    errorfile 400 /etc/haproxy/errors/400.http
    errorfile 403 /etc/haproxy/errors/403.http
    errorfile 408 /etc/haproxy/errors/408.http
    errorfile 500 /etc/haproxy/errors/500.http
    errorfile 502 /etc/haproxy/errors/502.http
    errorfile 503 /etc/haproxy/errors/503.http
    errorfile 504 /etc/haproxy/errors/504.http

# Stats page
listen stats
    bind *:{stats_port}
    stats enable
    stats uri /
    stats refresh 30s
    stats admin if TRUE

# Frontend for HTTP
frontend http_frontend
    bind *:{http_port}
    default_backend swarm_backend

# Frontend for HTTPS
frontend https_frontend
    bind *:{https_port}
    default_backend swarm_backend

# Backend for Docker Swarm nodes
backend swarm_backend
    balance roundrobin
    option httpchk GET /
    http-check expect status 200
{backend_servers_str}
"""
    
    # Write HAProxy config using base64 to avoid quote issues
    config_b64 = base64.b64encode(haproxy_config.encode()).decode()
    config_cmd = f"echo {config_b64} | base64 -d > /etc/haproxy/haproxy.cfg 2>&1"
    config_output = pct_exec(proxmox_host, container_id, config_cmd, check=False, capture_output=True, cfg=cfg)
    config_result = CommandWrapper.parse_result(config_output)
    if config_result.has_error:
        logger.error(f"Failed to write HAProxy config: {config_result.error_type.value} - {config_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify config file exists
    verify_cmd = "test -f /etc/haproxy/haproxy.cfg && echo exists || echo missing"
    verify_output = pct_exec(proxmox_host, container_id, verify_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if verify_output and "exists" not in verify_output:
        logger.error("HAProxy config file was not created")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Fix systemd service for LXC (disable PrivateNetwork)
    logger.info("Configuring HAProxy systemd service for LXC...")
    systemd_fix_cmd = "sed -i 's/PrivateNetwork=.*/PrivateNetwork=no/' /usr/lib/systemd/system/haproxy.service 2>/dev/null || true 2>&1"
    systemd_fix_output = pct_exec(proxmox_host, container_id, systemd_fix_cmd, check=False, capture_output=True, cfg=cfg)
    systemd_fix_result = CommandWrapper.parse_result(systemd_fix_output)
    if systemd_fix_result.has_error:
        logger.warning(f"Systemd service fix had issues: {systemd_fix_result.error_type.value} - {systemd_fix_result.error_message}")
    
    daemon_reload_cmd = SystemCtl.daemon_reload_cmd()
    daemon_reload_output = pct_exec(proxmox_host, container_id, daemon_reload_cmd, check=False, capture_output=True, cfg=cfg)
    daemon_reload_result = CommandWrapper.parse_result(daemon_reload_output)
    if daemon_reload_result.has_error:
        logger.warning(f"Daemon reload had issues: {daemon_reload_result.error_type.value} - {daemon_reload_result.error_message}")
    
    # Enable and start HAProxy
    logger.info("Starting HAProxy service...")
    start_cmd = SystemCtl.enable_and_start_cmd("haproxy")
    start_output = pct_exec(proxmox_host, container_id, start_cmd, check=False, capture_output=True, cfg=cfg)
    start_result = CommandWrapper.parse_result(start_output)
    if not start_result:
        logger.error(f"Failed to start HAProxy service: {start_result.error_type.value} - {start_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # If systemd fails, start manually as fallback
    is_active_cmd = SystemCtl.is_active_check_cmd("haproxy")
    is_active_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, cfg=cfg)
    if not SystemCtl.parse_is_active(is_active_output):
        logger.info("Systemd start failed, starting HAProxy manually...")
        manual_start_cmd = "haproxy -f /etc/haproxy/haproxy.cfg -D 2>&1"
        manual_start_output = pct_exec(proxmox_host, container_id, manual_start_cmd, check=False, capture_output=True, cfg=cfg)
        manual_start_result = CommandWrapper.parse_result(manual_start_output)
        if manual_start_result.has_error:
            logger.error(f"Manual HAProxy start failed: {manual_start_result.error_type.value} - {manual_start_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    time.sleep(cfg.waits.service_start)
    
    # Verify HAProxy is running
    is_active_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, cfg=cfg)
    if SystemCtl.parse_is_active(is_active_output):
        logger.info("HAProxy installed and running")
    else:
        # Check if process is running manually
        process_check_cmd = "pgrep haproxy >/dev/null 2>&1 && echo running || echo not_running"
        process_check_output = pct_exec(proxmox_host, container_id, process_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if process_check_output and "running" in process_check_output:
            logger.info("HAProxy is running (manual process)")
        else:
            logger.error(f"HAProxy is not running")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    logger.info(f"HAProxy container '{container_cfg.name}' created successfully")
    return True
