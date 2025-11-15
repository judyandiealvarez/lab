#!/usr/bin/env python3
"""
Lab CLI Tool - Manage Proxmox LXC containers and Docker Swarm
Implements all functionality directly in Python (not just calling bash scripts)
"""
import subprocess
import sys
import argparse
import os
import time
import re
import logging
from pathlib import Path
from datetime import datetime

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

SCRIPT_DIR = Path(__file__).parent.absolute()
CONFIG_FILE = SCRIPT_DIR / "lab.yaml"

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

# Initialize logging
from libs.logger import init_logger, get_logger
from libs.config import LabConfig
logger = get_logger(__name__)


def load_config() -> dict:
    """Load configuration from lab.yaml as dictionary"""
    if not CONFIG_FILE.exists():
        logger.error(f"Configuration file {CONFIG_FILE} not found")
        sys.exit(1)
    
    try:
        if HAS_YAML:
            with open(CONFIG_FILE, 'r') as f:
                config = yaml.safe_load(f)
            return config
        else:
            logger.error("PyYAML is required. Install it with: pip install pyyaml")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        sys.exit(1)


def get_config() -> LabConfig:
    """Get configuration and return as LabConfig instance"""
    config_dict = load_config()
    config = LabConfig.from_dict(config_dict)
    config.compute_derived_fields()
    return config


# Import functions from libs
from libs import common, container, template
from cli import PCT, SystemCtl, Apt, Docker, Gluster, CommandWrapper

# Re-export for backward compatibility
ssh_exec = common.ssh_exec
pct_exec = common.pct_exec
container_exists = common.container_exists
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
get_ssh_key = common.get_ssh_key
setup_ssh_key = common.setup_ssh_key
get_base_template = template.get_base_template
get_template_path = container.get_template_path
setup_container_base = container.setup_container_base


def create_container(container_cfg, cfg: LabConfig, step_num, total_steps):
    """Generic container creation dispatcher based on type - uses dynamic loading"""
    from ct import load_container_handler
    
    container_type = container_cfg.type
    container_name = container_cfg.name
    
    logger.info(f"[{step_num}/{total_steps}] Creating container '{container_name}' (type: {container_type})...")
    
    # Dynamically load container handler
    container_handler = load_container_handler(container_type)
    if not container_handler:
        logger.error(f"Unknown container type '{container_type}'")
        return False

    # Call the handler
    return container_handler(container_cfg, cfg)


def create_template(template_cfg, cfg: LabConfig, step_num, total_steps):
    """Generic template creation dispatcher based on type - uses dynamic loading"""
    from tmpl import load_template_handler
    
    template_type = template_cfg.type
    template_name = template_cfg.name
    
    logger.info(f"[{step_num}/{total_steps}] Creating template '{template_name}' (type: {template_type})...")
    
    # IP address is already computed in template_cfg.ip_address
    
    # Dynamically load template handler
    template_handler = load_template_handler(template_type)
    if not template_handler:
        logger.error(f"Unknown template type '{template_type}'")
        return False
    
    # Call the handler
    return template_handler(template_cfg, cfg)


def setup_glusterfs(cfg: LabConfig):
    """Setup GlusterFS distributed storage across Swarm nodes"""
    logger.info("\n[5/7] Setting up GlusterFS distributed storage...")
    
    proxmox_host = cfg.proxmox_host
    
    if not cfg.glusterfs:
        logger.info("GlusterFS configuration not found, skipping...")
        return True
    
    gluster_cfg = cfg.glusterfs
    volume_name = gluster_cfg.volume_name
    brick_path = gluster_cfg.brick_path
    mount_point = gluster_cfg.mount_point
    replica_count = gluster_cfg.replica_count
    
    # Get all node info - manager for management, workers for storage
    swarm_manager_configs = [c for c in cfg.containers if c.type == 'swarm-manager']
    swarm_worker_configs = [c for c in cfg.containers if c.type == 'swarm-node']
    
    if not swarm_manager_configs or not swarm_worker_configs:
        logger.error("Swarm managers or workers not found")
        return False
    
    manager = swarm_manager_configs[0]
    manager_node = (manager.id, manager.hostname, manager.ip_address)
    worker_nodes = [(w.id, w.hostname, w.ip_address) for w in swarm_worker_configs]
    # All nodes for mounting, but only workers for storage bricks
    all_nodes = [manager_node] + worker_nodes
    
    # Install GlusterFS server on all nodes (manager for management, workers for storage)
    logger.info("Installing GlusterFS server on all nodes...")
    apt_cache_containers = [c for c in cfg.containers if c.type == 'apt-cache']
    apt_cache_ip = apt_cache_containers[0].ip_address if apt_cache_containers else None
    apt_cache_port = cfg.apt_cache_port if apt_cache_ip else None
    
    # First, ensure apt sources are correct on all nodes
    for container_id, hostname, ip_address in all_nodes:
        logger.info(f"Fixing apt sources on {hostname}...")
        sources_result = pct_exec(proxmox_host, container_id,
                    "sed -i 's/oracular/plucky/g' /etc/apt/sources.list 2>/dev/null || true; "
                    "if ! grep -q '^deb.*plucky.*main' /etc/apt/sources.list; then "
                    "echo 'deb http://archive.ubuntu.com/ubuntu plucky main universe multiverse' > /etc/apt/sources.list; "
                    "echo 'deb http://archive.ubuntu.com/ubuntu plucky-updates main universe multiverse' >> /etc/apt/sources.list; "
                    "echo 'deb http://archive.ubuntu.com/ubuntu plucky-security main universe multiverse' >> /etc/apt/sources.list; "
                    "fi 2>&1",
                 check=False, capture_output=True, cfg=cfg)
        if sources_result and "error" in sources_result.lower():
            logger.warning(f"Apt sources fix had issues on {hostname}: {sources_result[-200:]}")
    
    for container_id, hostname, ip_address in all_nodes:
        logger.info(f"Installing on {hostname}...")
        
        # Try with apt-cache first, then without if it fails
        install_success = False
        max_retries = 2
        
        for attempt in range(1, max_retries + 1):
            if attempt == 1 and apt_cache_ip and apt_cache_port:
                # Try with apt-cache
                logger.info(f"Attempt {attempt}: Using apt-cache proxy...")
                proxy_result = pct_exec(proxmox_host, container_id,
                            f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true 2>&1",
                            check=False, capture_output=True, timeout=10, cfg=cfg)
                if proxy_result and "error" in proxy_result.lower():
                    logger.warning(f"Proxy configuration had issues: {proxy_result[-200:]}")
            else:
                # Remove proxy and try without
                logger.info(f"Attempt {attempt}: Removing proxy and trying direct...")
                rm_proxy_result = pct_exec(proxmox_host, container_id,
                        "rm -f /etc/apt/apt.conf.d/01proxy 2>&1",
                        check=False, capture_output=True, timeout=10, cfg=cfg)
                if rm_proxy_result and "error" in rm_proxy_result.lower():
                    logger.warning(f"Proxy removal had issues: {rm_proxy_result[-200:]}")
            
            # Update package lists
            logger.info("Updating package lists...")
            update_cmd = Apt.update_cmd(quiet=True)
            update_output = pct_exec(proxmox_host, container_id, update_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
            update_result = CommandWrapper.parse_result(update_output)
            
            if update_result.has_error or (update_result.output and ("Failed to fetch" in update_result.output or "Unable to connect" in update_result.output)):
                logger.warning("apt update failed, will retry without proxy...")
                if attempt < max_retries:
                    continue
            
            # Install GlusterFS
            logger.info("Installing glusterfs-server and glusterfs-client...")
            install_cmd = Apt.install_cmd(["glusterfs-server", "glusterfs-client"])
            install_output = pct_exec(proxmox_host, container_id, install_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
            install_result = CommandWrapper.parse_result(install_output)
            
            # Verify installation
            verify_cmd = Gluster.is_installed_check_cmd("gluster")
            verify_output = pct_exec(proxmox_host, container_id, verify_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            
            if Gluster.parse_is_installed(verify_output):
                logger.info(f"GlusterFS installed successfully")
                install_success = True
                break
            else:
                if install_result.has_error:
                    logger.warning(f"Installation attempt {attempt} failed: {install_result.error_type.value} - {install_result.error_message}")
                if attempt < max_retries:
                    logger.info("Retrying without proxy...")
                    time.sleep(2)
        
        if not install_success:
            logger.error(f"Failed to install GlusterFS on {hostname} after {max_retries} attempts")
            return False
        
        # Start and enable glusterd
        logger.info(f"Starting glusterd service...")
        glusterd_start_cmd = SystemCtl.enable_and_start_cmd("glusterd")
        glusterd_start_output = pct_exec(proxmox_host, container_id, glusterd_start_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
        glusterd_start_result = CommandWrapper.parse_result(glusterd_start_output)
        if glusterd_start_result.has_error:
            logger.error(f"Failed to start glusterd on {hostname}: {glusterd_start_result.error_type.value} - {glusterd_start_result.error_message}")
            return False
        
        # Verify glusterd is running
        time.sleep(3)
        is_active_cmd = SystemCtl.is_active_check_cmd("glusterd")
        glusterd_check_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        
        if SystemCtl.parse_is_active(glusterd_check_output):
            logger.info(f"{hostname}: GlusterFS installed and glusterd running")
        else:
            logger.error(f"{hostname}: GlusterFS installed but glusterd is not running: {glusterd_check_output}")
            return False
    
    time.sleep(cfg.waits.glusterfs_setup)
    
    # Create brick directories (only on worker nodes)
    logger.info("Creating brick directories on worker nodes...")
    for worker in swarm_worker_configs:
        container_id = worker.id
        hostname = worker.hostname
        logger.info(f"Creating brick on {hostname}...")
        brick_result = pct_exec(proxmox_host, container_id,
                    f"mkdir -p {brick_path} && chmod 755 {brick_path} 2>&1",
                 check=False, capture_output=True, cfg=cfg)
        if brick_result and "error" in brick_result.lower():
            logger.error(f"Failed to create brick directory on {hostname}: {brick_result[-300:]}")
            return False
    
    # Peer nodes together (from manager)
    manager_id = manager.id
    manager_hostname = manager.hostname
    manager_ip = manager.ip_address
    
    # Find gluster command path - try multiple common locations
    find_gluster_cmd = Gluster.find_gluster_cmd()
    gluster_path = pct_exec(proxmox_host, manager_id, find_gluster_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    gluster_cmd = gluster_path.strip() if gluster_path and gluster_path.strip() else "gluster"
    
    logger.info("Peering worker nodes together...")
    for worker in swarm_worker_configs:
        container_id = worker.id
        hostname = worker.hostname
        ip_address = worker.ip_address
        logger.info(f"Adding {hostname} ({ip_address}) to cluster...")
        # Try to probe, ignore if already connected
        probe_cmd = f"{Gluster.peer_probe_cmd(gluster_cmd, hostname)} || {Gluster.peer_probe_cmd(gluster_cmd, ip_address)}"
        probe_output = pct_exec(proxmox_host, manager_id, probe_cmd, check=False, capture_output=True, cfg=cfg)
        probe_result = CommandWrapper.parse_result(probe_output)
        if probe_result.has_error and "already" not in (probe_output or "").lower() and "already in peer list" not in (probe_output or "").lower():
            logger.warning(f"Peer probe had issues for {hostname}: {probe_result.error_type.value} - {probe_result.error_message}")
    
    time.sleep(10)  # Wait longer for peers to fully connect
    
    # Verify peer status - wait until all peers are connected
    logger.info("Verifying peer status...")
    max_peer_attempts = 10
    for attempt in range(1, max_peer_attempts + 1):
        peer_status_cmd = Gluster.peer_status_cmd(gluster_cmd)
        peer_status = pct_exec(proxmox_host, manager_id, peer_status_cmd, check=False, capture_output=True, cfg=cfg)
        if peer_status:
            logger.info(peer_status)
        # Check if all peers are connected
        connected_count = peer_status.count("Peer in Cluster (Connected)")
        if connected_count >= len(swarm_worker_configs):  # All workers connected
                logger.info(f"All {connected_count} worker peers connected")
                break
        if attempt < max_peer_attempts:
            logger.info(f"Waiting for peers to connect... ({attempt}/{max_peer_attempts})")
        time.sleep(3)
    else:
        logger.warning("Not all peers may be fully connected, continuing anyway...")
    
    # Create volume (only if it doesn't exist)
    logger.info(f"Creating GlusterFS volume '{volume_name}'...")
    volume_exists_cmd = Gluster.volume_exists_check_cmd(gluster_cmd, volume_name)
    volume_exists_output = pct_exec(proxmox_host, manager_id, volume_exists_cmd, check=False, capture_output=True, cfg=cfg)
    
    if not Gluster.parse_volume_exists(volume_exists_output):
        # Build volume create command - use IP addresses for reliability (only worker nodes)
        brick_list = [f"{w.ip_address}:{brick_path}" for w in swarm_worker_configs]
        create_cmd = Gluster.volume_create_cmd(gluster_cmd, volume_name, replica_count, brick_list, force=True)
        create_output = pct_exec(proxmox_host, manager_id, create_cmd, check=False, capture_output=True, cfg=cfg)
        create_result = CommandWrapper.parse_result(create_output)
        logger.info(f"{create_output}")
        
        # Check if creation was successful
        if create_result.success or "created" in (create_output or "").lower() or "success" in (create_output or "").lower():
            # Start volume
            logger.info(f"Starting volume '{volume_name}'...")
            start_cmd = Gluster.volume_start_cmd(gluster_cmd, volume_name)
            start_output = pct_exec(proxmox_host, manager_id, start_cmd, check=False, capture_output=True, cfg=cfg)
            logger.info(f"{start_output}")
        else:
            logger.error(f"Volume creation failed: {create_result.error_type.value} - {create_result.error_message}")
            return False
    else:
        logger.info(f"Volume '{volume_name}' already exists")
    
    # Verify volume status
    logger.info("Verifying volume status...")
    vol_status_cmd = Gluster.volume_status_cmd(gluster_cmd, volume_name)
    vol_status = pct_exec(proxmox_host, manager_id, vol_status_cmd, check=False, capture_output=True, cfg=cfg)
    if vol_status:
        logger.info(vol_status)
    
    # Mount GlusterFS volume on all nodes (for access, not storage)
    logger.info("Mounting GlusterFS volume on all nodes...")
    for node in [manager] + swarm_worker_configs:
        container_id = node.id
        hostname = node.hostname
        ip_address = node.ip_address
        logger.info(f"Mounting on {hostname}...")
        # Create mount point
        mkdir_result = pct_exec(proxmox_host, container_id,
                f"mkdir -p {mount_point} 2>&1",
                check=False, capture_output=True, cfg=cfg)
        if mkdir_result and "error" in mkdir_result.lower():
            logger.error(f"Failed to create mount point on {hostname}: {mkdir_result[-300:]}")
            return False
    
        # Add to fstab for persistence
        fstab_entry = f"{manager_hostname}:/{volume_name} {mount_point} glusterfs defaults,_netdev 0 0"
        fstab_result = pct_exec(proxmox_host, container_id,
                f"grep -q '{mount_point}' /etc/fstab || echo '{fstab_entry}' >> /etc/fstab 2>&1",
                check=False, capture_output=True, cfg=cfg)
        if fstab_result and "error" in fstab_result.lower():
            logger.warning(f"fstab update had issues on {hostname}: {fstab_result[-200:]}")
    
        # Mount
        mount_result = pct_exec(proxmox_host, container_id,
                f"mount -t glusterfs {manager_hostname}:/{volume_name} {mount_point} 2>&1 || "
                f"mount -t glusterfs {manager.ip_address}:/{volume_name} {mount_point} 2>&1",
             check=False, capture_output=True, cfg=cfg)
        if mount_result and "error" in mount_result.lower() and "already mounted" not in mount_result.lower():
            logger.error(f"Failed to mount GlusterFS on {hostname}: {mount_result[-300:]}")
            return False
    
        # Verify mount - check if it's actually mounted
        mount_verify = pct_exec(proxmox_host, container_id,
                              f"mount | grep -q '{mount_point}' && mount | grep '{mount_point}' | grep -q gluster && echo mounted || echo not_mounted",
                          check=False, capture_output=True, cfg=cfg)
        if "mounted" in mount_verify and "not_mounted" not in mount_verify:
            logger.info(f"{hostname}: Volume mounted successfully")
        else:
            # Check what actually happened
            mount_info = pct_exec(proxmox_host, container_id,
                                  f"mount | grep {mount_point} 2>/dev/null || echo 'NOT_MOUNTED'",
                                  check=False, capture_output=True, cfg=cfg)
            if "NOT_MOUNTED" in mount_info or not mount_info:
                logger.error(f"{hostname}: Mount failed - volume not mounted")
            else:
                logger.warning(f"{hostname}: Mount status unclear - {mount_info[:80]}")
    
    logger.info("GlusterFS distributed storage setup complete")
    logger.info(f"  Volume: {volume_name}")
    logger.info(f"  Mount point: {mount_point} on all nodes")
    logger.info(f"  Replication: {replica_count}x")
    return True


def deploy_swarm(cfg: LabConfig):
    """Deploy Docker Swarm"""
    proxmox_host = cfg.proxmox_host
    gateway = cfg.gateway
    
    # Get swarm container configs from containers list
    swarm_manager_configs = [c for c in cfg.containers if c.type == 'swarm-manager']
    swarm_worker_configs = [c for c in cfg.containers if c.type == 'swarm-node']
    
    if not swarm_manager_configs or not swarm_worker_configs:
        logger.error("Swarm manager or worker containers not found in configuration")
        return False
    
    # Get Docker template path
    template_path = get_template_path('docker-tmpl', cfg)
    logger.info(f"Using template: {template_path}")
    
    # Deploy all swarm containers (managers + workers)
    all_swarm_configs = swarm_manager_configs + swarm_worker_configs
    
    for container_cfg in all_swarm_configs:
        container_id = container_cfg.id
        hostname = container_cfg.hostname
        ip_address = container_cfg.ip_address
        logger.info(f"\nDeploying container {container_id} ({hostname})...")
        
        # Destroy if exists
        if container_exists(proxmox_host, container_id, cfg=cfg):
            logger.info(f"Destroying existing container {container_id}...")
            destroy_container(proxmox_host, container_id, cfg=cfg)
        
        # Get container resources from container config
        resources = container_cfg.resources
        if not resources:
            # Default fallback
            from libs.config import ContainerResources
            resources = ContainerResources(memory=4096, swap=4096, cores=8, rootfs_size=40)
        storage = cfg.proxmox_storage
        bridge = cfg.proxmox_bridge
        
        # Create container
        logger.info(f"Creating container {container_id} from template...")
        create_cmd = PCT.create_cmd(
            container_id=container_id,
            template_path=template_path,
            hostname=hostname,
            memory=resources.memory,
            swap=resources.swap,
            cores=resources.cores,
            ip_address=ip_address,
            gateway=gateway,
            bridge=bridge,
            storage=storage,
            rootfs_size=resources.rootfs_size,
            unprivileged=False,
            ostype="ubuntu",
            arch="amd64"
        )
        create_output = ssh_exec(proxmox_host, create_cmd, check=False, capture_output=True, cfg=cfg)
        create_result = CommandWrapper.parse_result(create_output)
        if not create_result:
            logger.error(f"Failed to create container: {create_result.error_type.value} - {create_result.error_message}")
            return False
    
    # Configure features
        logger.info("Configuring container features...")
        features_cmd = PCT.set_features_cmd(container_id, nesting=True, keyctl=True, fuse=True)
        features_output = ssh_exec(proxmox_host, features_cmd, check=False, capture_output=True, cfg=cfg)
        features_result = CommandWrapper.parse_result(features_output)
        if features_result.has_error:
            logger.warning(f"Failed to set container features: {features_result.error_type.value} - {features_result.error_message}")
        
        # Configure sysctl for manager
        is_manager = container_cfg.type == 'swarm-manager'
        if is_manager:
            logger.info("Configuring LXC container for sysctl access...")
            sysctl1_result = ssh_exec(proxmox_host, f"pct set {container_id} -lxc.cgroup2.devices.allow 'c 10:200 rwm' 2>/dev/null || true 2>&1",
                                      check=False, capture_output=True, cfg=cfg)
            sysctl2_result = ssh_exec(proxmox_host, f"pct set {container_id} -lxc.mount.auto 'proc:rw sys:rw' 2>/dev/null || true 2>&1",
                                      check=False, capture_output=True, cfg=cfg)
            if sysctl1_result and "error" in sysctl1_result.lower():
                logger.warning(f"Sysctl configuration had issues: {sysctl1_result[-200:]}")
    
    # Start container
        logger.info("Starting container...")
        start_cmd = PCT.start_cmd(container_id)
        start_output = ssh_exec(proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg)
        start_result = CommandWrapper.parse_result(start_output)
        # Check for explicit errors in output
        if start_result.has_error:
            logger.error(f"Failed to start container: {start_result.error_type.value} - {start_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        # Verify container is actually running
        time.sleep(cfg.waits.container_startup)
        status_cmd = PCT.status_cmd(container_id)
        status_output = ssh_exec(proxmox_host, status_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if not PCT.parse_status_output(status_output, container_id):
            logger.error(f"Container {container_id} is not running after start. Status: {status_output}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    # Wait for container
        if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
            logger.error("Container did not become ready")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        
        # Setup SSH key
        logger.info("Setting up SSH key...")
        if not setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
            logger.error("Failed to setup SSH key")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        
        # Configure apt cache for deployed nodes
        apt_cache_containers = [c for c in cfg.containers if c.type == 'apt-cache']
        if apt_cache_containers:
            apt_cache_ip = apt_cache_containers[0].ip_address
            apt_cache_port = cfg.apt_cache_port
            logger.info("Configuring apt cache...")
            apt_cache_result = pct_exec(proxmox_host, container_id,
                        f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true 2>&1",
                    check=False, capture_output=True, cfg=cfg)
            if apt_cache_result and "error" in apt_cache_result.lower():
                logger.warning(f"Apt cache configuration had issues: {apt_cache_result[-200:]}")
    
    # Verify Docker
        logger.info("Verifying Docker installation...")
        # Find docker command path first - try multiple common locations
        find_docker_cmd = Docker.find_docker_cmd()
        docker_path = pct_exec(proxmox_host, container_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
        docker_verify = pct_exec(proxmox_host, container_id, 
                               f"command -v {docker_cmd} >/dev/null 2>&1 && {docker_cmd} --version && {docker_cmd} ps 2>&1 | head -5 || echo 'Docker not found'",
                          check=False, capture_output=True, cfg=cfg)
        if "Docker not found" in docker_verify or "docker" not in docker_verify.lower():
            logger.info("Docker not installed, installing Docker...")
            # Use Docker's official installation script
            docker_install_cmd = (
                "rm -f /etc/apt/apt.conf.d/01proxy; "
                "DEBIAN_FRONTEND=noninteractive apt update -qq 2>&1 && "
                "if command -v curl >/dev/null 2>&1; then "
                "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh 2>&1 && "
                "  sh /tmp/get-docker.sh 2>&1 | tail -20 || "
                "  (echo 'get.docker.com failed, trying docker.io...' && "
                "   DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20); "
                "else "
                "  echo 'curl not available, installing docker.io...'; "
                "  DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20; "
                "fi"
            )
            install_result = pct_exec(proxmox_host, container_id, docker_install_cmd,
                                    check=False, capture_output=True, timeout=300, cfg=cfg)
            
            # Find docker command path after installation
            docker_path = pct_exec(proxmox_host, container_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
            
            # Verify Docker was installed
            docker_check_cmd = Docker.is_installed_check_cmd(docker_cmd)
            docker_check_output = pct_exec(proxmox_host, container_id, docker_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if Docker.parse_is_installed(docker_check_output):
                logger.info("Docker installed successfully")
            else:
                logger.warning("Docker installation may have failed")
        else:
            # Docker already installed, find its path
            docker_path = pct_exec(proxmox_host, container_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
        
        # Start Docker
        logger.info("Starting Docker service...")
        docker_start_cmd = SystemCtl.enable_and_start_cmd("docker")
        docker_start_output = pct_exec(proxmox_host, container_id, docker_start_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
        docker_start_result = CommandWrapper.parse_result(docker_start_output)
        if docker_start_result.has_error:
            logger.error(f"Failed to start Docker service: {docker_start_result.error_type.value} - {docker_start_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        
        # Verify Docker is running
        time.sleep(3)
        is_active_cmd = SystemCtl.is_active_check_cmd("docker")
        docker_status_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if SystemCtl.parse_is_active(docker_status_output):
            logger.info("Docker service is running")
        else:
            logger.error(f"Docker service is not running: {docker_status_output}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        
        # Manager-specific setup
        if is_manager:
            logger.info("Ensuring SSH service is running on manager...")
            ssh_result = pct_exec(proxmox_host, container_id, "systemctl start ssh 2>/dev/null || true 2>&1",
                                 check=False, capture_output=True, cfg=cfg)
            if ssh_result and "error" in ssh_result.lower():
                logger.warning(f"SSH start had issues: {ssh_result[-200:]}")
            logger.info("Configuring sysctl for Docker containers...")
            sysctl_result = pct_exec(proxmox_host, container_id,
                    "sysctl -w net.ipv4.ip_unprivileged_port_start=0 2>/dev/null || true; "
                    "echo 'net.ipv4.ip_unprivileged_port_start=0' >> /etc/sysctl.conf 2>/dev/null || true 2>&1",
                    check=False, capture_output=True, cfg=cfg)
            if sysctl_result and "error" in sysctl_result.lower():
                logger.warning(f"Sysctl configuration had issues: {sysctl_result[-200:]}")
            time.sleep(cfg.waits.network_config)
        
        logger.info(f"Container {container_id} ({hostname}) deployed successfully")
    
    # Ensure Docker is installed and running on manager (after all containers are created)
    manager_config = swarm_manager_configs[0]
    manager_id = manager_config.id
    
    # Check if Docker is installed
    find_docker_cmd = Docker.find_docker_cmd()
    docker_path = pct_exec(proxmox_host, manager_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
    docker_check_cmd = Docker.is_installed_check_cmd(docker_cmd)
    docker_check_output = pct_exec(proxmox_host, manager_id, docker_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    
    if not Docker.parse_is_installed(docker_check_output):
        logger.info("\nInstalling Docker on manager...")
        docker_install_cmd = (
            "rm -f /etc/apt/apt.conf.d/01proxy; "
            f"{Apt.update_cmd(quiet=True)} && "
            "if command -v curl >/dev/null 2>&1; then "
            "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh 2>&1 && "
            "  sh /tmp/get-docker.sh 2>&1 | tail -20 || "
            "  (echo 'get.docker.com failed, trying docker.io...' && "
            f"   {Apt.install_cmd(['docker.io'])} | tail -20); "
            "else "
            "  echo 'curl not available, installing docker.io...'; "
            f"  {Apt.install_cmd(['docker.io'])} | tail -20; "
            "fi"
        )
        install_output = pct_exec(proxmox_host, manager_id, docker_install_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
        install_result = CommandWrapper.parse_result(install_output)
        
        # Verify Docker was installed
        docker_check_output = pct_exec(proxmox_host, manager_id, docker_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if Docker.parse_is_installed(docker_check_output):
            logger.info("Docker installed successfully")
        else:
            logger.warning("Docker installation may have failed")
    
    # Start Docker service
    logger.info("Starting Docker service on manager...")
    docker_start_cmd = SystemCtl.enable_and_start_cmd("docker")
    pct_exec(proxmox_host, manager_id, f"{docker_start_cmd} && systemctl status docker --no-pager | head -5", check=False, cfg=cfg)
    
    time.sleep(cfg.waits.swarm_init)
    
    # Initialize Swarm (use the first manager config)
    manager_config = swarm_manager_configs[0]
    manager_id = manager_config.id
    manager_ip = manager_config.ip_address
    manager_hostname = manager_config.hostname
    
    # Find docker command path
    find_docker_cmd = Docker.find_docker_cmd()
    docker_path = pct_exec(proxmox_host, manager_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
    
    logger.info("\nInitializing Docker Swarm on manager node...")
    swarm_init_cmd = Docker.swarm_init_cmd(docker_cmd, manager_ip)
    swarm_init_output = pct_exec(proxmox_host, manager_id, swarm_init_cmd, check=False, capture_output=True, cfg=cfg)
    swarm_init_result = CommandWrapper.parse_result(swarm_init_output)
    
    if "already part of a swarm" in (swarm_init_output or ""):
        logger.info("Swarm already initialized, continuing...")
    elif swarm_init_result.has_error:
        logger.warning(f"Swarm initialization had errors: {swarm_init_result.error_type.value} - {swarm_init_result.error_message}")
    else:
        logger.info("Swarm initialized successfully")
    
    # Get worker join token
    logger.info("Getting worker join token...")
    join_token_cmd = Docker.swarm_join_token_cmd(docker_cmd, role="worker")
    join_token_output = pct_exec(proxmox_host, manager_id, join_token_cmd, check=False, capture_output=True, cfg=cfg)
    # Extract token - get the last non-empty line that looks like a token
    join_token = ""
    for line in join_token_output.strip().split('\n'):
        line = line.strip()
        if line and len(line) > 20 and not line.startswith('Error') and not line.startswith('Warning'):
            join_token = line
            break
    
    if not join_token:
        logger.error(f"Could not get worker join token. Output: {join_token_output}")
        return False
    
    # Set manager to drain
    logger.info("Setting manager node availability to drain...")
    node_update_cmd = Docker.node_update_cmd(docker_cmd, manager_hostname, "drain")
    pct_exec(proxmox_host, manager_id, node_update_cmd, check=False, cfg=cfg)
    
    # Join workers
    swarm_port = cfg.swarm_port
    
    for worker_config in swarm_worker_configs:
        worker_ip = worker_config.ip_address
        worker_hostname = worker_config.hostname
        worker_id = worker_config.id
        logger.info(f"Joining {worker_hostname} ({worker_ip}) to swarm...")
        # Find docker command path on worker
        worker_docker_path = pct_exec(proxmox_host, worker_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        worker_docker_cmd = worker_docker_path.strip() if worker_docker_path and worker_docker_path.strip() else "docker"
        # Use pct_exec
        join_cmd = Docker.swarm_join_cmd(worker_docker_cmd, join_token, f"{manager_ip}:{swarm_port}")
        join_output = pct_exec(proxmox_host, worker_id, join_cmd, check=False, capture_output=True, cfg=cfg)
        
        if "already part of a swarm" in join_output:
            logger.info(f"Node {worker_hostname} already part of swarm")
        elif "This node joined a swarm" in join_output:
            logger.info(f"Node {worker_hostname} joined swarm successfully")
        else:
            logger.warning(f"Node {worker_hostname} join had issues:")
            logger.warning(join_output)
    
    # Verify swarm
    logger.info("\nVerifying swarm status...")
    node_ls_cmd = Docker.node_ls_cmd(docker_cmd)
    pct_exec(proxmox_host, manager_id, node_ls_cmd, check=False, cfg=cfg)
    
    # Install Portainer
    logger.info("\nInstalling Portainer CE...")
    volume_create_cmd = Docker.volume_create_cmd(docker_cmd, "portainer_data")
    pct_exec(proxmox_host, manager_id, volume_create_cmd, check=False, cfg=cfg)
    
    # Remove existing portainer if it exists
    stop_cmd = Docker.stop_cmd(docker_cmd, "portainer")
    rm_cmd = Docker.rm_cmd(docker_cmd, "portainer")
    pct_exec(proxmox_host, manager_id, f"{stop_cmd}; {rm_cmd}", check=False, cfg=cfg)
    
    portainer_image = cfg.portainer_image
    portainer_port = cfg.portainer_port
    logger.info("Creating Portainer container...")
    portainer_cmd = Docker.run_cmd(
        docker_cmd,
        portainer_image,
        "portainer",
        restart="always",
        network="host",
        volumes=["/var/run/docker.sock:/var/run/docker.sock", "portainer_data:/data"]
    )
    pct_exec(proxmox_host, manager_id, portainer_cmd, check=False, cfg=cfg)
    
    time.sleep(cfg.waits.portainer_start)
    
    logger.info("Verifying Portainer is running...")
    portainer_status = pct_exec(proxmox_host, manager_id,
                               f"{docker_cmd} ps --format '{{{{.Names}}}} {{{{.Status}}}}' | grep portainer || {docker_cmd} ps -a --format '{{{{.Names}}}} {{{{.Status}}}}' | grep portainer",
                               check=False, capture_output=True, cfg=cfg)
    if portainer_status:
        logger.info(f"Portainer status: {portainer_status}")
    else:
        logger.warning("Portainer container not found")
    
    # Check Portainer logs if not running
    portainer_running = pct_exec(proxmox_host, manager_id,
                                 f"{docker_cmd} ps --format '{{{{.Names}}}}' | grep -q '^portainer$' && echo yes || echo no",
                                 check=False, capture_output=True, cfg=cfg)
    if "no" in portainer_running:
        logger.warning("Portainer failed to start. Checking logs...")
        logs = pct_exec(proxmox_host, manager_id,
                       f"{docker_cmd} logs portainer 2>&1 | tail -20",
                       check=False, capture_output=True, cfg=cfg)
        if logs:
            logger.warning(logs)
    
    logger.info("Docker Swarm deployed")
    return True


def cmd_deploy():
    """Deploy complete lab: apt-cache, templates, and Docker Swarm"""
    cfg = get_config()
    
    logger.info("=" * 50)
    logger.info("Deploying Lab Environment")
    logger.info("=" * 50)
    
    try:
        # Get apt-cache container name from config
        apt_cache_ct_name = cfg.apt_cache_ct
        
        # Create apt-cache container FIRST (before templates)
        containers = cfg.containers
        apt_cache_container = None
        for c in containers:
            if c.name == apt_cache_ct_name:
                apt_cache_container = c
                break
        
        step = 1
        templates = cfg.templates
        non_swarm_containers = [c for c in containers if c.type not in ['swarm-manager', 'swarm-node']]
        # Remove apt-cache from non_swarm_containers since we handle it separately
        non_swarm_containers = [c for c in non_swarm_containers if c.name != apt_cache_ct_name]
        
        total_steps = (1 if apt_cache_container else 0) + len(templates) + len(non_swarm_containers) + 1 + 1  # apt-cache + templates + containers + swarm + glusterfs
        
        if apt_cache_container:
            logger.info(f"\n[{step}/{total_steps}] Creating apt-cache container first...")
            # Create apt-cache using base template directly (before custom templates exist)
            # Temporarily override template to use base template
            original_template = apt_cache_container.template
            apt_cache_container.template = None  # Signal to use base template
            if not create_container(apt_cache_container, cfg, step, total_steps):
                sys.exit(1)
            # Restore original template setting
            if original_template:
                apt_cache_container.template = original_template
            
            # Verify apt-cache is running and ready before proceeding
            logger.info("Verifying apt-cache service is ready...")
            apt_cache_ip = apt_cache_container.ip_address
            apt_cache_port = cfg.apt_cache_port
            proxmox_host = cfg.proxmox_host
            container_id = apt_cache_container.id
            
            # Check if apt-cacher-ng service is running
            max_attempts = 10
            for i in range(1, max_attempts + 1):
                service_check = pct_exec(proxmox_host, container_id,
                                        "systemctl is-active apt-cacher-ng 2>/dev/null || echo 'inactive'",
                                        check=False, capture_output=True, timeout=10, cfg=cfg)
                if service_check and "active" in service_check:
                    # Test if port is accessible
                    port_check = pct_exec(proxmox_host, container_id,
                                         f"nc -z localhost {apt_cache_port} 2>/dev/null && echo 'port_open' || echo 'port_closed'",
                                         check=False, capture_output=True, timeout=10, cfg=cfg)
                    if port_check and "port_open" in port_check:
                        logger.info(f"apt-cache service is ready on {apt_cache_ip}:{apt_cache_port}")
                        break
                if i < max_attempts:
                    logger.info(f"Waiting for apt-cache service... ({i}/{max_attempts})")
                    time.sleep(3)
                else:
                    logger.error(f"apt-cache service is not ready after {max_attempts} attempts")
                    logger.error("Cannot proceed with template creation without apt-cache")
                    sys.exit(1)
        
            step += 1
        else:
            logger.error(f"\n[{step}/{total_steps}] apt-cache container '{apt_cache_ct_name}' not found in configuration")
            logger.error("Cannot proceed with template creation without apt-cache")
            sys.exit(1)
        
        # Create templates (can now use apt-cache)
        for template_cfg in templates:
            if not create_template(template_cfg, cfg, step, total_steps):
                sys.exit(1)
            step += 1
        
        # Create other containers (excluding swarm containers which are handled separately)
        for container_cfg in non_swarm_containers:
            if not create_container(container_cfg, cfg, step, total_steps):
                sys.exit(1)
            step += 1
        
        # Deploy swarm (creates swarm containers)
        swarm_step = step
        logger.info(f"\n[{swarm_step}/{total_steps}] Deploying Docker Swarm...")
        if not deploy_swarm(cfg):
            sys.exit(1)
        step += 1
        
        # Setup GlusterFS
        gluster_step = step
        logger.info(f"\n[{gluster_step}/{total_steps}] Setting up GlusterFS distributed storage...")
        if not setup_glusterfs(cfg):
            sys.exit(1)
        
        logger.info("\n" + "=" * 50)
        logger.info("Deployment Complete!")
        logger.info("=" * 50)
        logger.info(f"\nContainers:")
        for ct in containers:
            logger.info(f"  - {ct.id}: {ct.name} ({ct.ip_address})")
        
        # Show services
        manager_configs = [c for c in cfg.containers if c.type == 'swarm-manager']
        if manager_configs:
            manager = manager_configs[0]
            logger.info(f"\nPortainer: https://{manager.ip_address}:{cfg.portainer_port}")
        
        pgsql_containers = [c for c in containers if c.type == 'pgsql']
        if pgsql_containers:
            pgsql = pgsql_containers[0]
            params = pgsql.params
            logger.info(f"PostgreSQL: {pgsql.ip_address}:{params.get('port', 5432)}")
        
        haproxy_containers = [c for c in containers if c.type == 'haproxy']
        if haproxy_containers:
            haproxy = haproxy_containers[0]
            params = haproxy.params
            logger.info(f"HAProxy: http://{haproxy.ip_address}:{params.get('http_port', 80)} (Stats: http://{haproxy.ip_address}:{params.get('stats_port', 8404)})")
        if cfg.glusterfs:
            gluster_cfg = cfg.glusterfs
            logger.info(f"\nGlusterFS:")
            logger.info(f"  Volume: {gluster_cfg.volume_name}")
            logger.info(f"  Mount: {gluster_cfg.mount_point} on all nodes")
        
    except Exception as e:
        logger.error(f"Error during deployment: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def cmd_cleanup():
    """Remove all containers and templates"""
    try:
        cfg = get_config()
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    try:
        logger.info("=" * 50)
        logger.info("Cleaning Up Lab Environment")
        logger.info("=" * 50)
        logger.info("\nDestroying ALL containers and templates...")
        
        logger.info("\nStopping and destroying containers...")
        
        # Get all container IDs
        logger.info("Getting list of containers...")
        list_cmd = PCT.status_cmd()
        result = ssh_exec(cfg.proxmox_host, list_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
        
        container_ids = []
        if result:
            # Parse pct list output - format is: VMID       Status     Lock         Name
            lines = result.strip().split('\n')
            for line in lines[1:]:  # Skip header
                parts = line.split()
                if parts and parts[0].isdigit():
                    container_ids.append(parts[0])
        
        total = len(container_ids)
        if total > 0:
            logger.info(f"Found {total} containers to destroy: {', '.join(container_ids)}")
            
            for idx, cid in enumerate(container_ids, 1):
                logger.info(f"\n[{idx}/{total}] Processing container {cid}...")
                destroy_container(cfg.proxmox_host, cid, cfg=cfg)
        
            # Final verification
            logger.info("\nVerifying all containers are destroyed...")
            remaining_result = ssh_exec(cfg.proxmox_host, list_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
            remaining_ids = []
            if remaining_result:
                remaining_lines = remaining_result.strip().split('\n')
                for line in remaining_lines[1:]:  # Skip header
                    parts = line.split()
                    if parts and parts[0].isdigit():
                        remaining_ids.append(parts[0])
            
            if remaining_ids:
                logger.warning(f"{len(remaining_ids)} containers still exist: {', '.join(remaining_ids)}")
            else:
                logger.info("All containers destroyed")
        else:
            logger.info("No containers found")
        
        logger.info("\nRemoving templates...")
        template_dir = cfg.proxmox_template_dir
        logger.info(f"Cleaning template directory {template_dir}...")
        result = ssh_exec(cfg.proxmox_host,
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -print | wc -l",
        check=False, capture_output=True, cfg=cfg)
        template_count = result.strip() if result else "0"
        logger.info(f"Removing {template_count} template files...")
        ssh_exec(cfg.proxmox_host,
                f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -delete || true",
                check=False, cfg=cfg)
        logger.info("Templates removed")
        
        logger.info("\n" + "=" * 50)
        logger.info("Cleanup Complete!")
        logger.info("=" * 50)
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def cmd_status():
    """Show current lab status"""
    cfg = get_config()
    
    logger.info("=" * 50)
    logger.info("Lab Status")
    logger.info("=" * 50)
    
    # Check containers
    logger.info("\nContainers:")
    list_cmd = PCT.status_cmd()
    result = ssh_exec(cfg.proxmox_host, list_cmd, check=False, capture_output=True, cfg=cfg)
    if result:
        logger.info(result)
    else:
        logger.info("  No containers found")
    
    # Check templates
    template_dir = cfg.proxmox_template_dir
    logger.info("\nTemplates:")
    result = ssh_exec(cfg.proxmox_host,
                     f"ls -lh {template_dir}/*.tar.zst 2>/dev/null || echo 'No templates'",
                     check=False, capture_output=True, cfg=cfg)
    if result:
        logger.info(result)
    else:
        logger.info("  No templates found")
    
    # Check swarm status
    logger.info("\nDocker Swarm:")
    # Get manager from containers
    manager_configs = [c for c in cfg.containers if c.type == 'swarm-manager']
    if not manager_configs:
        logger.info("  No swarm manager found in configuration")
        return
    manager_id = manager_configs[0].id
    
    # Check if container exists before trying to run commands on it
    if not container_exists(cfg.proxmox_host, manager_id, cfg=cfg):
        logger.info("  Swarm manager container does not exist")
        return
    
    # Find docker command path
    find_docker_cmd = Docker.find_docker_cmd()
    docker_path = pct_exec(cfg.proxmox_host, manager_id, find_docker_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
    node_ls_cmd = Docker.node_ls_cmd(docker_cmd)
    result = pct_exec(cfg.proxmox_host, manager_id,
                    f"{node_ls_cmd} 2>/dev/null || echo 'Swarm not initialized or manager not available'",
                    check=False, capture_output=True, cfg=cfg)
    if result:
        logger.info(result)
    else:
        logger.info("  Swarm not available")


def main():
    """Main CLI entry point"""
    # Initialize logging
    init_logger(level=logging.INFO)
    
    parser = argparse.ArgumentParser(
        description="Lab CLI - Manage Proxmox LXC containers and Docker Swarm",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    deploy_parser = subparsers.add_parser('deploy', help='Deploy complete lab: apt-cache, templates, and Docker Swarm')
    deploy_parser.set_defaults(func=cmd_deploy)
    
    cleanup_parser = subparsers.add_parser('cleanup', help='Remove all containers and templates')
    cleanup_parser.set_defaults(func=cmd_cleanup)
    
    status_parser = subparsers.add_parser('status', help='Show current lab status')
    status_parser.set_defaults(func=cmd_status)
    
    args = parser.parse_args()
    
    if hasattr(args, 'func'):
        args.func()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
