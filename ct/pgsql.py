"""
PostgreSQL container type - creates a PostgreSQL database container
"""
import sys
import time
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
    """Create PostgreSQL container - method for type 'pgsql'"""
    proxmox_host = cfg.proxmox_host
    container_id = None
    # Use base template directly to avoid tar errors from custom template
    original_template = container_cfg.template
    container_cfg.template = None  # Use base template
    container_id = setup_container_base(container_cfg, cfg, privileged=False)
    # Restore original template setting
    if original_template:
        container_cfg.template = original_template
    if not container_id:
        logger.error(f"Failed to create container {container_cfg.id}")
        return False
    
    params = container_cfg.params
    postgresql_version = params.get('version', '17')
    postgresql_port = params.get('port', 5432)
    data_dir = params.get('data_dir', '/var/lib/postgresql/data')
    
    # Update and upgrade (already done in setup_container_base, but ensure packages are up to date)
    logger.info("Updating package lists...")
    update_cmd = Apt.update_cmd(quiet=False)
    update_output = pct_exec(proxmox_host, container_id, update_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    update_result = CommandWrapper.parse_result(update_output)
    if not update_result:
        logger.error(f"Failed to update packages: {update_result.error_type.value} - {update_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    logger.info("Upgrading to latest Ubuntu distribution (25.04)...")
    upgrade_cmd = Apt.upgrade_cmd(dist_upgrade=True)
    upgrade_output = pct_exec(proxmox_host, container_id, upgrade_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
    upgrade_result = CommandWrapper.parse_result(upgrade_output)
    if not upgrade_result:
        logger.error(f"Failed to upgrade distribution: {upgrade_result.error_type.value} - {upgrade_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Install PostgreSQL
    logger.info(f"Installing PostgreSQL {postgresql_version}...")
    install_cmd = Apt.install_cmd([f"postgresql-{postgresql_version}", "postgresql-contrib"])
    install_output = pct_exec(proxmox_host, container_id, install_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
    install_result = CommandWrapper.parse_result(install_output)
    if not install_result:
        logger.error(f"Failed to install PostgreSQL: {install_result.error_type.value} - {install_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify installation
    verify_cmd = f"command -v psql >/dev/null 2>&1 && test -d /etc/postgresql/{postgresql_version} && echo installed || echo not_installed"
    verify_output = pct_exec(proxmox_host, container_id, verify_cmd, check=False, capture_output=True, cfg=cfg)
    if verify_output and "not_installed" in verify_output:
        logger.error("PostgreSQL installation failed - command or config not found")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Configure PostgreSQL
    logger.info("Configuring PostgreSQL...")
    enable_cmd = SystemCtl.enable_and_start_cmd("postgresql")
    enable_output = pct_exec(proxmox_host, container_id, enable_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
    enable_result = CommandWrapper.parse_result(enable_output)
    if not enable_result:
        logger.error(f"Failed to start PostgreSQL service: {enable_result.error_type.value} - {enable_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    time.sleep(cfg.waits.service_start)
    
    # Verify service is running
    is_active_cmd = SystemCtl.is_active_check_cmd("postgresql")
    is_active_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, cfg=cfg)
    if not SystemCtl.parse_is_active(is_active_output):
        logger.error(f"PostgreSQL service is not running")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Configure PostgreSQL to listen on all interfaces
    logger.info("Configuring PostgreSQL network settings...")
    listen_cmd = (
        f"sed -i \"s/#listen_addresses = 'localhost'/listen_addresses = '*'/\" /etc/postgresql/{postgresql_version}/main/postgresql.conf 2>/dev/null || "
        f"sed -i \"s/listen_addresses = 'localhost'/listen_addresses = '*'/\" /etc/postgresql/{postgresql_version}/main/postgresql.conf 2>/dev/null || true 2>&1"
    )
    listen_output = pct_exec(proxmox_host, container_id, listen_cmd, check=False, capture_output=True, cfg=cfg)
    listen_result = CommandWrapper.parse_result(listen_output)
    if listen_result.has_error:
        logger.warning(f"Listen address configuration had issues: {listen_result.error_type.value} - {listen_result.error_message}")
    
    # Update pg_hba.conf to allow connections
    pg_hba_cmd = f"echo 'host all all 10.11.3.0/24 md5' >> /etc/postgresql/{postgresql_version}/main/pg_hba.conf 2>/dev/null || true 2>&1"
    pg_hba_output = pct_exec(proxmox_host, container_id, pg_hba_cmd, check=False, capture_output=True, cfg=cfg)
    pg_hba_result = CommandWrapper.parse_result(pg_hba_output)
    if pg_hba_result.has_error:
        logger.warning(f"pg_hba.conf update had issues: {pg_hba_result.error_type.value} - {pg_hba_result.error_message}")
    
    # Set PostgreSQL password
    password_cmd = f"sudo -u postgres psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\" 2>&1 || true"
    password_output = pct_exec(proxmox_host, container_id, password_cmd, check=False, capture_output=True, cfg=cfg)
    password_result = CommandWrapper.parse_result(password_output)
    if password_result.has_error and password_result.error_type.value not in ["already_exists"]:
        logger.warning(f"Password setting had issues: {password_result.error_type.value} - {password_result.error_message}")
    
    # Restart PostgreSQL
    restart_cmd = SystemCtl.restart_cmd("postgresql")
    restart_output = pct_exec(proxmox_host, container_id, restart_cmd, check=False, capture_output=True, cfg=cfg)
    restart_result = CommandWrapper.parse_result(restart_output)
    if not restart_result:
        logger.error(f"Failed to restart PostgreSQL: {restart_result.error_type.value} - {restart_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    time.sleep(cfg.waits.service_start)
    
    # Verify PostgreSQL is running
    is_active_cmd = SystemCtl.is_active_check_cmd("postgresql")
    is_active_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, cfg=cfg)
    if not SystemCtl.parse_is_active(is_active_output):
        logger.error(f"PostgreSQL is not running")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    logger.info("PostgreSQL installed and running")
    logger.info(f"PostgreSQL container '{container_cfg.name}' created successfully")
    return True
