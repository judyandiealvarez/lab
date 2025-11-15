"""
Ubuntu template type - creates a base Ubuntu template
"""
import sys
import time
import logging
from datetime import datetime

# Import helper functions from libs
from libs import common, template
from libs.config import LabConfig, TemplateConfig
from cli import PCT, Vzdump, Apt, SystemCtl, CommandWrapper

# Default timeout for long-running apt/dpkg commands (seconds)
APT_LONG_TIMEOUT = 600

# Get logger for this module
logger = logging.getLogger(__name__)

ssh_exec = common.ssh_exec
pct_exec = common.pct_exec
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
setup_ssh_key = common.setup_ssh_key
get_base_template = template.get_base_template


def create_template(template_cfg: TemplateConfig, cfg: LabConfig):
    """Create Ubuntu template - method for type 'ubuntu'"""
    proxmox_host = cfg.proxmox_host
    container_id = template_cfg.id
    ip_address = template_cfg.ip_address
    hostname = template_cfg.hostname
    gateway = cfg.gateway
    # Get apt-cache IP from containers (may not exist yet during template creation)
    apt_cache_containers = [c for c in cfg.containers if c.type == 'apt-cache']
    apt_cache_ip = apt_cache_containers[0].ip_address if apt_cache_containers else None
    template_name = template_cfg.name
    
    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    # Get container resources and settings
    resources = template_cfg.resources
    if not resources:
        # Default fallback
        from libs.config import ContainerResources
        resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)
    storage = cfg.proxmox_storage
    bridge = cfg.proxmox_bridge
    template_dir = cfg.proxmox_template_dir
    base_template = get_base_template(proxmox_host, cfg)
    if not base_template:
        logger.error("Failed to get base template")
        return False
    
    # Create container
    logger.info(f"Creating container {container_id}...")
    create_cmd = PCT.create_cmd(
        container_id=container_id,
        template_path=f"{template_dir}/{base_template}",
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
    
    # Start container
    logger.info("Starting container...")
    start_cmd = PCT.start_cmd(container_id)
    start_output = ssh_exec(proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg)
    start_result = CommandWrapper.parse_result(start_output)
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
    
    # Configure apt cache FIRST before any apt operations (if apt-cache exists)
    if apt_cache_ip:
        logger.info("Configuring apt cache...")
        apt_cache_port = cfg.apt_cache_port
        apt_cache_cmd = f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy 2>&1 || true"
        apt_cache_output = pct_exec(proxmox_host, container_id, apt_cache_cmd, check=False, capture_output=True, cfg=cfg)
        apt_cache_result = CommandWrapper.parse_result(apt_cache_output)
        if apt_cache_result.has_error:
            logger.warning(f"Failed to configure apt cache: {apt_cache_result.error_type.value} - {apt_cache_result.error_message}")
    
    # Disable AppArmor parser to avoid maintainer script failures inside LXC
    logger.info("Disabling AppArmor parser to avoid reload errors...")
    disable_apparmor_cmd = (
        "APPARMOR_BIN=/usr/sbin/apparmor_parser; "
        "if command -v dpkg-divert >/dev/null 2>&1 && [ -x \"$APPARMOR_BIN\" ]; then "
        "  dpkg-divert --quiet --local --rename --add \"$APPARMOR_BIN\" >/dev/null 2>&1 || true; "
        "  cat <<'EOF' >\"$APPARMOR_BIN\"\n"
        "#!/bin/sh\n"
        "if [ \"$1\" = \"--version\" ] || [ \"$1\" = \"-V\" ]; then\n"
        "  exec /usr/sbin/apparmor_parser.distrib \"$@\"\n"
        "fi\n"
        "exit 0\n"
        "EOF\n"
        "  chmod +x \"$APPARMOR_BIN\"; "
        "fi"
    )
    disable_output = pct_exec(proxmox_host, container_id, disable_apparmor_cmd, check=False, capture_output=True, timeout=15, cfg=cfg)
    disable_result = CommandWrapper.parse_result(disable_output)
    if disable_result.has_error:
        logger.warning(f"Failed to stub AppArmor parser: {disable_result.error_type.value} - {disable_result.error_message}")
    
    # Fix apt sources
    logger.info("Fixing apt sources...")
    apt_sources_cmd = (
        "sed -i 's/oracular/plucky/g' /etc/apt/sources.list || true; "
        "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true 2>&1"
    )
    apt_sources_output = pct_exec(proxmox_host, container_id, apt_sources_cmd, check=False, capture_output=True, cfg=cfg)
    apt_sources_result = CommandWrapper.parse_result(apt_sources_output)
    if apt_sources_result.has_error:
        logger.warning(f"Apt sources fix had issues: {apt_sources_result.error_type.value} - {apt_sources_result.error_message}")
    
    # Setup user and SSH
    default_user = cfg.users.default_user
    sudo_group = cfg.users.sudo_group
    logger.info("Setting up user and SSH access...")
    user_setup_cmd = (
        f"apt-get update -qq || true; "
        f"id -u {default_user} >/dev/null 2>&1 || useradd -m -s /bin/bash -G {sudo_group} {default_user}; "
        f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/{default_user}; "
        f"chmod 440 /etc/sudoers.d/{default_user}; "
        f"mkdir -p /home/{default_user}/.ssh /root/.ssh; chmod 700 /home/{default_user}/.ssh; "
        "apt-get install -y -qq openssh-server >/dev/null 2>&1 || true; "
        "systemctl enable ssh >/dev/null 2>&1 || true; "
        "systemctl start ssh >/dev/null 2>&1 || true 2>&1"
    )
    user_setup_output = pct_exec(proxmox_host, container_id, user_setup_cmd, check=False, capture_output=True, timeout=APT_LONG_TIMEOUT, cfg=cfg)
    user_setup_result = CommandWrapper.parse_result(user_setup_output)
    if user_setup_result.has_error and "User exists" not in (user_setup_result.output or ""):
        logger.warning(f"User setup had some issues: {user_setup_result.error_type.value} - {user_setup_result.error_message}")
    
    if not setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
        logger.error("Failed to setup SSH key")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Upgrade distribution
    logger.info("Upgrading distribution to latest (25.04)...")
    upgrade_cmd = (
        f"apt-get update -qq || true; "
        f"DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y 2>&1"
    )
    upgrade_output = pct_exec(proxmox_host, container_id, upgrade_cmd, check=False, capture_output=True, timeout=APT_LONG_TIMEOUT, cfg=cfg)
    upgrade_result = CommandWrapper.parse_result(upgrade_output)
    if upgrade_result.has_error or (upgrade_result.output and "E:" in upgrade_result.output):
        logger.error(f"Failed to upgrade distribution: {upgrade_result.error_type.value} - {upgrade_result.error_message}")
        logger.error(f"Full upgrade output: {upgrade_output}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Install base tools
    logger.info("Installing minimal base tools...")
    tools_cmd = "apt-get install -y -qq ca-certificates curl 2>&1"
    tools_output = pct_exec(proxmox_host, container_id, tools_cmd, check=False, capture_output=True, timeout=APT_LONG_TIMEOUT, cfg=cfg)
    tools_result = CommandWrapper.parse_result(tools_output)
    if tools_result.has_error or (tools_result.output and "E:" in tools_result.output):
        logger.error(f"Failed to install base tools: {tools_result.error_type.value} - {tools_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Cleanup for template
    logger.info("Cleanup for template...")
    cleanup_cmd = (
        f"bash -c '"
        f"rm -f /etc/apt/apt.conf.d/01proxy || true; "
        f"rm -f /etc/ssh/ssh_host_* || true; "
        f"truncate -s 0 /etc/machine-id || true; "
        f"rm -f /var/lib/dbus/machine-id || true; "
        f"ln -s /etc/machine-id /var/lib/dbus/machine-id || true; "
        f"apt-get clean; "
        f"rm -rf /var/lib/apt/lists/* || true; "
        f"find /var/log -type f -name \"*.log\" -delete 2>/dev/null || true; "
        f"find /var/log -type f -name \"*.gz\" -delete 2>/dev/null || true; "
        f"truncate -s 0 /root/.bash_history 2>/dev/null || true; "
        f"truncate -s 0 /home/{cfg.users.default_user}/.bash_history 2>/dev/null || true' 2>&1"
    )
    cleanup_output = pct_exec(proxmox_host, container_id, cleanup_cmd, check=False, capture_output=True, cfg=cfg)
    cleanup_result = CommandWrapper.parse_result(cleanup_output)
    if cleanup_result.has_error:
        logger.warning(f"Cleanup had some errors: {cleanup_result.error_type.value} - {cleanup_result.error_message}")
    
    # Stop container
    logger.info("Stopping container...")
    stop_cmd = PCT.stop_cmd(container_id)
    stop_output = ssh_exec(proxmox_host, stop_cmd, check=False, capture_output=True, cfg=cfg)
    stop_result = CommandWrapper.parse_result(stop_output)
    if stop_result.has_error and "already stopped" not in (stop_result.output or ""):
        logger.warning(f"Stop container had issues: {stop_result.error_type.value} - {stop_result.error_message}")
        # Try to force stop
        force_stop_cmd = PCT.stop_cmd(container_id, force=True)
        ssh_exec(proxmox_host, force_stop_cmd, check=False, cfg=cfg)
    time.sleep(2)
    
    # Create template - must complete successfully
    template_dir = cfg.proxmox_template_dir
    logger.info("Creating template archive...")
    vzdump_cmd = Vzdump.create_template_cmd(container_id, template_dir, compress="zstd", mode="stop")
    vzdump_output = ssh_exec(proxmox_host, vzdump_cmd, check=False, capture_output=True, cfg=cfg)
    
    # Check if vzdump succeeded
    if not vzdump_output:
        logger.error("vzdump produced no output")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    vzdump_result = CommandWrapper.parse_result(vzdump_output)
    if vzdump_result.has_error:
        logger.error(f"vzdump failed: {vzdump_result.error_type.value} - {vzdump_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Check for success indicators
    vzdump_upper = vzdump_output.upper()
    if "FINISHED" not in vzdump_upper and "archive" not in vzdump_output.lower():
        logger.warning("vzdump output doesn't show clear success, but no errors found")
    
    # Wait for archive file to be created and stable (not growing)
    logger.info("Waiting for template archive to be ready...")
    max_wait = 120  # Increased timeout for large containers
    wait_count = 0
    last_size = 0
    stable_count = 0
    backup_file = None
    
    while wait_count < max_wait:
        time.sleep(2)
        wait_count += 2
        # Find the archive file
        find_archive_cmd = Vzdump.find_archive_cmd(template_dir, container_id)
        backup_file = ssh_exec(proxmox_host, find_archive_cmd, check=False, capture_output=True, cfg=cfg)
        if backup_file:
            backup_file = backup_file.strip()
            # Check file size
            size_cmd = Vzdump.get_archive_size_cmd(backup_file)
            size_check = ssh_exec(proxmox_host, size_cmd, check=False, capture_output=True, cfg=cfg)
            if size_check:
                current_size = Vzdump.parse_archive_size(size_check)
                if current_size and current_size > 0:
                    if current_size == last_size:
                        stable_count += 1
                        if stable_count >= 3:  # File size stable for 6 seconds
                            break
                    else:
                        stable_count = 0
                        last_size = current_size
    
    if not backup_file:
        logger.error("Template archive file not found after vzdump")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify archive is not empty and has reasonable size (> 10MB)
    size_cmd = Vzdump.get_archive_size_cmd(backup_file)
    size_check = ssh_exec(proxmox_host, size_cmd, check=False, capture_output=True, cfg=cfg)
    if not size_check:
        logger.error("Failed to get archive file size")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    file_size = Vzdump.parse_archive_size(size_check)
    if not file_size or file_size < 10485760:  # Less than 10MB is suspicious
        logger.error(f"Template archive is too small ({file_size} bytes if found), likely corrupted")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    logger.info(f"Template archive size: {file_size / 1048576:.2f} MB")
    
    # Rename template
    template_pattern = cfg.template_config.patterns['ubuntu']
    final_template_name = template_pattern.replace('{date}', datetime.now().strftime('%Y%m%d'))
    rename_cmd = f"mv '{backup_file}' {template_dir}/{final_template_name} && ls -lh {template_dir}/{final_template_name} 2>&1"
    rename_output = ssh_exec(proxmox_host, rename_cmd, check=False, capture_output=True, cfg=cfg)
    rename_result = CommandWrapper.parse_result(rename_output)
    
    if not rename_result:
        logger.error(f"Failed to rename template archive: {rename_result.error_type.value if rename_result else 'No output'}")
        # Verify file was actually moved
        verify_cmd = f"test -f {template_dir}/{final_template_name} && echo exists || echo missing"
        verify_output = ssh_exec(proxmox_host, verify_cmd, check=False, capture_output=True, cfg=cfg)
        if not verify_output or "exists" not in verify_output:
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    # Update template list
    pveam_cmd = "pveam update 2>&1"
    pveam_output = ssh_exec(proxmox_host, pveam_cmd, check=False, capture_output=True, cfg=cfg)
    pveam_result = CommandWrapper.parse_result(pveam_output)
    if pveam_result.has_error:
        logger.warning(f"pveam update had issues: {pveam_result.error_type.value} - {pveam_result.error_message}")
    
    # Cleanup other templates
    logger.info("Cleaning up other template archives...")
    preserve_patterns = " ".join([f"! -name '{p}'" for p in cfg.template_config.preserve])
    cleanup_old_cmd = (
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
        f"! -name '{final_template_name}' {preserve_patterns} -delete 2>&1 || true"
    )
    ssh_exec(proxmox_host, cleanup_old_cmd, check=False, cfg=cfg)
    
    # Destroy container
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    logger.info(f"Ubuntu template '{template_name}' created successfully")
    return True
