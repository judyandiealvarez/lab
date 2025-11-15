"""
Ubuntu+Docker template type - creates an Ubuntu template with Docker pre-installed
"""
import sys
import time
import logging
from datetime import datetime

# Import helper functions from libs
from libs import common, template
from libs.config import LabConfig, TemplateConfig
from cli import PCT, Vzdump, Apt, SystemCtl, Docker, CommandWrapper

# Get logger for this module
logger = logging.getLogger(__name__)

ssh_exec = common.ssh_exec
pct_exec = common.pct_exec
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
setup_ssh_key = common.setup_ssh_key
get_base_template = template.get_base_template


def create_template(template_cfg: TemplateConfig, cfg: LabConfig):
    """Create Docker template - method for type 'ubuntu+docker'"""
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
        resources = ContainerResources(memory=4096, swap=4096, cores=8, rootfs_size=40)
    storage = cfg.proxmox_storage
    bridge = cfg.proxmox_bridge
    template_dir = cfg.proxmox_template_dir
    base_template = get_base_template(proxmox_host, cfg)
    if not base_template:
        logger.error("Failed to get base template")
        return False
    default_user = cfg.users.default_user
    sudo_group = cfg.users.sudo_group
    apt_cache_port = cfg.apt_cache_port
    
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
    
    # Setup user
    logger.info("Creating user and configuring sudo...")
    user_cmd = (
        f"useradd -m -s /bin/bash -G {sudo_group} {default_user} 2>/dev/null || echo User exists; "
        f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' | tee /etc/sudoers.d/{default_user}; "
        f"chmod 440 /etc/sudoers.d/{default_user}; "
        f"mkdir -p /home/{default_user}/.ssh; chown -R {default_user}:{default_user} /home/{default_user}; chmod 700 /home/{default_user}/.ssh 2>&1"
    )
    user_output = pct_exec(proxmox_host, container_id, user_cmd, check=False, capture_output=True, cfg=cfg)
    user_result = CommandWrapper.parse_result(user_output)
    if user_result.has_error and "User exists" not in (user_result.output or ""):
        logger.error(f"Failed to setup user: {user_result.error_type.value} - {user_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    if not setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
        logger.error("Failed to setup SSH key")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Fix apt sources
    logger.info("Fixing apt sources...")
    apt_sources_cmd = (
        "sed -i 's/oracular/plucky/g' /etc/apt/sources.list 2>/dev/null || true; "
        "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true; "
        "if ! grep -q '^deb.*plucky.*main' /etc/apt/sources.list; then "
        "echo 'deb http://archive.ubuntu.com/ubuntu plucky main universe multiverse' > /etc/apt/sources.list; "
        "echo 'deb http://archive.ubuntu.com/ubuntu plucky-updates main universe multiverse' >> /etc/apt/sources.list; "
        "echo 'deb http://archive.ubuntu.com/ubuntu plucky-security main universe multiverse' >> /etc/apt/sources.list; "
        "fi 2>&1"
    )
    apt_sources_output = pct_exec(proxmox_host, container_id, apt_sources_cmd, check=False, capture_output=True, cfg=cfg)
    apt_sources_result = CommandWrapper.parse_result(apt_sources_output)
    if apt_sources_result.has_error:
        logger.warning(f"Apt sources fix had issues: {apt_sources_result.error_type.value} - {apt_sources_result.error_message}")
    
    # Update packages - remove proxy first to avoid connection issues
    logger.info("Updating package lists...")
    pct_exec(proxmox_host, container_id, "rm -f /etc/apt/apt.conf.d/01proxy 2>&1", check=False, cfg=cfg)
    
    # Try update without proxy first
    update_cmd = Apt.update_cmd(quiet=False)
    update_output = pct_exec(proxmox_host, container_id, update_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    update_result = CommandWrapper.parse_result(update_output)
    
    # If update fails and we have apt-cache, try with proxy
    if apt_cache_ip and update_result.has_error:
        logger.info("Update failed, trying with apt-cache proxy...")
        proxy_cmd = (
            f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true; "
            f"{update_cmd}"
        )
        proxy_output = pct_exec(proxmox_host, container_id, proxy_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
        proxy_result = CommandWrapper.parse_result(proxy_output)
        if proxy_result.has_error:
            logger.error(f"Failed to update packages even with proxy: {proxy_result.error_type.value} - {proxy_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    elif update_result.has_error:
        logger.error(f"Failed to update packages: {update_result.error_type.value} - {update_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Install prerequisites - try without proxy first
    logger.info("Installing prerequisites...")
    install_cmd = Apt.install_cmd(["curl", "apt-transport-https", "ca-certificates", "software-properties-common", "gnupg", "lsb-release"])
    install_output = pct_exec(proxmox_host, container_id, install_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    install_result = CommandWrapper.parse_result(install_output)
    
    # If install fails, remove proxy and try again
    if install_result.has_error:
        logger.info("Install failed, removing proxy and retrying...")
        retry_cmd = (
            "rm -f /etc/apt/apt.conf.d/01proxy; "
            f"{Apt.update_cmd(quiet=True)} && "
            f"{install_cmd}"
        )
        retry_output = pct_exec(proxmox_host, container_id, retry_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
        retry_result = CommandWrapper.parse_result(retry_output)
        if retry_result.has_error:
            logger.error(f"Failed to install prerequisites: {retry_result.error_type.value} - {retry_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    # Upgrade
    logger.info("Upgrading to latest Ubuntu distribution (25.04)...")
    upgrade_cmd = Apt.upgrade_cmd(dist_upgrade=True)
    upgrade_output = pct_exec(proxmox_host, container_id, upgrade_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
    upgrade_result = CommandWrapper.parse_result(upgrade_output)
    if upgrade_result.has_error:
        logger.error(f"Failed to upgrade distribution: {upgrade_result.error_type.value} - {upgrade_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Install Docker - remove proxy first to avoid connection issues
    logger.info("Installing Docker...")
    pct_exec(proxmox_host, container_id, "rm -f /etc/apt/apt.conf.d/01proxy 2>&1", check=False, cfg=cfg)
    
    docker_install_script = (
        "rm -f /etc/apt/apt.conf.d/01proxy; "
        "DEBIAN_FRONTEND=noninteractive apt update -qq 2>&1 && "
        "if command -v curl >/dev/null 2>&1; then "
        "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh 2>&1 && sh /tmp/get-docker.sh 2>&1 || "
        "  (echo 'get.docker.com failed, trying docker.io...' && DEBIAN_FRONTEND=noninteractive apt install -y docker.io containerd.io 2>&1); "
        "else "
        "  echo 'curl not available, installing docker.io...'; "
        "  DEBIAN_FRONTEND=noninteractive apt install -y docker.io containerd.io 2>&1; "
        "fi"
    )
    docker_output = pct_exec(proxmox_host, container_id, docker_install_script, check=False, capture_output=True, timeout=300, cfg=cfg)
    docker_result = CommandWrapper.parse_result(docker_output)
    
    # Verify Docker installation
    logger.info("Verifying Docker install...")
    docker_check_cmd = Docker.is_installed_check_cmd("docker")
    docker_check_output = pct_exec(proxmox_host, container_id, docker_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    
    if not Docker.parse_is_installed(docker_check_output):
        logger.info("Docker not found, installing docker.io directly...")
        docker_io_cmd = (
            "rm -f /etc/apt/apt.conf.d/01proxy; "
            f"{Apt.update_cmd(quiet=True)} && "
            f"{Apt.install_cmd(['docker.io', 'containerd.io'])}"
        )
        docker_io_output = pct_exec(proxmox_host, container_id, docker_io_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
        docker_io_result = CommandWrapper.parse_result(docker_io_output)
        if docker_io_result.has_error:
            logger.error(f"Failed to install docker.io: {docker_io_result.error_type.value} - {docker_io_result.error_message}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
        # Verify again
        docker_check_output = pct_exec(proxmox_host, container_id, docker_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if not Docker.parse_is_installed(docker_check_output):
            logger.error("Docker installation failed - docker command not found")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    # Configure Docker user group
    logger.info("Configuring Docker user group...")
    usermod_cmd = f"usermod -aG docker {default_user} 2>&1"
    usermod_output = pct_exec(proxmox_host, container_id, usermod_cmd, check=False, capture_output=True, cfg=cfg)
    usermod_result = CommandWrapper.parse_result(usermod_output)
    if usermod_result.has_error:
        logger.warning(f"Failed to add user to docker group: {usermod_result.error_type.value} - {usermod_result.error_message}")
    
    # Start Docker
    logger.info("Starting Docker service...")
    docker_start_cmd = SystemCtl.enable_and_start_cmd("docker")
    docker_start_output = pct_exec(proxmox_host, container_id, docker_start_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
    docker_start_result = CommandWrapper.parse_result(docker_start_output)
    if docker_start_result.has_error:
        logger.error(f"Failed to start Docker service: {docker_start_result.error_type.value} - {docker_start_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Disable AppArmor
    logger.info("Disabling AppArmor for Docker...")
    apparmor_cmd = "systemctl stop apparmor && systemctl disable apparmor 2>/dev/null || true 2>&1"
    pct_exec(proxmox_host, container_id, apparmor_cmd, check=False, cfg=cfg)
    
    # Verify Docker
    logger.info("Verifying Docker installation...")
    docker_verify_cmd = "docker --version && docker ps 2>&1"
    docker_verify_output = pct_exec(proxmox_host, container_id, docker_verify_cmd, check=False, capture_output=True, cfg=cfg)
    docker_verify_result = CommandWrapper.parse_result(docker_verify_output)
    if docker_verify_result.has_error or "Cannot connect" in (docker_verify_output or ""):
        logger.error(f"Docker verification failed: {docker_verify_result.error_type.value if docker_verify_result.has_error else 'Cannot connect'}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Cleanup for template
    logger.info("Cleaning up container-specific data for template...")
    cleanup_cmd = (
        "bash -c '"
        "rm -f /etc/apt/apt.conf.d/01proxy || true; "
        "echo \"localhost\" > /etc/hostname; "
        "hostnamectl set-hostname localhost 2>/dev/null || true; "
        f"rm -f /root/.ssh/authorized_keys 2>/dev/null || true; "
        f"rm -f /home/{cfg.users.default_user}/.ssh/authorized_keys 2>/dev/null || true; "
        "rm -f /etc/machine-id; touch /etc/machine-id; chmod 444 /etc/machine-id; "
        "journalctl --vacuum-time=1s 2>/dev/null || true; "
        "rm -rf /var/log/*.log 2>/dev/null || true; "
        "rm -rf /var/log/journal/* 2>/dev/null || true; "
        f"rm -f /root/.bash_history 2>/dev/null || true; "
        f"rm -f /home/{cfg.users.default_user}/.bash_history 2>/dev/null || true; "
        "apt clean 2>/dev/null || true; "
        "rm -rf /var/lib/apt/lists/* 2>/dev/null || true; "
        "systemctl stop docker 2>/dev/null || true; "
        "docker system prune -af 2>/dev/null || true' 2>&1"
    )
    cleanup_output = pct_exec(proxmox_host, container_id, cleanup_cmd, check=False, capture_output=True, cfg=cfg)
    cleanup_result = CommandWrapper.parse_result(cleanup_output)
    if cleanup_result.has_error:
        logger.warning(f"Cleanup had some errors: {cleanup_result.error_type.value} - {cleanup_result.error_message}")
    
    # Stop container
    template_dir = cfg.proxmox_template_dir
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
    logger.info("Creating template from container...")
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
    template_pattern = cfg.template_config.patterns['ubuntu+docker']
    final_template_name = template_pattern.replace('{date}', datetime.now().strftime('%Y%m%d'))
    rename_cmd = f"mv '{backup_file}' {template_dir}/{final_template_name} && echo 'Template created: {final_template_name}' 2>&1"
    rename_output = ssh_exec(proxmox_host, rename_cmd, check=False, capture_output=True, cfg=cfg)
    rename_result = CommandWrapper.parse_result(rename_output)
    
    if not rename_result or "Template created" not in (rename_output or ""):
        logger.error(f"Failed to rename template archive: {rename_result.error_type.value if rename_result.has_error else 'No output'}")
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
    
    # Cleanup
    logger.info("Cleaning up other template archives...")
    preserve_patterns = " ".join([f"! -name '{p}'" for p in cfg.template_config.preserve])
    cleanup_old_cmd = (
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
        f"! -name '{final_template_name}' {preserve_patterns} -delete 2>&1 || true"
    )
    ssh_exec(proxmox_host, cleanup_old_cmd, check=False, cfg=cfg)
    
    # Destroy container
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    logger.info(f"Docker template '{template_name}' created successfully")
    return True
