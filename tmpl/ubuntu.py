"""
Ubuntu template type - creates a base Ubuntu template
"""

import logging
import subprocess
import time
from datetime import datetime

# Import helper functions from libs
from libs import common, template
from libs.config import ContainerResources, LabConfig, TemplateConfig
from cli import PCT, Vzdump, Apt, SystemCtl, CommandWrapper, FileOps, User, Dpkg, Sed
from ct.apt_cache import run_apt_command as run_apt_with_lock

# Default timeout for long-running apt/dpkg commands (seconds)
APT_LONG_TIMEOUT = 600

# Get logger for this module
logger = logging.getLogger(__name__)


def _wait_for_archive_file(proxmox_host, container_id, template_dir, cfg, max_wait=120):
    """Wait for archive file to be created and stable (not growing)."""
    wait_count = 0
    last_size = 0
    stable_count = 0
    backup_file = None

    while wait_count < max_wait:
        time.sleep(2)
        wait_count += 2
        # Find the archive file
        find_archive_cmd = Vzdump.find_archive_cmd(template_dir, container_id)
        backup_file = ssh_exec(
            proxmox_host, find_archive_cmd, capture_output=True, cfg=cfg
        )
        if not backup_file:
            continue

        backup_file = backup_file.strip()
        # Check file size
        size_cmd = Vzdump.get_archive_size_cmd(backup_file)
        size_check = ssh_exec(proxmox_host, size_cmd, capture_output=True, cfg=cfg)
        if not size_check:
            continue

        current_size = Vzdump.parse_archive_size(size_check)
        if not current_size or current_size <= 0:
            continue

        if current_size == last_size:
            stable_count += 1
            if stable_count >= 3:  # File size stable for 6 seconds
                break
        else:
            stable_count = 0
            last_size = current_size

    return backup_file


ssh_exec = common.ssh_exec
pct_exec = common.pct_exec
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
setup_ssh_key = common.setup_ssh_key
get_base_template = template.get_base_template


def _run_pct_command(  # pylint: disable=too-many-arguments
    proxmox_host,
    container_id,
    command,
    cfg,
    description,
    *,
    timeout=APT_LONG_TIMEOUT,
    warn_only=False,
):
    """Execute pct command with consistent logging and parsing."""
    output = pct_exec(
        proxmox_host,
        container_id,
        command,
        capture_output=True,
        timeout=timeout,
        cfg=cfg,
    )
    result = CommandWrapper.parse_result(output)
    if result.has_error:
        log_fn = logger.warning if warn_only else logger.error
        log_fn(
            "%s failed: %s - %s",
            description,
            result.error_type.value,
            result.error_message,
        )
        if output:
            log_fn(output.splitlines()[-1])
        return False, output
    return True, output


def _ensure_user_exists(proxmox_host, container_id, cfg, username, sudo_group):
    """Create template user if it does not already exist."""
    exists_cmd = User.check_exists_cmd(username)
    try:
        pct_exec(
            proxmox_host,
            container_id,
            exists_cmd,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        return True
    except subprocess.CalledProcessError:
        create_cmd = User.add_cmd(username, shell="/bin/bash", groups=[sudo_group])
        succeeded, _ = _run_pct_command(
            proxmox_host,
            container_id,
            create_cmd,
            cfg,
            "Create template user",
        )
        return succeeded


def _configure_sudoers(proxmox_host, container_id, cfg, username):
    sudoers_path = f"/etc/sudoers.d/{username}"
    sudoers_content = f"{username} ALL=(ALL) NOPASSWD: ALL\n"
    write_cmd = FileOps.write_cmd(sudoers_path, sudoers_content)
    succeeded, _ = _run_pct_command(
        proxmox_host,
        container_id,
        write_cmd,
        cfg,
        "Write sudoers entry",
    )
    if not succeeded:
        return False
    chmod_cmd = FileOps.chmod_cmd(sudoers_path, "440")
    succeeded, _ = _run_pct_command(
        proxmox_host,
        container_id,
        chmod_cmd,
        cfg,
        "Secure sudoers entry",
    )
    return succeeded


def _prepare_ssh_directories(proxmox_host, container_id, cfg, username):
    commands = [
        (
            FileOps.mkdir_cmd(f"/home/{username}/.ssh"),
            f"Create {username} .ssh directory",
        ),
        (FileOps.mkdir_cmd("/root/.ssh"), "Create root .ssh directory"),
        (
            FileOps.chmod_cmd(f"/home/{username}/.ssh", "700"),
            f"Secure {username} .ssh directory",
        ),
        (
            FileOps.chown_cmd(f"/home/{username}/.ssh", username, username),
            f"Set ownership for {username} .ssh directory",
        ),
    ]
    for cmd, description in commands:
        succeeded, _ = _run_pct_command(
            proxmox_host,
            container_id,
            cmd,
            cfg,
            description,
        )
        if not succeeded:
            return False
    return True


def create_template(template_cfg: TemplateConfig, cfg: LabConfig):  # pylint: disable=too-many-locals,too-many-return-statements,too-many-branches,too-many-statements
    """Create Ubuntu template - method for type 'ubuntu'"""
    proxmox_host = cfg.proxmox_host
    container_id = template_cfg.id
    ip_address = template_cfg.ip_address
    hostname = template_cfg.hostname
    gateway = cfg.gateway
    # Get apt-cache IP from containers (may not exist yet during template creation)
    apt_cache_containers = [c for c in cfg.containers if c.type == "apt-cache"]
    apt_cache_ip = apt_cache_containers[0].ip_address if apt_cache_containers else None
    template_name = template_cfg.name

    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)

    # Get container resources and settings
    resources = template_cfg.resources
    if not resources:
        # Default fallback
        resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)
    storage = cfg.proxmox_storage
    bridge = cfg.proxmox_bridge
    template_dir = cfg.proxmox_template_dir
    base_template = get_base_template(proxmox_host, cfg)
    if not base_template:
        logger.error("Failed to get base template")
        return False

    # Create container
    logger.info("Creating container %s...", container_id)
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
        arch="amd64",
    )
    create_output = ssh_exec(proxmox_host, create_cmd, capture_output=True, cfg=cfg)
    create_result = CommandWrapper.parse_result(create_output)
    if not create_result:
        logger.error(
            "Failed to create container: %s - %s",
            create_result.error_type.value,
            create_result.error_message,
        )
        return False

    # Configure features
    logger.info("Configuring container features...")
    features_cmd = PCT.set_features_cmd(
        container_id, nesting=True, keyctl=True, fuse=True
    )
    features_output = ssh_exec(proxmox_host, features_cmd, capture_output=True, cfg=cfg)
    features_result = CommandWrapper.parse_result(features_output)
    if features_result.has_error:
        logger.warning(
            "Failed to set container features: %s - %s",
            features_result.error_type.value,
            features_result.error_message,
        )

    # Start container
    logger.info("Starting container...")
    start_cmd = PCT.start_cmd(container_id)
    start_output = ssh_exec(proxmox_host, start_cmd, capture_output=True, cfg=cfg)
    start_result = CommandWrapper.parse_result(start_output)
    if start_result.has_error:
        logger.error(
            "Failed to start container: %s - %s",
            start_result.error_type.value,
            start_result.error_message,
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Verify container is actually running
    time.sleep(cfg.waits.container_startup)
    status_cmd = PCT.status_cmd(container_id)
    status_output = ssh_exec(
        proxmox_host, status_cmd, capture_output=True, timeout=10, cfg=cfg
    )
    if not PCT.parse_status_output(status_output, container_id):
        logger.error(
            "Container %s is not running after start. Status: %s",
            container_id,
            status_output,
        )
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
        proxy_content = (
            f'Acquire::http::Proxy "http://{apt_cache_ip}:{apt_cache_port}";\n'
        )
        proxy_cmd = FileOps.write_cmd("/etc/apt/apt.conf.d/01proxy", proxy_content)
        _run_pct_command(
            proxmox_host,
            container_id,
            proxy_cmd,
            cfg,
            "Configure apt cache proxy",
            warn_only=True,
        )

    # Disable AppArmor parser to avoid maintainer script failures inside LXC
    logger.info("Disabling AppArmor parser to avoid reload errors...")
    divert_cmd = Dpkg.divert_cmd("/usr/sbin/apparmor_parser")
    _run_pct_command(
        proxmox_host,
        container_id,
        divert_cmd,
        cfg,
        "Divert AppArmor parser",
        warn_only=True,
        timeout=15,
    )
    stub_content = (
        "#!/bin/sh\n"
        'if [ "$1" = "--version" ] || [ "$1" = "-V" ]; then\n'
        '  exec /usr/sbin/apparmor_parser.distrib "$@"\n'
        "fi\n"
        "exit 0\n"
    )
    stub_write_cmd = FileOps.write_cmd("/usr/sbin/apparmor_parser", stub_content)
    _run_pct_command(
        proxmox_host,
        container_id,
        stub_write_cmd,
        cfg,
        "Write AppArmor parser stub",
        warn_only=True,
        timeout=15,
    )
    _run_pct_command(
        proxmox_host,
        container_id,
        FileOps.chmod_cmd("/usr/sbin/apparmor_parser", "+x"),
        cfg,
        "Make AppArmor parser stub executable",
        warn_only=True,
        timeout=15,
    )

    # Fix apt sources
    logger.info("Fixing apt sources...")
    sed_cmds = [
        Sed.replace_cmd("/etc/apt/sources.list", "oracular", "plucky"),
        Sed.replace_cmd(
            "/etc/apt/sources.list",
            "old-releases.ubuntu.com",
            "archive.ubuntu.com",
            delimiter="|",
        ),
    ]
    for idx, sed_cmd in enumerate(sed_cmds, start=1):
        _run_pct_command(
            proxmox_host,
            container_id,
            sed_cmd,
            cfg,
            f"Fix apt sources step {idx}",
            warn_only=True,
        )

    logger.info("Running apt update...")
    update_output = run_apt_with_lock(
        proxmox_host, container_id, Apt.update_cmd(quiet=True), cfg
    )
    if update_output is None:
        logger.error("apt update failed for template container")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Setup user and SSH
    default_user = cfg.users.default_user
    sudo_group = cfg.users.sudo_group
    logger.info("Setting up user and SSH access...")
    if not _ensure_user_exists(
        proxmox_host, container_id, cfg, default_user, sudo_group
    ):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    if not _configure_sudoers(proxmox_host, container_id, cfg, default_user):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    if not _prepare_ssh_directories(proxmox_host, container_id, cfg, default_user):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    logger.info("Installing openssh-server...")
    ssh_install_output = run_apt_with_lock(
        proxmox_host,
        container_id,
        Apt.install_cmd(["openssh-server"]),
        cfg,
    )
    if ssh_install_output is None:
        logger.error("Failed to install openssh-server in template container")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    for action, systemctl_cmd in [
        ("Enable ssh service", SystemCtl.enable_cmd("ssh")),
        ("Start ssh service", SystemCtl.start_cmd("ssh")),
    ]:
        succeeded, _ = _run_pct_command(
            proxmox_host,
            container_id,
            systemctl_cmd,
            cfg,
            action,
        )
        if not succeeded:
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False

    if not setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
        logger.error("Failed to setup SSH key")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Upgrade distribution
    logger.info("Upgrading distribution to latest (25.04)...")
    upgrade_output = run_apt_with_lock(
        proxmox_host,
        container_id,
        Apt.upgrade_cmd(dist_upgrade=True),
        cfg,
    )
    if upgrade_output is None:
        logger.error("Distribution upgrade failed")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Install base tools
    logger.info("Installing minimal base tools...")
    tools_output = run_apt_with_lock(
        proxmox_host,
        container_id,
        Apt.install_cmd(["ca-certificates", "curl"]),
        cfg,
    )
    if tools_output is None:
        logger.error("Failed to install base tools")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Cleanup for template
    logger.info("Cleanup for template...")
    cleanup_commands = [
        (
            "Remove apt proxy configuration",
            FileOps.remove_cmd("/etc/apt/apt.conf.d/01proxy"),
        ),
        (
            "Remove SSH host keys",
            FileOps.remove_cmd("/etc/ssh/ssh_host_*", allow_glob=True),
        ),
        ("Truncate machine-id", FileOps.truncate_cmd("/etc/machine-id")),
        ("Remove DBus machine-id", FileOps.remove_cmd("/var/lib/dbus/machine-id")),
        (
            "Recreate DBus machine-id symlink",
            FileOps.symlink_cmd("/etc/machine-id", "/var/lib/dbus/machine-id"),
        ),
        (
            "Remove apt lists",
            FileOps.remove_cmd("/var/lib/apt/lists/*", recursive=True, allow_glob=True),
        ),
        ("Remove log files", FileOps.find_delete_cmd("/var/log", "*.log")),
        ("Remove compressed logs", FileOps.find_delete_cmd("/var/log", "*.gz")),
        (
            "Clear root history",
            FileOps.truncate_cmd("/root/.bash_history", suppress_errors=True),
        ),
        (
            f"Clear {cfg.users.default_user} history",
            FileOps.truncate_cmd(
                f"/home/{cfg.users.default_user}/.bash_history", suppress_errors=True
            ),
        ),
    ]
    for desc, cmd in cleanup_commands:
        _run_pct_command(
            proxmox_host,
            container_id,
            cmd,
            cfg,
            desc,
            warn_only=True,
        )
    _run_pct_command(
        proxmox_host,
        container_id,
        Apt.clean_cmd(),
        cfg,
        "Clean apt cache",
        warn_only=True,
    )

    # Stop container
    logger.info("Stopping container...")
    stop_cmd = PCT.stop_cmd(container_id)
    stop_output = ssh_exec(proxmox_host, stop_cmd, capture_output=True, cfg=cfg)
    stop_result = CommandWrapper.parse_result(stop_output)
    if stop_result.has_error and "already stopped" not in (stop_result.output or ""):
        logger.warning(
            "Stop container had issues: %s - %s",
            stop_result.error_type.value,
            stop_result.error_message,
        )
        # Try to force stop
        force_stop_cmd = PCT.stop_cmd(container_id, force=True)
        ssh_exec(proxmox_host, force_stop_cmd, cfg=cfg)
    time.sleep(2)

    # Create template - must complete successfully
    template_dir = cfg.proxmox_template_dir
    logger.info("Creating template archive...")
    vzdump_cmd = Vzdump.create_template_cmd(
        container_id, template_dir, compress="zstd", mode="stop"
    )
    vzdump_output = ssh_exec(proxmox_host, vzdump_cmd, capture_output=True, cfg=cfg)

    # Check if vzdump succeeded
    if not vzdump_output:
        logger.error("vzdump produced no output")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    vzdump_result = CommandWrapper.parse_result(vzdump_output)
    if vzdump_result.has_error:
        logger.error(
            "vzdump failed: %s - %s",
            vzdump_result.error_type.value,
            vzdump_result.error_message,
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Check for success indicators
    vzdump_upper = vzdump_output.upper()
    if "FINISHED" not in vzdump_upper and "archive" not in vzdump_output.lower():
        logger.warning("vzdump output doesn't show clear success, but no errors found")

    # Wait for archive file to be created and stable (not growing)
    logger.info("Waiting for template archive to be ready...")
    backup_file = _wait_for_archive_file(
        proxmox_host, container_id, template_dir, cfg, max_wait=120
    )

    if not backup_file:
        logger.error("Template archive file not found after vzdump")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Verify archive is not empty and has reasonable size (> 10MB)
    size_cmd = Vzdump.get_archive_size_cmd(backup_file)
    size_check = ssh_exec(proxmox_host, size_cmd, capture_output=True, cfg=cfg)
    if not size_check:
        logger.error("Failed to get archive file size")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    file_size = Vzdump.parse_archive_size(size_check)
    if not file_size or file_size < 10485760:  # Less than 10MB is suspicious
        logger.error(
            "Template archive is too small (%s bytes if found), likely corrupted",
            file_size,
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    logger.info("Template archive size: %.2f MB", file_size / 1048576)

    # Rename template
    template_pattern = cfg.template_config.patterns["ubuntu"]
    final_template_name = template_pattern.replace(
        "{date}", datetime.now().strftime("%Y%m%d")
    )
    rename_cmd = f"mv '{backup_file}' {template_dir}/{final_template_name} 2>&1"
    try:
        rename_output = ssh_exec(proxmox_host, rename_cmd, capture_output=True, cfg=cfg)
    except subprocess.CalledProcessError as exc:
        logger.error("Failed to rename template archive: %s", exc)
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    _ = rename_output  # keep for completeness
    ls_cmd = f"ls -lh {template_dir}/{final_template_name} 2>&1"
    try:
        ssh_exec(proxmox_host, ls_cmd, capture_output=True, cfg=cfg)
    except subprocess.CalledProcessError as exc:
        logger.warning("Listing new template file failed: %s", exc)

    # Update template list
    pveam_cmd = "pveam update 2>&1"
    pveam_output = ssh_exec(proxmox_host, pveam_cmd, capture_output=True, cfg=cfg)
    pveam_result = CommandWrapper.parse_result(pveam_output)
    if pveam_result.has_error:
        logger.warning(
            "pveam update had issues: %s - %s",
            pveam_result.error_type.value,
            pveam_result.error_message,
        )

    # Cleanup other templates
    logger.info("Cleaning up other template archives...")
    preserve_patterns = " ".join(
        [f"! -name '{p}'" for p in cfg.template_config.preserve]
    )
    cleanup_old_cmd = (
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
        f"! -name '{final_template_name}' {preserve_patterns} -delete 2>&1"
    )
    ssh_exec(proxmox_host, cleanup_old_cmd, cfg=cfg)

    # Destroy container
    destroy_container(proxmox_host, container_id, cfg=cfg)

    logger.info("Ubuntu template '%s' created successfully", template_name)
    return True
