"""
Container-specific functions - only used by container modules
"""

import logging
import time
from typing import Optional

from cli import CommandWrapper, PCT

from .common import (
    container_exists,
    destroy_container,
    pct_exec,
    setup_ssh_key,
    ssh_exec,
    wait_for_container,
)
from .config import ContainerConfig, ContainerResources, LabConfig
from .template import get_base_template

# Get logger for this module
logger = logging.getLogger(__name__)


def _check_container_exists(
    proxmox_host, _container_id, list_check_cmd, exists_check_cmd, cfg
):
    """Check if container exists by querying list and config."""
    list_check = ssh_exec(
        proxmox_host,
        list_check_cmd,
        check=False,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    config_check = ssh_exec(
        proxmox_host,
        exists_check_cmd,
        check=False,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    return (list_check and "exists" in list_check) or (
        config_check and "exists" in config_check
    )


def _try_base_template_fallback(  # pylint: disable=too-many-arguments,too-many-locals
    proxmox_host,
    container_id,
    template_name,
    template_path,
    hostname,
    resources,
    ip_address,
    gateway,
    bridge,
    storage,
    unprivileged,
    list_check_cmd,
    cfg,
):
    """Try creating container with base template as fallback."""
    using_custom_template = (
        template_name
        and template_name != "ubuntu-tmpl"
        and "ubuntu-25.04-template" not in template_path
        and "ubuntu-24.10-standard" not in template_path
    )
    if not using_custom_template:
        return False, None

    logger.warning(
        "Template %s failed, trying with base template...",
        template_name,
    )
    base_template = get_base_template(proxmox_host, cfg)
    base_template_path = f"{cfg.proxmox_template_dir}/{base_template}"
    time.sleep(2)
    ssh_exec(
        proxmox_host,
        f"pct destroy {container_id} 2>/dev/null || true",
        check=False,
        cfg=cfg,
    )
    time.sleep(1)
    fallback_create_cmd = PCT.create_cmd(
        container_id=container_id,
        template_path=base_template_path,
        hostname=hostname,
        memory=resources.memory,
        swap=resources.swap,
        cores=resources.cores,
        ip_address=ip_address,
        gateway=gateway,
        bridge=bridge,
        storage=storage,
        rootfs_size=resources.rootfs_size,
        unprivileged=bool(unprivileged),
        ostype="ubuntu",
        arch="amd64",
    )
    fallback_result = ssh_exec(
        proxmox_host,
        fallback_create_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    time.sleep(3)
    list_check = ssh_exec(
        proxmox_host,
        list_check_cmd,
        check=False,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    if list_check and "exists" in list_check:
        logger.info(
            "Container %s created successfully with base template",
            container_id,
        )
        return True, None
    logger.error(
        "Container %s creation failed even with base template",
        container_id,
    )
    error_msg = (
        fallback_result[-500:] if fallback_result else "Unknown error"
    )
    return False, error_msg


def get_template_path(template_name: Optional[str], cfg: LabConfig) -> str:
    """Get path to template file by template name"""
    proxmox_host = cfg.proxmox_host
    template_dir = cfg.proxmox_template_dir

    # If template_name is None, use base template directly
    if template_name is None:
        base_template = get_base_template(proxmox_host, cfg)
        return f"{template_dir}/{base_template}"

    # Find template config
    template_cfg = None
    for tmpl in cfg.templates:
        if tmpl.name == template_name:
            template_cfg = tmpl
            break

    if not template_cfg:
        # Fallback to base template
        base_template = get_base_template(proxmox_host, cfg)
        return f"{template_dir}/{base_template}"

    # Find template file by pattern
    template_type = template_cfg.type
    pattern = cfg.template_config.patterns.get(template_type, "").replace("{date}", "*")
    template_file = ssh_exec(
        proxmox_host,
        f"ls -t {template_dir}/{pattern} 2>/dev/null | head -1 | xargs basename 2>/dev/null",
        check=False,
        capture_output=True,
        cfg=cfg,
    )

    if template_file:
        return f"{template_dir}/{template_file.strip()}"
    # Fallback to base template
    base_template = get_base_template(proxmox_host, cfg)
    return f"{template_dir}/{base_template}"


def setup_container_base(  # pylint: disable=too-many-locals,too-many-return-statements,too-many-branches,too-many-statements
    container_cfg: ContainerConfig,
    cfg: LabConfig,
    privileged=False,
    configure_proxy=True,
):
    """Common container setup: create, start, configure network, user, SSH, DNS, apt"""
    proxmox_host = cfg.proxmox_host
    container_id = container_cfg.id
    ip_address = container_cfg.ip_address
    hostname = container_cfg.hostname
    gateway = cfg.gateway
    template_name = container_cfg.template or "ubuntu-tmpl"

    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)

    # Get template path
    template_path = get_template_path(template_name, cfg)

    # Validate template file exists and is readable before attempting creation
    template_validate_cmd = f"test -f {template_path} && test -r {template_path} && echo 'valid' || echo 'invalid'"
    template_valid = ssh_exec(
        proxmox_host,
        template_validate_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if not template_valid or "invalid" in template_valid:
        logger.error("Template file %s is missing or not readable", template_path)
        # Try base template immediately
        base_template = get_base_template(proxmox_host, cfg)
        template_path = f"{cfg.proxmox_template_dir}/{base_template}"
        logger.warning("Falling back to base template: %s", template_path)

    # Get container resources
    resources = container_cfg.resources
    if not resources:
        # Default fallback
        resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)
    storage = cfg.proxmox_storage
    bridge = cfg.proxmox_bridge

    # Create container
    logger.info("Creating container %s from template...", container_id)
    unprivileged = 0 if privileged else 1

    # Try to create container - the tar errors for postfix dev files are non-fatal
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
        unprivileged=bool(unprivileged),
        ostype="ubuntu",
        arch="amd64",
    )
    create_result = ssh_exec(
        proxmox_host, create_cmd, check=False, capture_output=True, cfg=cfg
    )

    # Check if container was actually created despite tar warnings
    # Tar errors for postfix dev files are often non-fatal - check if container exists
    # First check via pct list (most reliable)
    time.sleep(1)
    list_check_cmd = (
        f"pct list 2>/dev/null | grep -E '^{container_id}\\s' && echo exists || echo missing"
    )
    list_check = ssh_exec(
        proxmox_host,
        list_check_cmd,
        check=False,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )

    # Also check config file
    exists_check_cmd = PCT.exists_check_cmd(container_id)

    container_exists_flag = _check_container_exists(
        proxmox_host, container_id, list_check_cmd, exists_check_cmd, cfg
    )

    if not container_exists_flag:
        # Container was not created - check if it's due to tar errors
        if create_result and "tar:" in create_result:
            # Try to create container again - tar errors are often non-fatal
            logger.warning("Container creation had tar errors, retrying...")
            # Wait a moment for cleanup
            time.sleep(3)
            # Destroy any partial container first
            ssh_exec(
                proxmox_host,
                f"pct destroy {container_id} 2>/dev/null || true",
                check=False,
                cfg=cfg,
            )
            time.sleep(2)
            # Try creating again - sometimes it works on retry
            retry_result = ssh_exec(
                proxmox_host, create_cmd, check=False, capture_output=True, cfg=cfg
            )
            # Check again - wait a bit for container to be registered
            time.sleep(5)
            container_exists_flag = _check_container_exists(
                proxmox_host, container_id, list_check_cmd, exists_check_cmd, cfg
            )

            if not container_exists_flag and retry_result and "tar:" in retry_result:
                # Second retry with longer wait and cleanup
                logger.warning("Container creation still failing, second retry...")
                time.sleep(5)
                ssh_exec(
                    proxmox_host,
                    f"pct destroy {container_id} --force 2>/dev/null || true",
                    check=False,
                    cfg=cfg,
                )
                time.sleep(3)
                # Verify template file exists and is readable
                template_check = ssh_exec(
                    proxmox_host,
                    f"test -f {template_path} && ls -lh {template_path} | head -1 || echo 'template missing'",
                    check=False,
                    capture_output=True,
                    cfg=cfg,
                )
                if template_check and "template missing" not in template_check:
                    retry2_result = ssh_exec(
                        proxmox_host, create_cmd, check=False, capture_output=True, cfg=cfg
                    )
                    time.sleep(5)
                    container_exists_flag = _check_container_exists(
                        proxmox_host, container_id, list_check_cmd, exists_check_cmd, cfg
                    )

            if not container_exists_flag:
                # Even after retry, check one more time with longer wait
                time.sleep(2)
                list_check = ssh_exec(
                    proxmox_host,
                    list_check_cmd,
                    check=False,
                    capture_output=True,
                    timeout=10,
                    cfg=cfg,
                )
                if list_check and "exists" in list_check:
                    logger.warning(
                        "Container %s exists despite tar errors, continuing...",
                        container_id,
                    )
                    container_exists_flag = True
                else:
                    # Try one more time with base template as fallback
                    success, error_msg = _try_base_template_fallback(
                        proxmox_host,
                        container_id,
                        template_name,
                        template_path,
                        hostname,
                        resources,
                        ip_address,
                        gateway,
                        bridge,
                        storage,
                        unprivileged,
                        list_check_cmd,
                        cfg,
                    )
                    if success:
                        container_exists_flag = True
                    else:
                        error_msg = (
                            error_msg
                            or retry_result[-500:]
                            if retry_result
                            else create_result[-500:]
                            if create_result
                            else "Unknown error"
                        )
                        logger.error("Error output: %s", error_msg)
                        return False
        else:
            # Already using base template or template path issue
            # Try with the original ubuntu-24.10-standard template as last resort
            if (
                "ubuntu-25.04-template" in template_path
                or "ubuntu-tmpl" in template_path
            ):
                logger.warning(
                    "Custom template failed, trying with original "
                    "ubuntu-24.10-standard template..."
                )
                original_template = "ubuntu-24.10-standard_24.10-1_amd64.tar.zst"
                original_template_path = (
                    f"{cfg.proxmox_template_dir}/{original_template}"
                )
                time.sleep(2)
                ssh_exec(
                    proxmox_host,
                    f"pct destroy {container_id} 2>/dev/null || true",
                    check=False,
                    cfg=cfg,
                )
                time.sleep(1)
                fallback_create_cmd = PCT.create_cmd(
                    container_id=container_id,
                    template_path=original_template_path,
                    hostname=hostname,
                    memory=resources.memory,
                    swap=resources.swap,
                    cores=resources.cores,
                    ip_address=ip_address,
                    gateway=gateway,
                    bridge=bridge,
                    storage=storage,
                    rootfs_size=resources.rootfs_size,
                    unprivileged=bool(unprivileged),
                    ostype="ubuntu",
                    arch="amd64",
                )
                fallback_result = ssh_exec(
                    proxmox_host,
                    fallback_create_cmd,
                    check=False,
                    capture_output=True,
                    cfg=cfg,
                )
                time.sleep(3)
                list_check = ssh_exec(
                    proxmox_host,
                    list_check_cmd,
                    check=False,
                    capture_output=True,
                    timeout=10,
                    cfg=cfg,
                )
                if list_check and "exists" in list_check:
                    logger.info(
                        "Container %s created successfully with original template",
                        container_id,
                    )
                    container_exists_flag = True
                else:
                    # Check one more time if container exists despite errors
                    time.sleep(2)
                    final_check = ssh_exec(
                        proxmox_host,
                        list_check_cmd,
                        check=False,
                        capture_output=True,
                        timeout=10,
                        cfg=cfg,
                    )
                    if final_check and "exists" in final_check:
                        logger.warning(
                            "Container %s exists despite tar errors, continuing...",
                            container_id,
                        )
                        container_exists_flag = True
                    else:
                        logger.error(
                            "Container %s creation failed even with original template",
                            container_id,
                        )
                        error_msg = (
                            fallback_result[-500:]
                            if fallback_result
                            else retry_result[-500:]
                            if retry_result
                            else create_result[-500:]
                        )
                        logger.error("Error output: %s", error_msg)
                        return False
            else:
                # Check if maybe container was created anyway
                logger.warning(
                    "Checking if container %s exists despite errors...",
                    container_id,
                )
                time.sleep(2)
                final_check = ssh_exec(
                    proxmox_host,
                    list_check_cmd,
                    check=False,
                    capture_output=True,
                    timeout=10,
                    cfg=cfg,
                )
                if final_check and "exists" in final_check:
                    logger.warning(
                        "Container %s exists despite tar errors, continuing...",
                        container_id,
                    )
                    container_exists_flag = True
                else:
                    logger.error(
                        "Container %s creation failed after retry", container_id
                    )
                    error_msg = (
                        retry_result[-500:]
                        if retry_result
                        else create_result[-500:]
                    )
                    logger.error("Error output: %s", error_msg)
                    return False
                # Other error - fail immediately
                logger.error("Container %s creation failed", container_id)
                create_result_parsed = CommandWrapper.parse_result(create_result)
                if create_result_parsed.has_error:
                    logger.error(
                        "Error: %s - %s",
                        create_result_parsed.error_type.value,
                        create_result_parsed.error_message,
                    )
                return False
    else:
        # Container exists - tar errors were non-fatal
        if create_result and "tar:" in create_result:
            logger.warning(
                "Non-fatal tar errors during container creation "
                "(container was created successfully)"
            )

    # Verify container exists
    if not container_exists(proxmox_host, container_id, cfg=cfg):
        logger.error("Container %s was not created", container_id)
        return False

    # Start container
    logger.info("Starting container...")
    start_cmd = PCT.start_cmd(container_id)
    start_output = ssh_exec(
        proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg
    )
    start_result = CommandWrapper.parse_result(start_output)
    if start_result.has_error:
        logger.error(
            "Failed to start container %s: %s - %s",
            container_id,
            start_result.error_type.value,
            start_result.error_message,
        )
        # Try to get more details about why it failed
        if privileged:
            logger.info("Checking container configuration for privileged container...")
            config_cmd = PCT.config_cmd(container_id)
            config_output = ssh_exec(
                proxmox_host, config_cmd, check=False, capture_output=True, cfg=cfg
            )
            if config_output:
                logger.info("Container config: %s", config_output[:500])
        return False
    time.sleep(cfg.waits.container_startup)

    # Verify container is actually running before trying to exec
    # Try multiple times as privileged containers may take longer
    max_start_attempts = 5
    for attempt in range(1, max_start_attempts + 1):
        status_cmd = PCT.status_cmd(container_id)
        status_output = ssh_exec(
            proxmox_host, status_cmd, check=False, capture_output=True, cfg=cfg
        )
        if PCT.parse_status_output(status_output, container_id):
            break
        if attempt < max_start_attempts:
            logger.info(
                "Container %s not running yet, waiting... (attempt %s/%s)",
                container_id,
                attempt,
                max_start_attempts,
            )
            time.sleep(5)
            # Try starting again if it stopped
            if status_output and "stopped" in status_output:
                logger.info(
                    "Container %s stopped, trying to start again...", container_id
                )
                ssh_exec(
                    proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg
                )
                time.sleep(5)
        else:
            # Last attempt failed - get more details
            logger.error(
                "Container %s is not running after %s attempts. Status: %s",
                container_id,
                max_start_attempts,
                status_output,
            )
            # Get container logs if available
            if privileged:
                logger.info("Checking for startup errors in privileged container...")
                # Try to get any error messages
                error_output = ssh_exec(
                    proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg
                )
                error_result = CommandWrapper.parse_result(error_output)
                if error_result.has_error:
                    logger.error(
                        "Start error: %s - %s",
                        error_result.error_type.value,
                        error_result.error_message,
                    )
            return False

    # Configure network
    logger.info("Configuring network...")
    network_cmd = (
        f"ip link set eth0 up && ip addr add {ip_address}/24 dev eth0 "
        f"2>/dev/null || true && ip route add default via {gateway} dev eth0 "
        f"2>/dev/null || true && sleep 2 2>&1"
    )
    network_result = pct_exec(
        proxmox_host,
        container_id,
        network_cmd,
        check=False,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    if network_result and "error" in network_result.lower():
        logger.warning("Network configuration had issues: %s", network_result[-200:])
    time.sleep(cfg.waits.network_config)

    # Wait for container
    if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
        logger.error("Container did not become ready")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Create user and configure sudo
    default_user = cfg.users.default_user
    sudo_group = cfg.users.sudo_group
    logger.info("Creating user and configuring sudo...")
    # Wait a bit more to ensure container is ready for exec
    time.sleep(2)
    user_cmd = (
        f"useradd -m -s /bin/bash -G {sudo_group} {default_user} "
        f"2>/dev/null || echo User exists; "
        f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' | "
        f"tee /etc/sudoers.d/{default_user}; "
        f"chmod 440 /etc/sudoers.d/{default_user}; "
        f"mkdir -p /home/{default_user}/.ssh; "
        f"chown -R {default_user}:{default_user} /home/{default_user}; "
        f"chmod 700 /home/{default_user}/.ssh 2>&1"
    )
    user_result = pct_exec(
        proxmox_host,
        container_id,
        user_cmd,
        check=False,
        capture_output=True,
        timeout=30,
        cfg=cfg,
    )
    if not user_result or (
        "error" in user_result.lower() and "User exists" not in user_result
    ):
        logger.error(
            "Failed to setup user: %s",
            user_result[-300:] if user_result else "No output",
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Setup SSH key
    logger.info("Setting up SSH key...")
    if not setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
        logger.error("Failed to setup SSH key")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    # Configure DNS
    logger.info("Configuring DNS...")
    dns_servers = cfg.dns.servers
    # Build command for remaining DNS servers (skip first one)
    remaining_dns_cmd = " && ".join(
        [
            f"echo 'nameserver {dns}' >> /etc/resolv.conf"
            for dns in dns_servers[1:]
        ]
    )
    dns_setup_cmd = (
        f"echo 'nameserver {dns_servers[0]}' > /etc/resolv.conf"
    )
    if remaining_dns_cmd:
        dns_setup_cmd = f"{dns_setup_cmd} && {remaining_dns_cmd}"
    dns_setup_cmd = f"{dns_setup_cmd} 2>&1"
    dns_result = pct_exec(
        proxmox_host,
        container_id,
        dns_setup_cmd,
        check=False,
        capture_output=True,
        timeout=30,
        cfg=cfg,
    )
    if dns_result and "error" in dns_result.lower():
        logger.warning("DNS configuration had issues: %s", dns_result[-200:])

    # Fix apt sources
    logger.info("Fixing apt sources...")
    fix_sources_cmd = (
        "if grep -q oracular /etc/apt/sources.list; then "
        "sed -i 's/oracular/plucky/g' /etc/apt/sources.list && "
        "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' "
        "/etc/apt/sources.list 2>/dev/null || true && "
        "sed -i 's/plucky main/plucky main universe multiverse/g' "
        "/etc/apt/sources.list && "
        "sed -i 's/plucky-updates main/plucky-updates main universe multiverse/g' "
        "/etc/apt/sources.list && "
        "sed -i 's/plucky-security main/plucky-security main universe multiverse/g' "
        "/etc/apt/sources.list; "
        "elif grep -q noble /etc/apt/sources.list; then "
        "sed -i 's/noble main/noble main universe multiverse/g' "
        "/etc/apt/sources.list && "
        "sed -i 's/noble-updates main/noble-updates main universe multiverse/g' "
        "/etc/apt/sources.list && "
        "sed -i 's/noble-security main/noble-security main universe multiverse/g' "
        "/etc/apt/sources.list; "
        "fi 2>&1"
    )
    sources_output = pct_exec(
        proxmox_host, container_id, fix_sources_cmd, capture_output=True, cfg=cfg
    )
    sources_result = CommandWrapper.parse_result(sources_output)
    if sources_result.has_error:
        logger.warning(
            "Apt sources fix had issues: %s - %s",
            sources_result.error_type.value,
            sources_result.error_message,
        )

    # Configure apt cache (if apt-cache container exists)
    if configure_proxy:
        apt_cache_containers = [c for c in cfg.containers if c.type == "apt-cache"]
        if apt_cache_containers:
            apt_cache_ip = apt_cache_containers[0].ip_address
            apt_cache_port = cfg.apt_cache_port
            logger.info("Configuring apt cache...")
            proxy_cmd = (
                f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' "
                f"> /etc/apt/apt.conf.d/01proxy || true 2>&1"
            )
            apt_cache_result = pct_exec(
                proxmox_host,
                container_id,
                proxy_cmd,
                check=False,
                capture_output=True,
                cfg=cfg,
            )
            if apt_cache_result and "error" in apt_cache_result.lower():
                logger.warning(
                    "Apt cache configuration had issues: %s", apt_cache_result[-200:]
                )

    return container_id
