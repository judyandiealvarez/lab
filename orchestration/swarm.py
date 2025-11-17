"""Docker Swarm orchestration utilities."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from cli import Apt, CommandWrapper, Docker, PCT, SystemCtl
from libs import common
from libs.config import LabConfig, ContainerResources
from libs.container import get_template_path
from libs.logger import get_logger

logger = get_logger(__name__)

pct_exec = common.pct_exec
ssh_exec = common.ssh_exec
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
setup_ssh_key = common.setup_ssh_key
container_exists = common.container_exists


@dataclass(frozen=True)
class SwarmContainerContext:
    """Holds immutable data required to deploy a swarm container."""

    cfg: LabConfig
    container_cfg: object
    template_path: str
    apt_cache: Tuple[Optional[str], Optional[int]]


@dataclass(frozen=True)
class SwarmDeployContext:
    """Shared data computed once for swarm deployment."""

    cfg: LabConfig
    managers: Sequence[object]
    workers: Sequence[object]
    template_path: str
    apt_cache: Tuple[Optional[str], Optional[int]]

    @property
    def proxmox_host(self):
        """Return cached proxmox host string."""
        return self.cfg.proxmox_host

    @property
    def all_nodes(self):
        """Return list combining managers and workers."""
        return list(self.managers) + list(self.workers)


def deploy_swarm(cfg: LabConfig):
    """Deploy Docker Swarm"""
    context = _build_swarm_context(cfg)
    if not context:
        return False

    if not _deploy_swarm_nodes(context):
        return False

    manager_config = context.managers[0]
    docker_cmd = _ensure_manager_docker(context, manager_config)
    if not docker_cmd:
        return False

    if not _initialize_swarm_manager(context, manager_config, docker_cmd):
        return False

    join_token = _fetch_worker_join_token(context, manager_config, docker_cmd)
    if not join_token:
        return False

    result = _join_workers_to_swarm(context, docker_cmd, join_token) and _install_portainer(
        context, docker_cmd
    )
    if result:
        logger.info("Docker Swarm deployed")
    return result


def _build_swarm_context(cfg: LabConfig) -> Optional[SwarmDeployContext]:
    """Collect and validate configuration needed for swarm deployment."""
    managers = [c for c in cfg.containers if c.type == "swarm-manager"]
    workers = [c for c in cfg.containers if c.type == "swarm-node"]
    if not managers or not workers:
        logger.error("Swarm manager or worker containers not found in configuration")
        return None

    template_path = get_template_path("docker-tmpl", cfg)
    logger.info("Using template: %s", template_path)
    apt_cache = _get_apt_cache_proxy(cfg)
    return SwarmDeployContext(cfg, managers, workers, template_path, apt_cache)


def _get_apt_cache_proxy(cfg: LabConfig):
    """Return apt-cache proxy settings if available."""
    apt_cache = next((c for c in cfg.containers if c.type == "apt-cache"), None)
    if not apt_cache:
        return None, None
    return apt_cache.ip_address, cfg.apt_cache_port


def _deploy_swarm_nodes(context: SwarmDeployContext) -> bool:
    """Create and configure all swarm containers."""
    for container_cfg in context.all_nodes:
        ctx = SwarmContainerContext(
            cfg=context.cfg,
            container_cfg=container_cfg,
            template_path=context.template_path,
            apt_cache=context.apt_cache,
        )
        if not _deploy_single_swarm_container(ctx):
            return False
    return True


def _deploy_single_swarm_container(ctx: SwarmContainerContext) -> bool:
    """Provision one swarm container from template through Docker readiness."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_cfg = ctx.container_cfg
    container_id = container_cfg.id
    hostname = container_cfg.hostname
    ip_address = container_cfg.ip_address

    logger.info("\nDeploying container %s (%s)...", container_id, hostname)
    _destroy_existing_container(proxmox_host, container_id, cfg)

    resources = _resolve_container_resources(container_cfg)
    if not _create_swarm_container(ctx, resources):
        return False

    _configure_container_features(ctx)
    if container_cfg.type == "swarm-manager":
        _configure_lxc_sysctl_access(proxmox_host, container_id, cfg)

    if not _start_and_verify_container(ctx, ip_address):
        return False

    if not _setup_container_ssh(ctx, ip_address):
        return False

    _configure_container_proxy(ctx)

    if not _ensure_container_docker(ctx):
        return False

    if container_cfg.type == "swarm-manager":
        _configure_manager_runtime(ctx)

    logger.info("Container %s (%s) deployed successfully", container_id, hostname)
    return True


def _destroy_existing_container(proxmox_host, container_id, cfg):
    """Remove existing container if present."""
    if container_exists(proxmox_host, container_id, cfg=cfg):
        logger.info("Destroying existing container %s...", container_id)
        destroy_container(proxmox_host, container_id, cfg=cfg)


def _resolve_container_resources(container_cfg) -> ContainerResources:
    """Return explicit container resources, falling back to defaults."""
    resources = container_cfg.resources
    if resources:
        return resources
    return ContainerResources(memory=4096, swap=4096, cores=8, rootfs_size=40)


def _create_swarm_container(
    ctx: SwarmContainerContext, resources: ContainerResources
) -> bool:
    """Create the LXC container from template."""
    cfg = ctx.cfg
    container_cfg = ctx.container_cfg
    create_cmd = PCT.create_cmd(
        container_id=container_cfg.id,
        template_path=ctx.template_path,
        hostname=container_cfg.hostname,
        memory=resources.memory,
        swap=resources.swap,
        cores=resources.cores,
        ip_address=container_cfg.ip_address,
        gateway=cfg.gateway,
        bridge=cfg.proxmox_bridge,
        storage=cfg.proxmox_storage,
        rootfs_size=resources.rootfs_size,
        unprivileged=False,
        ostype="ubuntu",
        arch="amd64",
    )
    create_output = ssh_exec(
        cfg.proxmox_host, create_cmd, check=False, capture_output=True, cfg=cfg
    )
    create_result = CommandWrapper.parse_result(create_output)
    if create_result.has_error:
        logger.error(
            "Failed to create container %s: %s - %s",
            container_cfg.id,
            create_result.error_type.value,
            create_result.error_message,
        )
        return False
    return True


def _configure_container_features(ctx: SwarmContainerContext):
    """Enable required PCT features."""
    cfg = ctx.cfg
    container_id = ctx.container_cfg.id
    features_cmd = PCT.set_features_cmd(container_id, nesting=True, keyctl=True, fuse=True)
    features_output = ssh_exec(
        cfg.proxmox_host, features_cmd, check=False, capture_output=True, cfg=cfg
    )
    features_result = CommandWrapper.parse_result(features_output)
    if features_result.has_error:
        logger.warning(
            "Failed to set container features: %s - %s",
            features_result.error_type.value,
            features_result.error_message,
        )


def _configure_lxc_sysctl_access(proxmox_host, container_id, cfg):
    """Allow manager containers to adjust sysctl."""
    logger.info("Configuring LXC container for sysctl access...")
    sysctl_device_cmd = (
        f"pct set {container_id} -lxc.cgroup2.devices.allow "
        "'c 10:200 rwm' 2>/dev/null || true 2>&1"
    )
    sysctl_mount_cmd = (
        f"pct set {container_id} -lxc.mount.auto 'proc:rw sys:rw' "
        "2>/dev/null || true 2>&1"
    )
    sysctl1_result = ssh_exec(
        proxmox_host,
        sysctl_device_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    sysctl2_result = ssh_exec(
        proxmox_host,
        sysctl_mount_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if sysctl1_result and "error" in sysctl1_result.lower():
        logger.warning("Sysctl configuration had issues: %s", sysctl1_result[-200:])
    if sysctl2_result and "error" in sysctl2_result.lower():
        logger.warning(
            "Sysctl mount configuration had issues: %s",
            sysctl2_result[-200:],
        )


def _start_and_verify_container(ctx: SwarmContainerContext, ip_address: str) -> bool:
    """Start container, confirm it is running, and wait for SSH."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_id = ctx.container_cfg.id

    logger.info("Starting container...")
    start_cmd = PCT.start_cmd(container_id)
    start_output = ssh_exec(
        proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg
    )
    start_result = CommandWrapper.parse_result(start_output)
    if start_result.has_error:
        logger.error(
            "Failed to start container: %s - %s",
            start_result.error_type.value,
            start_result.error_message,
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    time.sleep(cfg.waits.container_startup)
    status_cmd = PCT.status_cmd(container_id)
    status_output = ssh_exec(
        proxmox_host,
        status_cmd,
        check=False,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    if not PCT.parse_status_output(status_output, container_id):
        logger.error(
            "Container %s is not running after start. Status: %s",
            container_id,
            status_output,
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
        logger.error("Container did not become ready")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    return True


def _setup_container_ssh(ctx: SwarmContainerContext, ip_address: str) -> bool:
    """Install SSH keys inside the container."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_id = ctx.container_cfg.id
    logger.info("Setting up SSH key...")
    if setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
        return True
    logger.error("Failed to setup SSH key")
    destroy_container(proxmox_host, container_id, cfg=cfg)
    return False


def _configure_container_proxy(ctx: SwarmContainerContext):
    """Write apt proxy settings when apt-cache is enabled."""
    proxmox_host = ctx.cfg.proxmox_host
    container_id = ctx.container_cfg.id
    apt_cache_ip, apt_cache_port = ctx.apt_cache
    if not apt_cache_ip or not apt_cache_port:
        return
    logger.info("Configuring apt cache...")
    proxy_cmd = (
        "echo 'Acquire::http::Proxy "
        f"\"http://{apt_cache_ip}:{apt_cache_port}\";' "
        "> /etc/apt/apt.conf.d/01proxy || true 2>&1"
    )
    apt_cache_result = pct_exec(
        proxmox_host,
        container_id,
        proxy_cmd,
        check=False,
        capture_output=True,
        cfg=ctx.cfg,
    )
    if apt_cache_result and "error" in apt_cache_result.lower():
        logger.warning(
            "Apt cache configuration had issues: %s",
            apt_cache_result[-200:],
        )


def _ensure_container_docker(ctx: SwarmContainerContext) -> bool:
    """Install and start Docker inside the container."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_id = ctx.container_cfg.id

    docker_cmd = _find_docker_command(proxmox_host, container_id, cfg)
    if not _docker_exists(proxmox_host, container_id, docker_cmd, cfg):
        if not _install_docker(ctx):
            return False
        docker_cmd = _find_docker_command(proxmox_host, container_id, cfg)

    if not _start_docker_service(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not _verify_docker_active(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    return True


def _find_docker_command(proxmox_host, container_id, cfg):
    """Locate docker binary inside container."""
    find_docker_cmd = Docker.find_docker_cmd()
    docker_path = pct_exec(
        proxmox_host,
        container_id,
        find_docker_cmd,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    return docker_path.strip() if docker_path and docker_path.strip() else "docker"


def _docker_exists(proxmox_host, container_id, docker_cmd, cfg):
    """Check whether docker binary appears functional."""
    docker_verify_cmd = " ".join(
        [
            f"command -v {docker_cmd} >/dev/null 2>&1",
            f"&& {docker_cmd} --version",
            f"&& {docker_cmd} ps 2>&1 | head -5",
            "|| echo 'Docker not found'",
        ]
    )
    docker_verify = pct_exec(
        proxmox_host,
        container_id,
        docker_verify_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    return docker_verify and "Docker not found" not in docker_verify


def _install_docker(ctx: SwarmContainerContext) -> bool:
    """Install Docker using official script with apt fallback."""
    logger.info("Docker not installed, installing Docker...")
    proxmox_host = ctx.cfg.proxmox_host
    container_id = ctx.container_cfg.id
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
    pct_exec(
        proxmox_host,
        container_id,
        docker_install_cmd,
        check=False,
        capture_output=True,
        timeout=300,
        cfg=ctx.cfg,
    )
    docker_cmd = _find_docker_command(proxmox_host, container_id, ctx.cfg)
    docker_check_cmd = Docker.is_installed_check_cmd(docker_cmd)
    docker_check_output = pct_exec(
        proxmox_host,
        container_id,
        docker_check_cmd,
        capture_output=True,
        timeout=10,
        cfg=ctx.cfg,
    )
    if Docker.parse_is_installed(docker_check_output):
        logger.info("Docker installed successfully")
        return True

    logger.warning("Docker installation may have failed")
    return False


def _start_docker_service(proxmox_host, container_id, cfg):
    """Start docker service inside the container."""
    logger.info("Starting Docker service...")
    docker_start_cmd = SystemCtl.enable_and_start_cmd("docker")
    docker_start_output = pct_exec(
        proxmox_host,
        container_id,
        docker_start_cmd,
        capture_output=True,
        timeout=30,
        cfg=cfg,
    )
    docker_start_result = CommandWrapper.parse_result(docker_start_output)
    if docker_start_result.has_error:
        logger.error(
            "Failed to start Docker service: %s - %s",
            docker_start_result.error_type.value,
            docker_start_result.error_message,
        )
        return False
    return True


def _verify_docker_active(proxmox_host, container_id, cfg):
    """Verify docker service is running."""
    time.sleep(3)
    is_active_cmd = SystemCtl.is_active_check_cmd("docker")
    docker_status_output = pct_exec(
        proxmox_host,
        container_id,
        is_active_cmd,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    if SystemCtl.parse_is_active(docker_status_output):
        logger.info("Docker service is running")
        return True
    logger.error("Docker service is not running: %s", docker_status_output)
    return False


def _configure_manager_runtime(ctx: SwarmContainerContext):
    """Enable SSH and sysctl tweaks on manager containers."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_id = ctx.container_cfg.id
    logger.info("Ensuring SSH service is running on manager...")
    ssh_result = pct_exec(
        proxmox_host,
        container_id,
        "systemctl start ssh 2>/dev/null || true 2>&1",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if ssh_result and "error" in ssh_result.lower():
        logger.warning("SSH start had issues: %s", ssh_result[-200:])
    logger.info("Configuring sysctl for Docker containers...")
    sysctl_cmd = " ".join(
        [
            "sysctl -w net.ipv4.ip_unprivileged_port_start=0 2>/dev/null || true;",
            "echo 'net.ipv4.ip_unprivileged_port_start=0'"
            " >> /etc/sysctl.conf 2>/dev/null || true 2>&1",
        ]
    )
    sysctl_result = pct_exec(
        proxmox_host,
        container_id,
        sysctl_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if sysctl_result and "error" in sysctl_result.lower():
        logger.warning("Sysctl configuration had issues: %s", sysctl_result[-200:])
    time.sleep(cfg.waits.network_config)


def _ensure_manager_docker(
    context: SwarmDeployContext, manager_config
) -> Optional[str]:
    """Ensure Docker is installed and running on the manager."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_id = manager_config.id
    docker_cmd = _find_docker_command(proxmox_host, manager_id, cfg)
    docker_check_cmd = Docker.is_installed_check_cmd(docker_cmd)
    docker_check_output = pct_exec(
        proxmox_host,
        manager_id,
        docker_check_cmd,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
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
        install_output = pct_exec(
            proxmox_host,
            manager_id,
            docker_install_cmd,
            capture_output=True,
            timeout=300,
            cfg=cfg,
        )
        install_result = CommandWrapper.parse_result(install_output)
        docker_check_output = pct_exec(
            proxmox_host,
            manager_id,
            docker_check_cmd,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        if Docker.parse_is_installed(docker_check_output):
            logger.info("Docker installed successfully")
        else:
            logger.warning("Docker installation may have failed")
            if install_result.has_error:
                logger.warning(
                    "Installer reported: %s - %s",
                    install_result.error_type.value,
                    install_result.error_message,
                )
            else:
                logger.warning("Installer output: %s", install_output[-200:])

    logger.info("Starting Docker service on manager...")
    docker_start_cmd = SystemCtl.enable_and_start_cmd("docker")
    pct_exec(
        proxmox_host,
        manager_id,
        f"{docker_start_cmd} && systemctl status docker --no-pager | head -5",
        cfg=cfg,
    )
    time.sleep(cfg.waits.swarm_init)
    return docker_cmd


def _initialize_swarm_manager(
    context: SwarmDeployContext, manager_config, docker_cmd
) -> bool:
    """Initialize Docker swarm on manager node."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_id = manager_config.id
    manager_ip = manager_config.ip_address
    logger.info("\nInitializing Docker Swarm on manager node...")
    swarm_init_cmd = Docker.swarm_init_cmd(docker_cmd, manager_ip)
    swarm_init_output = pct_exec(
        proxmox_host, manager_id, swarm_init_cmd, capture_output=True, cfg=cfg
    )
    swarm_init_result = CommandWrapper.parse_result(swarm_init_output)
    if "already part of a swarm" in (swarm_init_output or ""):
        logger.info("Swarm already initialized, continuing...")
    elif swarm_init_result.has_error:
        logger.warning(
            "Swarm initialization had errors: %s - %s",
            swarm_init_result.error_type.value,
            swarm_init_result.error_message,
        )
    else:
        logger.info("Swarm initialized successfully")
    return True


def _fetch_worker_join_token(context, manager_config, docker_cmd):
    """Retrieve the swarm worker join token."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_id = manager_config.id
    logger.info("Getting worker join token...")
    join_token_cmd = Docker.swarm_join_token_cmd(docker_cmd, role="worker")
    join_token_output = pct_exec(
        proxmox_host, manager_id, join_token_cmd, capture_output=True, cfg=cfg
    )
    for line in join_token_output.strip().split("\n"):
        line = line.strip()
        if (
            line
            and len(line) > 20
            and not line.startswith("Error")
            and not line.startswith("Warning")
        ):
            return line
    logger.error("Could not get worker join token. Output: %s", join_token_output)
    return None


def _join_workers_to_swarm(context, docker_cmd, join_token):  # pylint: disable=too-many-locals
    """Join all worker containers to the swarm."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_config = context.managers[0]
    manager_id = manager_config.id
    manager_hostname = manager_config.hostname
    manager_ip = manager_config.ip_address

    logger.info("Setting manager node availability to drain...")
    node_update_cmd = Docker.node_update_cmd(docker_cmd, manager_hostname, "drain")
    pct_exec(proxmox_host, manager_id, node_update_cmd, cfg=cfg)

    find_docker_cmd = Docker.find_docker_cmd()
    swarm_port = cfg.swarm_port

    for worker_config in context.workers:
        worker_id = worker_config.id
        worker_hostname = worker_config.hostname
        worker_ip = worker_config.ip_address
        logger.info("Joining %s (%s) to swarm...", worker_hostname, worker_ip)
        worker_docker_path = pct_exec(
            proxmox_host,
            worker_id,
            find_docker_cmd,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        worker_docker_cmd = (
            worker_docker_path.strip()
            if worker_docker_path and worker_docker_path.strip()
            else "docker"
        )
        join_cmd = Docker.swarm_join_cmd(
            worker_docker_cmd, join_token, f"{manager_ip}:{swarm_port}"
        )
        join_output = pct_exec(
            proxmox_host, worker_id, join_cmd, capture_output=True, cfg=cfg
        )
        if "already part of a swarm" in join_output:
            logger.info("Node %s already part of swarm", worker_hostname)
        elif "This node joined a swarm" in join_output:
            logger.info("Node %s joined swarm successfully", worker_hostname)
        else:
            logger.warning("Node %s join had issues:", worker_hostname)
            logger.warning(join_output)

    return True


def _install_portainer(context: SwarmDeployContext, docker_cmd: str) -> bool:  # pylint: disable=too-many-locals
    """Install and verify Portainer on the manager node."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_config = context.managers[0]
    manager_id = manager_config.id

    logger.info("\nVerifying swarm status...")
    node_ls_cmd = Docker.node_ls_cmd(docker_cmd)
    pct_exec(proxmox_host, manager_id, node_ls_cmd, cfg=cfg)

    logger.info("\nInstalling Portainer CE...")
    volume_create_cmd = Docker.volume_create_cmd(docker_cmd, "portainer_data")
    pct_exec(proxmox_host, manager_id, volume_create_cmd, cfg=cfg)

    stop_cmd = Docker.stop_cmd(docker_cmd, "portainer")
    rm_cmd = Docker.rm_cmd(docker_cmd, "portainer")
    pct_exec(proxmox_host, manager_id, f"{stop_cmd}; {rm_cmd}", cfg=cfg)

    portainer_image = cfg.portainer_image
    logger.info("Creating Portainer container...")
    portainer_cmd = Docker.run_cmd(
        docker_cmd,
        portainer_image,
        "portainer",
        restart="always",
        network="host",
        volumes=[
            "/var/run/docker.sock:/var/run/docker.sock",
            "portainer_data:/data",
        ],
    )
    pct_exec(proxmox_host, manager_id, portainer_cmd, cfg=cfg)

    time.sleep(cfg.waits.portainer_start)

    logger.info("Verifying Portainer is running...")
    ps_format = "{{.Names}} {{.Status}}"
    portainer_status_cmd = " ".join(
        [
            f"{docker_cmd} ps --format '{ps_format}' | grep portainer",
            "||",
            f"{docker_cmd} ps -a --format '{ps_format}' | grep portainer",
        ]
    )
    portainer_status = pct_exec(
        proxmox_host,
        manager_id,
        portainer_status_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if portainer_status:
        logger.info("Portainer status: %s", portainer_status)
    else:
        logger.warning("Portainer container not found")

    portainer_running = pct_exec(
        proxmox_host,
        manager_id,
        f"{docker_cmd} ps --format '{{{{.Names}}}}' | grep -q '^portainer$' && echo yes || echo no",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if "no" in portainer_running:
        logger.warning("Portainer failed to start. Checking logs...")
        logs = pct_exec(
            proxmox_host,
            manager_id,
            f"{docker_cmd} logs portainer 2>&1 | tail -20",
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if logs:
            logger.warning(logs)

    return True
