"""Docker Swarm orchestration utilities."""
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple
from cli import Apt, CommandWrapper, Docker, PCT, SystemCtl
from libs import common
from libs.config import LabConfig, ContainerResources
from libs.logger import get_logger
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)
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
    """Deploy Docker Swarm - containers should already exist from deploy process"""
    context = _build_swarm_context(cfg)
    if not context:
        return False
    # Containers should already exist from the deploy process
    # We only need to perform swarm-specific orchestration (init, join, portainer)
    # Docker should already be installed and running via actions
    manager_config = context.managers[0]
    docker_cmd = _get_manager_docker_command(context, manager_config)
    if not docker_cmd:
        logger.error("Docker command not found on manager")
        return False
    if not _initialize_swarm_manager(context, manager_config, docker_cmd):
        return False
    join_token = _fetch_worker_join_token(context, manager_config, docker_cmd)
    if not join_token:
        return False
    result = _join_workers_to_swarm(context, docker_cmd, join_token) and _install_portainer(context, docker_cmd)
    if result:
        logger.info("Docker Swarm deployed")
    return result

def _build_swarm_context(cfg: LabConfig) -> Optional[SwarmDeployContext]:
    """Collect and validate configuration needed for swarm deployment."""
    if not cfg.swarm or not cfg.swarm.managers or not cfg.swarm.workers:
        logger.error("Swarm configuration not found or incomplete")
        return None
    # Find containers by ID from swarm config
    manager_ids = set(cfg.swarm.managers)
    worker_ids = set(cfg.swarm.workers)
    managers = [c for c in cfg.containers if c.id in manager_ids]
    workers = [c for c in cfg.containers if c.id in worker_ids]
    if not managers or not workers:
        logger.error("Swarm manager or worker containers not found in configuration")
        return None
    # Containers should already exist from deploy process, so we don't need template_path
    # But we keep it for backward compatibility with SwarmDeployContext
    template_path = ""  # Not used when containers already exist
    apt_cache = _get_apt_cache_proxy(cfg)
    return SwarmDeployContext(cfg, managers, workers, template_path, apt_cache)

def _get_apt_cache_proxy(cfg: LabConfig):
    """Return apt-cache proxy settings if available."""
    apt_cache = next((c for c in cfg.containers if c.name == cfg.apt_cache_ct), None)
    if not apt_cache:
        return None, None
    return apt_cache.ip_address, cfg.apt_cache_port

def _deploy_single_swarm_container(ctx: SwarmContainerContext) -> bool:
    """Provision one swarm container from template through Docker readiness."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_cfg = ctx.container_cfg
    container_id = container_cfg.id
    hostname = container_cfg.hostname
    ip_address = container_cfg.ip_address
    logger.info("\nDeploying container %s (%s)...", container_id, hostname)
    
    # Only destroy and recreate if container doesn't exist
    if not container_exists(proxmox_host, container_id, cfg=cfg):
        resources = _resolve_container_resources(container_cfg)
        if not _create_swarm_container(ctx, resources):
            return False
        _configure_container_features(ctx)
        # Configure sysctl access for managers (containers in swarm.managers)
        if cfg.swarm and container_id in cfg.swarm.managers:
            _configure_lxc_sysctl_access(proxmox_host, container_id, cfg)
        if not _start_and_verify_container(ctx, ip_address):
            return False
        if not _setup_container_ssh(ctx, ip_address):
            return False
        _configure_container_proxy(ctx)
    else:
        logger.info("Container %s already exists, reusing it", container_id)
        # Ensure container is running
        status_cmd = PCT().container_id(container_id).status()
        status_output = ssh_exec(proxmox_host, status_cmd, check=False, timeout=10, cfg=cfg)
        if not PCT.parse_status_output(status_output, container_id):
            logger.info("Starting existing container %s...", container_id)
            start_cmd = PCT().container_id(container_id).start()
            ssh_exec(proxmox_host, start_cmd, check=False, cfg=cfg)
            time.sleep(cfg.waits.container_startup)
        # Wait for container to be ready
        if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
            logger.error("Container did not become ready")
            return False
    
    # Ensure Docker is running (for both new and existing containers)
    if not _ensure_container_docker(ctx):
        return False
    # Configure manager runtime for managers (containers in swarm.managers)
    if cfg.swarm and container_id in cfg.swarm.managers:
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

def _create_swarm_container(ctx: SwarmContainerContext, resources: ContainerResources) -> bool:
    """Create the LXC container from template."""
    cfg = ctx.cfg
    container_cfg = ctx.container_cfg
    create_cmd = (
        PCT()
        .container_id(container_cfg.id)
        .create(
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
    )
    create_output = ssh_exec(cfg.proxmox_host, create_cmd, check=False, cfg=cfg)
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
    features_cmd = PCT().container_id(container_id).nesting().keyctl().fuse().set_features()
    features_output = ssh_exec(cfg.proxmox_host, features_cmd, check=False, cfg=cfg)
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
    sysctl_device_cmd = f"pct set {container_id} -lxc.cgroup2.devices.allow " "'c 10:200 rwm' 2>/dev/null || true 2>&1"
    sysctl_mount_cmd = f"pct set {container_id} -lxc.mount.auto 'proc:rw sys:rw' " "2>/dev/null || true 2>&1"
    sysctl1_result = ssh_exec(proxmox_host, sysctl_device_cmd, check=False, cfg=cfg,
    )
    sysctl2_result = ssh_exec(proxmox_host, sysctl_mount_cmd, check=False, cfg=cfg,
    )
    if sysctl1_result and "error" in sysctl1_result.lower():
        logger.warning("Sysctl configuration had issues: %s", sysctl1_result[-200:])
    if sysctl2_result and "error" in sysctl2_result.lower():
        logger.warning("Sysctl mount configuration had issues: %s", sysctl2_result[-200:],
        )

def _start_and_verify_container(ctx: SwarmContainerContext, ip_address: str) -> bool:
    """Start container, confirm it is running, and wait for SSH."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_id = ctx.container_cfg.id
    logger.info("Starting container...")
    start_cmd = PCT().container_id(container_id).start()
    start_output = ssh_exec(proxmox_host, start_cmd, check=False, cfg=cfg)
    start_result = CommandWrapper.parse_result(start_output)
    if start_result.has_error:
        logger.error("Failed to start container: %s - %s", start_result.error_type.value, start_result.error_message,
        )
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    time.sleep(cfg.waits.container_startup)
    status_cmd = PCT().container_id(container_id).status()
    status_output = ssh_exec(proxmox_host, status_cmd, check=False, timeout=10, cfg=cfg,
    )
    if not PCT.parse_status_output(status_output, container_id):
        logger.error("Container %s is not running after start. Status: %s", container_id, status_output,
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
        f'"http://{apt_cache_ip}:{apt_cache_port}";\' '
        "> /etc/apt/apt.conf.d/01proxy || true"
    )
    lxc_service = LXCService(proxmox_host, ctx.cfg.ssh)
    if lxc_service.connect():
        try:
            pct_service = PCTService(lxc_service)
            apt_cache_result, _ = pct_service.execute(str(container_id), proxy_cmd)
            if apt_cache_result and "error" in apt_cache_result.lower():
                logger.warning("Apt cache configuration had issues: %s", apt_cache_result[-200:])
        finally:
            lxc_service.disconnect()

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
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return "docker"
    try:
        pct_service = PCTService(lxc_service)
        find_docker_cmd = Docker().find_docker()
        docker_path, _ = pct_service.execute(str(container_id), find_docker_cmd, timeout=10)
        if docker_path and docker_path.strip():
            # Take only the first line (first path found)
            first_line = docker_path.strip().split("\n")[0].strip()
            return first_line if first_line else "docker"
        return "docker"
    finally:
        lxc_service.disconnect()

def _docker_exists(proxmox_host, container_id, docker_cmd, cfg):
    """Check whether docker binary appears functional."""
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        docker_verify_cmd = " ".join(
            [
                f"command -v {docker_cmd} >/dev/null",
                f"&& {docker_cmd} --version",
                f"&& {docker_cmd} ps | head -5",
                "|| echo 'Docker not found'",
            ]
        )
        docker_verify, _ = pct_service.execute(str(container_id), docker_verify_cmd)
        return docker_verify and "Docker not found" not in docker_verify
    finally:
        lxc_service.disconnect()

def _install_docker(ctx: SwarmContainerContext) -> bool:
    """Install Docker using official script with apt fallback."""
    logger.info("Docker not installed, installing Docker...")
    proxmox_host = ctx.cfg.proxmox_host
    container_id = ctx.container_cfg.id
    docker_install_cmd = (
        "rm -f /etc/apt/apt.conf.d/01proxy; "
        "apt update -qq && "
        "if command -v curl >/dev/null; then "
        "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh && "
        "  sh /tmp/get-docker.sh | tail -20 || "
        "  (echo 'get.docker.com failed, trying docker.io...' && "
        "   apt install -y docker.io | tail -20); "
        "else "
        "  echo 'curl not available, installing docker.io...'; "
        "  apt install -y docker.io | tail -20; "
        "fi"
    )
    lxc_service = LXCService(proxmox_host, ctx.cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        pct_service.execute(str(container_id), docker_install_cmd, timeout=300)
        docker_cmd = _find_docker_command(proxmox_host, container_id, ctx.cfg)
        docker_check_cmd = Docker().docker_cmd(docker_cmd).is_installed_check()
        docker_check_output, _ = pct_service.execute(str(container_id), docker_check_cmd, timeout=10)
    finally:
        lxc_service.disconnect()
    if Docker.parse_is_installed(docker_check_output):
        logger.info("Docker installed successfully")
        return True
    logger.warning("Docker installation may have failed")
    return False

def _start_docker_service(proxmox_host, container_id, cfg):
    """Start docker service inside the container."""
    logger.info("Starting Docker service...")
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        docker_start_cmd = SystemCtl().service("docker").enable_and_start()
        docker_start_output, _ = pct_service.execute(str(container_id), docker_start_cmd, timeout=30)
        docker_start_result = CommandWrapper.parse_result(docker_start_output)
        if docker_start_result.has_error:
            logger.error(
                "Failed to start Docker service: %s - %s",
                docker_start_result.error_type.value,
                docker_start_result.error_message,
            )
            return False
        return True
    finally:
        lxc_service.disconnect()

def _verify_docker_active(proxmox_host, container_id, cfg):
    """Verify docker service is running."""
    time.sleep(3)
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        is_active_cmd = SystemCtl().service("docker").is_active()
        docker_status_output, _ = pct_service.execute(str(container_id), is_active_cmd, timeout=10)
        if SystemCtl.parse_is_active(docker_status_output):
            logger.info("Docker service is running")
            return True
        logger.error("Docker service is not running: %s", docker_status_output)
        return False
    finally:
        lxc_service.disconnect()

def _configure_manager_runtime(ctx: SwarmContainerContext):
    """Enable SSH and sysctl tweaks on manager containers."""
    cfg = ctx.cfg
    proxmox_host = cfg.proxmox_host
    container_id = ctx.container_cfg.id
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return
    try:
        pct_service = PCTService(lxc_service)
        logger.info("Ensuring SSH service is running on manager...")
        ssh_result, _ = pct_service.execute(str(container_id), "systemctl start ssh 2>/dev/null || true 2>&1")
        if ssh_result and "error" in ssh_result.lower():
            logger.warning("SSH start had issues: %s", ssh_result[-200:])
        logger.info("Configuring sysctl for Docker containers...")
        sysctl_cmd = " ".join(
            [
                "sysctl -w net.ipv4.ip_unprivileged_port_start=0 2>/dev/null || true;",
                "echo 'net.ipv4.ip_unprivileged_port_start=0'" " >> /etc/sysctl.conf 2>/dev/null || true 2>&1",
            ]
        )
        sysctl_result, _ = pct_service.execute(str(container_id), sysctl_cmd)
        if sysctl_result and "error" in sysctl_result.lower():
            logger.warning("Sysctl configuration had issues: %s", sysctl_result[-200:])
        time.sleep(cfg.waits.network_config)
    finally:
        lxc_service.disconnect()

def _get_manager_docker_command(context: SwarmDeployContext, manager_config) -> Optional[str]:
    """Get Docker command from manager (Docker should already be installed via actions)."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_id = manager_config.id
    docker_cmd = _find_docker_command(proxmox_host, manager_id, cfg)
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return None
    try:
        pct_service = PCTService(lxc_service)
        docker_check_cmd = Docker().docker_cmd(docker_cmd).is_installed_check()
        docker_check_output, _ = pct_service.execute(str(manager_id), docker_check_cmd, timeout=10)
        if not Docker.parse_is_installed(docker_check_output):
            logger.error("Docker not found on manager - actions should have installed it")
            return None
        logger.info("Docker is available on manager")
        return docker_cmd
    finally:
        lxc_service.disconnect()

def _initialize_swarm_manager(context: SwarmDeployContext, manager_config, docker_cmd) -> bool:
    """Initialize Docker swarm on manager node."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_id = manager_config.id
    manager_ip = manager_config.ip_address
    logger.info("\nInitializing Docker Swarm on manager node...")
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        swarm_init_cmd = Docker().docker_cmd(docker_cmd).swarm_init(manager_ip)
        swarm_init_output, _ = pct_service.execute(str(manager_id), swarm_init_cmd)
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
    finally:
        lxc_service.disconnect()

def _fetch_worker_join_token(context, manager_config, docker_cmd):
    """Retrieve the swarm worker join token."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_id = manager_config.id
    logger.info("Getting worker join token...")
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return None
    try:
        pct_service = PCTService(lxc_service)
        join_token_cmd = Docker().docker_cmd(docker_cmd).swarm_join_token("worker")
        join_token_output, _ = pct_service.execute(str(manager_id), join_token_cmd)
        if not join_token_output:
            logger.error("Could not get worker join token: no output")
            return None
        for line in join_token_output.strip().split("\n"):
            line = line.strip()
            if line and len(line) > 20 and not line.startswith("Error") and not line.startswith("Warning"):
                return line
        logger.error("Could not get worker join token. Output: %s", join_token_output)
        return None
    finally:
        lxc_service.disconnect()

def _join_workers_to_swarm(context, docker_cmd, join_token):  # pylint: disable=too-many-locals
    """Join all worker containers to the swarm."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_config = context.managers[0]
    manager_id = manager_config.id
    manager_hostname = manager_config.hostname
    manager_ip = manager_config.ip_address
    logger.info("Setting manager node availability to drain...")
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        node_update_cmd = Docker().docker_cmd(docker_cmd).node_update(manager_hostname, "drain")
        pct_service.execute(str(manager_id), node_update_cmd)
        find_docker_cmd = Docker().find_docker()
        swarm_port = cfg.swarm_port
        for worker_config in context.workers:
            worker_id = worker_config.id
            worker_hostname = worker_config.hostname
            worker_ip = worker_config.ip_address
            logger.info("Joining %s (%s) to swarm...", worker_hostname, worker_ip)
            worker_docker_path, _ = pct_service.execute(str(worker_id), find_docker_cmd, timeout=10)
            if worker_docker_path and worker_docker_path.strip():
                # Take only the first line (first path found)
                first_line = worker_docker_path.strip().split("\n")[0].strip()
                worker_docker_cmd = first_line if first_line else "docker"
            else:
                worker_docker_cmd = "docker"
            join_cmd = Docker().docker_cmd(worker_docker_cmd).swarm_join(join_token, f"{manager_ip}:{swarm_port}")
            join_output, _ = pct_service.execute(str(worker_id), join_cmd)
            if not join_output:
                logger.error("Failed to join node %s to swarm: no output", worker_hostname)
                continue
            if "already part of a swarm" in (join_output or ""):
                logger.info("Node %s already part of swarm", worker_hostname)
            elif "This node joined a swarm" in (join_output or ""):
                logger.info("Node %s joined swarm successfully", worker_hostname)
            else:
                logger.warning("Node %s join had issues:", worker_hostname)
                logger.warning(join_output)
        return True
    finally:
        lxc_service.disconnect()

def _install_portainer(context: SwarmDeployContext, docker_cmd: str) -> bool:  # pylint: disable=too-many-locals
    """Install and verify Portainer on the manager node."""
    proxmox_host = context.proxmox_host
    cfg = context.cfg
    manager_config = context.managers[0]
    manager_id = manager_config.id
    lxc_service = LXCService(proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        return False
    try:
        pct_service = PCTService(lxc_service)
        logger.info("\nVerifying swarm status...")
        node_ls_cmd = Docker().docker_cmd(docker_cmd).node_ls()
        pct_service.execute(str(manager_id), node_ls_cmd)
        logger.info("\nInstalling Portainer CE...")
        stop_cmd = Docker().docker_cmd(docker_cmd).stop("portainer")
        rm_cmd = Docker().docker_cmd(docker_cmd).rm("portainer")
        pct_service.execute(str(manager_id), f"{stop_cmd}; {rm_cmd}")
        portainer_image = cfg.portainer_image
        logger.info("Creating portainer_data volume...")
        volume_create_cmd = Docker().docker_cmd(docker_cmd).volume_create("portainer_data")
        pct_service.execute(str(manager_id), volume_create_cmd)
        logger.info("Creating Portainer container...")
        portainer_cmd = (
            Docker()
            .docker_cmd(docker_cmd)
            .run(
                portainer_image,
                "portainer",
                restart="always",
                network="host",
                volumes=[
                    "/var/run/docker.sock:/var/run/docker.sock",
                    "portainer_data:/data",
                ],
                security_opts=["apparmor=unconfined"],
                )
            )
        pct_service.execute(str(manager_id), portainer_cmd)
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
        portainer_status, _ = pct_service.execute(str(manager_id), portainer_status_cmd)
        if portainer_status:
            logger.info("Portainer status: %s", portainer_status)
        else:
            logger.warning("Portainer container not found")
            portainer_running, _ = pct_service.execute(
                str(manager_id),
                f"{docker_cmd} ps --format '{{{{.Names}}}}' | grep -q '^portainer$' && echo yes || echo no",
            )
            if portainer_running and "no" in portainer_running:
                logger.warning("Portainer failed to start. Checking logs...")
                logs, _ = pct_service.execute(
                    str(manager_id),
                    f"{docker_cmd} logs portainer 2>&1 | tail -20",
                )
                if logs:
                    logger.warning(logs)
                return False
        return True
    finally:
        lxc_service.disconnect()
