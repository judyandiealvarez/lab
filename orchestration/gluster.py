# pylint: disable=duplicate-code
"""GlusterFS distributed storage orchestration."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from cli import Apt, CommandWrapper, Gluster, SystemCtl
from libs import common
from libs.config import LabConfig
from libs.logger import get_logger

logger = get_logger(__name__)

pct_exec = common.pct_exec
ssh_exec = common.ssh_exec
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
container_exists = common.container_exists


@dataclass(frozen=True)
class NodeInfo:
    """Minimal representation of a container needed for orchestration steps."""

    container_id: int
    hostname: str
    ip_address: str

    @classmethod
    def from_container(cls, container_cfg):
        """Build node info from container configuration."""
        return cls(
            container_id=container_cfg.id,
            hostname=container_cfg.hostname,
            ip_address=container_cfg.ip_address,
        )


def setup_glusterfs(cfg: LabConfig):
    """Setup GlusterFS distributed storage across Swarm nodes"""
    logger.info("\n[5/7] Setting up GlusterFS distributed storage...")

    if not cfg.glusterfs:
        logger.info("GlusterFS configuration not found, skipping...")
        return True

    gluster_cfg = cfg.glusterfs

    manager, workers = _collect_gluster_nodes(cfg)
    if not manager or not workers:
        return False

    all_nodes = [manager] + workers
    apt_cache_ip, apt_cache_port = _get_apt_cache_proxy(cfg)
    proxy_settings = (apt_cache_ip, apt_cache_port)

    logger.info("Installing GlusterFS server on all nodes...")
    failure_detected = False
    ordered_steps = [
        lambda: _fix_apt_sources(all_nodes, cfg),
        lambda: _install_gluster_packages(all_nodes, proxy_settings, cfg),
        lambda: _delay(cfg.waits.glusterfs_setup),
        lambda: _create_bricks(workers, gluster_cfg.brick_path, cfg),
    ]
    for step in ordered_steps:
        if not step():
            failure_detected = True
            break

    gluster_cmd = None
    if not failure_detected:
        gluster_cmd = _resolve_gluster_cmd(manager, cfg)
        if not gluster_cmd:
            failure_detected = True

    if not failure_detected and not _peer_workers(manager, workers, gluster_cmd, cfg):
        failure_detected = True

    if not failure_detected:
        peers_ready = _wait_for_peers(manager, workers, gluster_cmd, cfg)
        if not peers_ready:
            logger.warning("Not all peers may be fully connected, continuing anyway...")

    if not failure_detected and not _ensure_volume(
        manager, workers, gluster_cmd, gluster_cfg, cfg
    ):
        failure_detected = True

    if not failure_detected and not _mount_gluster_volume(
        manager, workers, gluster_cfg, cfg
    ):
        failure_detected = True

    if failure_detected:
        return False

    _log_gluster_summary(gluster_cfg)
    return True


def _collect_gluster_nodes(cfg: LabConfig) -> Tuple[Optional[NodeInfo], Sequence[NodeInfo]]:
    """Return manager and worker nodes as NodeInfo objects."""
    managers = [c for c in cfg.containers if c.type == "swarm-manager"]
    workers = [c for c in cfg.containers if c.type == "swarm-node"]
    if not managers or not workers:
        logger.error("Swarm managers or workers not found")
        return None, []
    manager_node = NodeInfo.from_container(managers[0])
    worker_nodes = [NodeInfo.from_container(worker) for worker in workers]
    return manager_node, worker_nodes


def _get_apt_cache_proxy(cfg: LabConfig):
    """Return apt-cache proxy settings if available."""
    apt_cache = next((c for c in cfg.containers if c.type == "apt-cache"), None)
    if not apt_cache:
        return None, None
    return apt_cache.ip_address, cfg.apt_cache_port


def _delay(seconds):
    """Sleep helper that always returns True for step sequencing."""
    time.sleep(seconds)
    return True


def _fix_apt_sources(nodes, cfg):
    """Ensure all nodes use the expected Ubuntu sources."""
    proxmox_host = cfg.proxmox_host
    for node in nodes:
        logger.info("Fixing apt sources on %s...", node.hostname)
        sources_cmd = " ".join(
            [
                (
                    "sed -i 's/oracular/plucky/g' /etc/apt/sources.list "
                    "2>/dev/null || true;"
                ),
                (
                    "if ! grep -q '^deb.*plucky.*main' /etc/apt/sources.list; then"
                ),
                (
                    "echo 'deb http://archive.ubuntu.com/ubuntu plucky main "
                    "universe multiverse' > /etc/apt/sources.list;"
                ),
                (
                    "echo 'deb http://archive.ubuntu.com/ubuntu plucky-updates main "
                    "universe multiverse' >> /etc/apt/sources.list;"
                ),
                (
                    "echo 'deb http://archive.ubuntu.com/ubuntu plucky-security main "
                    "universe multiverse' >> /etc/apt/sources.list;"
                ),
                "fi 2>&1",
            ]
        )
        sources_result = pct_exec(
            proxmox_host,
            node.container_id,
            sources_cmd,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if sources_result and "error" in sources_result.lower():
            logger.warning(
                "Apt sources fix had issues on %s: %s",
                node.hostname,
                sources_result[-200:],
            )
    return True


def _install_gluster_packages(nodes, proxy_settings, cfg):
    """Install GlusterFS packages and ensure glusterd is running on each node."""
    for node in nodes:
        logger.info("Installing on %s...", node.hostname)
        if not _configure_gluster_node(
            node, proxy_settings, cfg
        ):
            return False
    return True


def _configure_gluster_node(node, proxy_settings, cfg, max_retries=2):
    """Configure GlusterFS packages on a single node."""
    proxmox_host = cfg.proxmox_host
    for attempt in range(1, max_retries + 1):
        _configure_proxy(
            node.container_id,
            attempt == 1,
            proxy_settings,
            cfg,
        )
        update_cmd = Apt.update_cmd(quiet=True)
        update_output = pct_exec(
            proxmox_host,
            node.container_id,
            update_cmd,
            capture_output=True,
            timeout=120,
            cfg=cfg,
        )
        update_result = CommandWrapper.parse_result(update_output)
        if _should_retry_update(update_result) and attempt < max_retries:
            logger.warning("apt update failed, will retry without proxy...")
            continue

        install_cmd = Apt.install_cmd(["glusterfs-server", "glusterfs-client"])
        install_output = pct_exec(
            proxmox_host,
            node.container_id,
            install_cmd,
            capture_output=True,
            timeout=300,
            cfg=cfg,
        )
        install_result = CommandWrapper.parse_result(install_output)
        verify_cmd = Gluster.is_installed_check_cmd("gluster")
        verify_output = pct_exec(
            proxmox_host,
            node.container_id,
            verify_cmd,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        if Gluster.parse_is_installed(verify_output):
            logger.info("GlusterFS installed successfully on %s", node.hostname)
            return _ensure_glusterd_running(node, cfg)

        logger.warning(
            "Installation attempt %s failed on %s: %s - %s",
            attempt,
            node.hostname,
            install_result.error_type.value
            if install_result.error_type
            else "unknown",
            install_result.error_message,
        )
        if attempt < max_retries:
            logger.info("Retrying without proxy...")
            time.sleep(2)

    logger.error(
        "Failed to install GlusterFS on %s after %s attempts",
        node.hostname,
        max_retries,
    )
    return False


def _configure_proxy(container_id, use_proxy, proxy_settings, cfg):
    """Enable or disable apt proxy on a node."""
    proxmox_host = cfg.proxmox_host
    apt_cache_ip, apt_cache_port = proxy_settings
    if use_proxy and apt_cache_ip and apt_cache_port:
        proxy_cmd = (
            "echo 'Acquire::http::Proxy "
            f"\"http://{apt_cache_ip}:{apt_cache_port}\";' "
            "> /etc/apt/apt.conf.d/01proxy || true 2>&1"
        )
        proxy_result = pct_exec(
            proxmox_host,
            container_id,
            proxy_cmd,
            check=False,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        if proxy_result and "error" in proxy_result.lower():
            logger.warning(
                "Proxy configuration had issues: %s", proxy_result[-200:]
            )
    else:
        rm_proxy_result = pct_exec(
            proxmox_host,
            container_id,
            "rm -f /etc/apt/apt.conf.d/01proxy 2>&1",
            check=False,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        if rm_proxy_result and "error" in rm_proxy_result.lower():
            logger.warning(
                "Proxy removal had issues: %s", rm_proxy_result[-200:]
            )


def _should_retry_update(update_result):
    """Determine if apt update should be retried without proxy."""
    return bool(
        update_result.has_error
        or (
            update_result.output
            and (
                "Failed to fetch" in update_result.output
                or "Unable to connect" in update_result.output
            )
        )
    )


def _ensure_glusterd_running(node, cfg):
    """Enable, start, and verify glusterd on a node."""
    logger.info("Starting glusterd service on %s...", node.hostname)
    proxmox_host = cfg.proxmox_host
    glusterd_start_cmd = SystemCtl.enable_and_start_cmd("glusterd")
    glusterd_start_output = pct_exec(
        proxmox_host,
        node.container_id,
        glusterd_start_cmd,
        capture_output=True,
        timeout=30,
        cfg=cfg,
    )
    glusterd_start_result = CommandWrapper.parse_result(glusterd_start_output)
    if glusterd_start_result.has_error:
        logger.error(
            "Failed to start glusterd on %s: %s - %s",
            node.hostname,
            glusterd_start_result.error_type.value,
            glusterd_start_result.error_message,
        )
        return False

    time.sleep(3)
    is_active_cmd = SystemCtl.is_active_check_cmd("glusterd")
    glusterd_check_output = pct_exec(
        proxmox_host,
        node.container_id,
        is_active_cmd,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )

    if SystemCtl.parse_is_active(glusterd_check_output):
        logger.info("%s: GlusterFS installed and glusterd running", node.hostname)
        return True

    logger.error(
        "%s: GlusterFS installed but glusterd is not running: %s",
        node.hostname,
        glusterd_check_output,
    )
    return False


def _create_bricks(workers, brick_path, cfg):
    """Create brick directories on worker nodes."""
    logger.info("Creating brick directories on worker nodes...")
    proxmox_host = cfg.proxmox_host
    for worker in workers:
        logger.info("Creating brick on %s...", worker.hostname)
        brick_result = pct_exec(
            proxmox_host,
            worker.container_id,
            f"mkdir -p {brick_path} && chmod 755 {brick_path} 2>&1",
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if brick_result and "error" in brick_result.lower():
            logger.error(
                "Failed to create brick directory on %s: %s",
                worker.hostname,
                brick_result[-300:],
            )
            return False
    return True


def _resolve_gluster_cmd(manager: NodeInfo, cfg):
    """Find the gluster executable inside the manager container."""
    find_gluster_cmd = Gluster.find_gluster_cmd()
    gluster_path = pct_exec(
        cfg.proxmox_host,
        manager.container_id,
        find_gluster_cmd,
        capture_output=True,
        timeout=10,
        cfg=cfg,
    )
    if not gluster_path:
        logger.error("Unable to locate gluster binary")
        return None
    return gluster_path.strip() or "gluster"


def _peer_workers(manager, workers, gluster_cmd, cfg):
    """Peer all worker nodes to the manager."""
    logger.info("Peering worker nodes together...")
    proxmox_host = cfg.proxmox_host
    for worker in workers:
        logger.info("Adding %s (%s) to cluster...", worker.hostname, worker.ip_address)
        probe_cmd = (
            f"{Gluster.peer_probe_cmd(gluster_cmd, worker.hostname)} || "
            f"{Gluster.peer_probe_cmd(gluster_cmd, worker.ip_address)}"
        )
        probe_output = pct_exec(
            proxmox_host,
            manager.container_id,
            probe_cmd,
            capture_output=True,
            cfg=cfg,
        )
        probe_result = CommandWrapper.parse_result(probe_output)
        if (
            probe_result.has_error
            and "already" not in (probe_output or "").lower()
            and "already in peer list" not in (probe_output or "").lower()
        ):
            logger.warning(
                "Peer probe had issues for %s: %s - %s",
                worker.hostname,
                probe_result.error_type.value,
                probe_result.error_message,
            )
    time.sleep(10)
    return True


def _wait_for_peers(manager, workers, gluster_cmd, cfg):
    """Wait until all peers report as connected."""
    logger.info("Verifying peer status...")
    proxmox_host = cfg.proxmox_host
    max_peer_attempts = 10
    for attempt in range(1, max_peer_attempts + 1):
        peer_status_cmd = Gluster.peer_status_cmd(gluster_cmd)
        peer_status = pct_exec(
            proxmox_host,
            manager.container_id,
            peer_status_cmd,
            capture_output=True,
            cfg=cfg,
        )
        if peer_status:
            logger.info(peer_status)
        connected_count = peer_status.count("Peer in Cluster (Connected)")
        if connected_count >= len(workers):
            logger.info("All %s worker peers connected", connected_count)
            return True
        if attempt < max_peer_attempts:
            logger.info(
                "Waiting for peers to connect... (%s/%s)",
                attempt,
                max_peer_attempts,
            )
            time.sleep(3)
    return False


def _ensure_volume(  # pylint: disable=too-many-locals
    manager,
    workers,
    gluster_cmd,
    gluster_cfg,
    cfg,
):
    """Create the Gluster volume if needed and ensure it is running."""
    proxmox_host = cfg.proxmox_host
    volume_name = gluster_cfg.volume_name
    brick_path = gluster_cfg.brick_path
    replica_count = gluster_cfg.replica_count

    logger.info("Creating GlusterFS volume '%s'...", volume_name)
    volume_exists_cmd = Gluster.volume_exists_check_cmd(gluster_cmd, volume_name)
    volume_exists_output = pct_exec(
        proxmox_host,
        manager.container_id,
        volume_exists_cmd,
        capture_output=True,
        cfg=cfg,
    )

    if Gluster.parse_volume_exists(volume_exists_output):
        logger.info("Volume '%s' already exists", volume_name)
        return True

    brick_list = [f"{worker.ip_address}:{brick_path}" for worker in workers]
    create_cmd = Gluster.volume_create_cmd(
        gluster_cmd, volume_name, replica_count, brick_list, force=True
    )
    create_output = pct_exec(
        proxmox_host,
        manager.container_id,
        create_cmd,
        capture_output=True,
        cfg=cfg,
    )
    create_result = CommandWrapper.parse_result(create_output)
    logger.info("%s", create_output)

    if not (
        create_result.success
        or "created" in (create_output or "").lower()
        or "success" in (create_output or "").lower()
    ):
        logger.error(
            "Volume creation failed: %s - %s",
            create_result.error_type.value,
            create_result.error_message,
        )
        return False

    logger.info("Starting volume '%s'...", volume_name)
    start_cmd = Gluster.volume_start_cmd(gluster_cmd, volume_name)
    start_output = pct_exec(
        proxmox_host,
        manager.container_id,
        start_cmd,
        capture_output=True,
        cfg=cfg,
    )
    logger.info("%s", start_output)

    logger.info("Verifying volume status...")
    vol_status_cmd = Gluster.volume_status_cmd(gluster_cmd, volume_name)
    vol_status = pct_exec(
        proxmox_host,
        manager.container_id,
        vol_status_cmd,
        capture_output=True,
        cfg=cfg,
    )
    if vol_status:
        logger.info(vol_status)
    return True


def _mount_gluster_volume(
    manager,
    workers,
    gluster_cfg,
    cfg,
):
    """Mount Gluster volume on manager and worker nodes."""
    nodes = [manager] + workers
    volume_name = gluster_cfg.volume_name
    mount_point = gluster_cfg.mount_point
    proxmox_host = cfg.proxmox_host

    logger.info("Mounting GlusterFS volume on all nodes...")
    for node in nodes:
        logger.info("Mounting on %s...", node.hostname)
        mkdir_result = pct_exec(
            proxmox_host,
            node.container_id,
            f"mkdir -p {mount_point} 2>&1",
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if mkdir_result and "error" in mkdir_result.lower():
            logger.error(
                "Failed to create mount point on %s: %s",
                node.hostname,
                mkdir_result[-300:],
            )
            return False

        fstab_entry = (
            f"{manager.hostname}:/{volume_name} {mount_point} "
            "glusterfs defaults,_netdev 0 0"
        )
        fstab_cmd = " ".join(
            [
                f"grep -q '{mount_point}' /etc/fstab",
                f"|| echo '{fstab_entry}' >> /etc/fstab 2>&1",
            ]
        )
        fstab_result = pct_exec(
            proxmox_host,
            node.container_id,
            fstab_cmd,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if fstab_result and "error" in fstab_result.lower():
            logger.warning(
                "fstab update had issues on %s: %s",
                node.hostname,
                fstab_result[-200:],
            )

        mount_cmd = " ".join(
            [
                f"mount -t glusterfs {manager.hostname}:/{volume_name} {mount_point} 2>&1",
                "||",
                f"mount -t glusterfs {manager.ip_address}:/{volume_name} {mount_point} 2>&1",
            ]
        )
        mount_result = pct_exec(
            proxmox_host,
            node.container_id,
            mount_cmd,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if (
            mount_result
            and "error" in mount_result.lower()
            and "already mounted" not in mount_result.lower()
        ):
            logger.error(
                "Failed to mount GlusterFS on %s: %s",
                node.hostname,
                mount_result[-300:],
            )
            return False

        if not _verify_mount(node, mount_point, cfg):
            return False
    return True


def _verify_mount(node, mount_point, cfg):
    """Verify Gluster mount status on a node."""
    proxmox_host = cfg.proxmox_host
    mount_verify_cmd = " ".join(
        [
            f"mount | grep -q '{mount_point}'",
            "&& mount | grep '{mount_point}' | grep -q gluster",
            "&& echo mounted || echo not_mounted",
        ]
    )
    mount_verify = pct_exec(
        proxmox_host,
        node.container_id,
        mount_verify_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if "mounted" in mount_verify and "not_mounted" not in mount_verify:
        logger.info("%s: Volume mounted successfully", node.hostname)
        return True

    mount_info_cmd = f"mount | grep {mount_point} 2>/dev/null || echo 'NOT_MOUNTED'"
    mount_info = pct_exec(
        proxmox_host,
        node.container_id,
        mount_info_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if "NOT_MOUNTED" in mount_info or not mount_info:
        logger.error("%s: Mount failed - volume not mounted", node.hostname)
        return False
    logger.warning("%s: Mount status unclear - %s", node.hostname, mount_info[:80])
    return True


def _log_gluster_summary(gluster_cfg):
    """Print a concise summary of GlusterFS deployment."""
    logger.info("GlusterFS distributed storage setup complete")
    logger.info("  Volume: %s", gluster_cfg.volume_name)
    logger.info("  Mount point: %s on all nodes", gluster_cfg.mount_point)
    logger.info("  Replication: %sx", gluster_cfg.replica_count)
