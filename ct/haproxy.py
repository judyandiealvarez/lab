"""
HAProxy container type - creates an HAProxy load balancer container
"""

# pylint: disable=duplicate-code

import logging
import textwrap

from libs import common, container
from libs.config import ContainerConfig, LabConfig
from cli import Apt, FileOps, SystemCtl
from ct.apt_cache import run_apt_command as run_apt_with_lock
from ct.helpers import run_apt_step, run_pct_command

logger = logging.getLogger(__name__)

setup_container_base = container.setup_container_base
destroy_container = common.destroy_container
pct_exec = common.pct_exec


def create_container(container_cfg: ContainerConfig, cfg: LabConfig):
    """Create HAProxy load balancer container."""
    container_id = setup_container_base(container_cfg, cfg, privileged=True)
    if not container_id:
        logger.error("Failed to create container %s", container_cfg.id)
        return False

    proxmox_host = cfg.proxmox_host
    params = container_cfg.params
    http_port = params.get("http_port", 80)
    https_port = params.get("https_port", 443)
    stats_port = params.get("stats_port", 8404)

    if not run_apt_step(
        proxmox_host,
        container_id,
        cfg,
        Apt.update_cmd(quiet=True),
        "apt update",
        runner=run_apt_with_lock,
        logger=logger,
    ):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    if not run_apt_step(
        proxmox_host,
        container_id,
        cfg,
        Apt.install_cmd(["haproxy"]),
        "haproxy installation",
        runner=run_apt_with_lock,
        logger=logger,
    ):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    config_text = _render_haproxy_config(cfg, http_port, https_port, stats_port)
    write_cmd = FileOps.write_cmd("/etc/haproxy/haproxy.cfg", config_text)
    if not run_pct_command(
        proxmox_host,
        container_id,
        write_cmd,
        cfg,
        "write haproxy configuration",
        logger=logger,
    ):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not _configure_haproxy_systemd(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not _enable_haproxy_service(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    logger.info(
        "HAProxy container '%s' configured (HTTP %s, HTTPS %s, stats %s)",
        container_cfg.name,
        http_port,
        https_port,
        stats_port,
    )
    return True


def _render_haproxy_config(cfg, http_port, https_port, stats_port):
    backend_servers = _build_backend_servers(cfg)
    servers_text = "\n".join(backend_servers) or "    server dummy 127.0.0.1:80 check"
    return textwrap.dedent(
        f"""
        global
            log /dev/log local0
            log /dev/log local1 notice
            maxconn 2048
            daemon

        defaults
            log     global
            mode    http
            option  httplog
            option  dontlognull
            timeout connect 5s
            timeout client  50s
            timeout server  50s

        frontend http-in
            bind *:{http_port}
            default_backend nodes

        frontend https-in
            bind *:{https_port}
            mode http
            default_backend nodes

        backend nodes
{servers_text}

        listen stats
            bind *:{stats_port}
            mode http
            stats enable
            stats uri /
            stats refresh 10s
        """
    ).strip() + "\n"


def _build_backend_servers(cfg):
    servers = []
    swarm_nodes = cfg.swarm_managers + cfg.swarm_workers
    for index, node in enumerate(swarm_nodes, start=1):
        servers.append(f"    server node{index} {node.ip_address}:80 check")
    return servers


def _configure_haproxy_systemd(proxmox_host, container_id, cfg):
    """Configure systemd override for HAProxy to work in container."""
    mkdir_cmd = FileOps.mkdir_cmd(
        "/etc/systemd/system/haproxy.service.d", parents=True
    )
    if not run_pct_command(
        proxmox_host,
        container_id,
        mkdir_cmd,
        cfg,
        "create haproxy systemd override directory",
        logger=logger,
    ):
        return False
    override_content = "[Service]\nPrivateNetwork=no\nProtectSystem=no\nProtectHome=no\n"
    override_cmd = FileOps.write_cmd(
        "/etc/systemd/system/haproxy.service.d/override.conf",
        override_content,
    )
    if not run_pct_command(
        proxmox_host,
        container_id,
        override_cmd,
        cfg,
        "write haproxy systemd override",
        logger=logger,
    ):
        return False
    reload_cmd = "systemctl daemon-reload"
    if not run_pct_command(
        proxmox_host,
        container_id,
        reload_cmd,
        cfg,
        "reload systemd daemon",
        logger=logger,
    ):
        return False
    return True


def _enable_haproxy_service(proxmox_host, container_id, cfg):
    # Validate config before starting
    validate_cmd = "haproxy -c -f /etc/haproxy/haproxy.cfg 2>&1"
    validate_output = pct_exec(
        proxmox_host,
        container_id,
        validate_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    # pct_exec returns string when capture_output=True, None on error
    if validate_output is None:
        logger.error("HAProxy config validation command failed")
        return False
    if isinstance(validate_output, str) and (
        "Fatal errors found" in validate_output or "[ALERT]" in validate_output
    ):
        logger.error("HAProxy config validation failed: %s", validate_output)
        return False
    start_cmd = SystemCtl.enable_and_start_cmd("haproxy")
    if not run_pct_command(
        proxmox_host,
        container_id,
        start_cmd,
        cfg,
        "start haproxy service",
        logger=logger,
    ):
        # Get detailed error from systemctl
        status_cmd = "systemctl status haproxy.service --no-pager -l"
        status_output = pct_exec(
            proxmox_host,
            container_id,
            status_cmd,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        # pct_exec returns string when capture_output=True
        status_text = status_output if isinstance(status_output, str) else str(status_output)
        logger.error("HAProxy service start failed. Status: %s", status_text)
        return False
    status_cmd = SystemCtl.is_active_check_cmd("haproxy")
    status = pct_exec(
        proxmox_host,
        container_id,
        status_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if SystemCtl.parse_is_active(status):
        return True
    logger.error("HAProxy service is not active")
    return False
