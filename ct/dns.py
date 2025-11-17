"""
DNS container type - creates a caching DNS server container
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
    """Create a DNS server container powered by bind9."""
    container_id = setup_container_base(container_cfg, cfg, privileged=True)
    if not container_id:
        logger.error("Failed to create container %s", container_cfg.id)
        return False

    proxmox_host = cfg.proxmox_host

    apt_steps = [
        ("apt update", Apt.update_cmd(quiet=True)),
        ("bind9 installation", Apt.install_cmd(["bind9"])),
    ]
    for description, command in apt_steps:
        if not run_apt_step(
            proxmox_host,
            container_id,
            cfg,
            command,
            description,
            runner=run_apt_with_lock,
            logger=logger,
        ):
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False

    if not _configure_bind_options(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not _enable_bind_service(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    logger.info("DNS container '%s' configured", container_cfg.name)
    return True


def _configure_bind_options(proxmox_host, container_id, cfg):
    forwarders = getattr(cfg.dns, "servers", []) or ["8.8.8.8", "1.1.1.1"]
    forwarder_lines = "\n".join(f"        {server};" for server in forwarders)
    options_content = textwrap.dedent(
        f"""
        options {{
            directory "/var/cache/bind";
            recursion yes;
            allow-query {{ any; }};
            forwarders {{
{forwarder_lines}
            }};
            dnssec-validation auto;
            auth-nxdomain no;
            listen-on {{ any; }};
        }};
        """
    ).strip()
    write_cmd = FileOps.write_cmd("/etc/bind/named.conf.options", options_content)
    return run_pct_command(
        proxmox_host,
        container_id,
        write_cmd,
        cfg,
        "write bind options",
        logger=logger,
    )


def _enable_bind_service(proxmox_host, container_id, cfg):
    start_cmd = SystemCtl.enable_and_start_cmd("bind9")
    if not run_pct_command(
        proxmox_host,
        container_id,
        start_cmd,
        cfg,
        "start bind9 service",
        logger=logger,
    ):
        return False
    status_cmd = SystemCtl.is_active_check_cmd("bind9")
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
    logger.error("bind9 service is not active")
    return False
