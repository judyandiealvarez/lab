"""
PostgreSQL container type - creates a PostgreSQL database container
"""

# pylint: disable=duplicate-code

import logging
import time

from libs import common, container
from libs.config import ContainerConfig, LabConfig
from cli import Apt, FileOps, Sed, SystemCtl
from ct.apt_cache import run_apt_command as run_apt_with_lock
from ct.helpers import run_apt_step, run_pct_command

logger = logging.getLogger(__name__)

setup_container_base = container.setup_container_base
destroy_container = common.destroy_container
pct_exec = common.pct_exec


def create_container(container_cfg: ContainerConfig, cfg: LabConfig):
    """Create PostgreSQL container - method for type 'pgsql'."""
    container_id = setup_container_base(container_cfg, cfg, privileged=False)
    if not container_id:
        logger.error("Failed to create container %s", container_cfg.id)
        return False

    proxmox_host = cfg.proxmox_host
    params = container_cfg.params
    version = str(params.get("version", "17"))
    port = params.get("port", 5432)
    allow_cidr = params.get("cidr", "10.11.3.0/24")
    password = params.get("password", "postgres")

    apt_steps = [
        ("apt update", Apt.update_cmd(quiet=True)),
        ("distribution upgrade", Apt.upgrade_cmd(dist_upgrade=True)),
        (
            f"PostgreSQL {version} installation",
            Apt.install_cmd([f"postgresql-{version}", "postgresql-contrib"]),
        ),
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

    if not _configure_postgres_service(proxmox_host, container_id, cfg):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not _configure_postgres_files(
        proxmox_host, container_id, cfg, version, port, allow_cidr
    ):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    if not _set_postgres_password(
        proxmox_host, container_id, cfg, password
    ):
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False

    logger.info(
        "PostgreSQL container '%s' configured on port %s",
        container_cfg.name,
        port,
    )
    return True


def _configure_postgres_service(proxmox_host, container_id, cfg):
    start_cmd = SystemCtl.enable_and_start_cmd("postgresql")
    if not run_pct_command(
        proxmox_host,
        container_id,
        start_cmd,
        cfg,
        "start postgresql service",
        logger=logger,
    ):
        return False
    time.sleep(cfg.waits.service_start)
    is_active_cmd = SystemCtl.is_active_check_cmd("postgresql")
    status = pct_exec(
        proxmox_host,
        container_id,
        is_active_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if SystemCtl.parse_is_active(status):
        return True
    logger.error("PostgreSQL service is not active")
    return False


def _configure_postgres_files(  # pylint: disable=too-many-arguments
    proxmox_host,
    container_id,
    cfg,
    version,
    port,
    allow_cidr,
):
    config_path = f"/etc/postgresql/{version}/main/postgresql.conf"
    listen_cmd = Sed.replace_cmd(
        config_path,
        "^#?listen_addresses.*",
        "listen_addresses = '*'",
        flags="",
    )
    port_cmd = Sed.replace_cmd(
        config_path,
        "^#?port.*",
        f"port = {port}",
        flags="",
    )
    pg_hba_cmd = FileOps.write_cmd(
        f"/etc/postgresql/{version}/main/pg_hba.conf",
        f"host all all {allow_cidr} md5\n",
        append=True,
    )
    return all(
        [
            run_pct_command(
                proxmox_host,
                container_id,
                listen_cmd,
                cfg,
                "configure listen_addresses",
                warn_only=True,
                logger=logger,
            ),
            run_pct_command(
                proxmox_host,
                container_id,
                port_cmd,
                cfg,
                "configure postgres port",
                warn_only=True,
                logger=logger,
            ),
            run_pct_command(
                proxmox_host,
                container_id,
                pg_hba_cmd,
                cfg,
                "append pg_hba rule",
                warn_only=True,
                logger=logger,
            ),
        ]
    )


def _set_postgres_password(proxmox_host, container_id, cfg, password):
    command = (
        "sudo -u postgres psql -c "
        f"\"ALTER USER postgres WITH PASSWORD '{password}';\" 2>&1 || true"
    )
    return run_pct_command(
        proxmox_host,
        container_id,
        command,
        cfg,
        "set postgres password",
        warn_only=True,
        logger=logger,
    )
