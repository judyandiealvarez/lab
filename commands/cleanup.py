"""Cleanup command orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from cli import PCT
from libs import common
from libs.logger import get_logger

logger = get_logger(__name__)

ssh_exec = common.ssh_exec
destroy_container = common.destroy_container


class CleanupError(RuntimeError):
    """Raised when cleanup fails."""


@dataclass
class CleanupPlan:
    """Holds cleanup context."""

    cfg: object


def run_cleanup(cfg):
    """Remove all containers and templates."""
    logger.info("=" * 50)
    logger.info("Cleaning Up Lab Environment")
    logger.info("=" * 50)
    logger.info("\nDestroying ALL containers and templates...")

    plan = CleanupPlan(cfg)
    _destroy_containers(plan)
    _remove_templates(plan)


def _destroy_containers(plan: CleanupPlan):
    cfg = plan.cfg
    logger.info("\nStopping and destroying containers...")
    container_ids = _list_container_ids(cfg)
    total = len(container_ids)
    if total == 0:
        logger.info("No containers found")
        return

    logger.info("Found %s containers to destroy: %s", total, ", ".join(container_ids))

    for idx, cid in enumerate(container_ids, 1):
        logger.info("\n[%s/%s] Processing container %s...", idx, total, cid)
        destroy_container(cfg.proxmox_host, cid, cfg=cfg)

    _verify_containers_removed(cfg)


def _list_container_ids(cfg) -> List[str]:
    list_cmd = PCT.status_cmd()
    result = ssh_exec(
        cfg.proxmox_host,
        list_cmd,
        check=False,
        capture_output=True,
        timeout=30,
        cfg=cfg,
    )
    container_ids: List[str] = []
    if result:
        lines = result.strip().split("\n")
        for line in lines[1:]:
            parts = line.split()
            if parts and parts[0].isdigit():
                container_ids.append(parts[0])
    return container_ids


def _verify_containers_removed(cfg):
    logger.info("\nVerifying all containers are destroyed...")
    remaining_result = ssh_exec(
        cfg.proxmox_host,
        PCT.status_cmd(),
        check=False,
        capture_output=True,
        timeout=30,
        cfg=cfg,
    )
    remaining_ids: List[str] = []
    if remaining_result:
        remaining_lines = remaining_result.strip().split("\n")
        for line in remaining_lines[1:]:
            parts = line.split()
            if parts and parts[0].isdigit():
                remaining_ids.append(parts[0])

    if remaining_ids:
        raise CleanupError(
            f"{len(remaining_ids)} containers still exist: {', '.join(remaining_ids)}"
        )
    logger.info("All containers destroyed")


def _remove_templates(plan: CleanupPlan):
    cfg = plan.cfg
    logger.info("\nRemoving templates...")
    template_dir = cfg.proxmox_template_dir
    logger.info("Cleaning template directory %s...", template_dir)
    count_result = ssh_exec(
        cfg.proxmox_host,
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -print | wc -l",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    template_count = count_result.strip() if count_result else "0"
    logger.info("Removing %s template files...", template_count)
    ssh_exec(
        cfg.proxmox_host,
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -delete || true",
        check=False,
        cfg=cfg,
    )
    logger.info("Templates removed")
