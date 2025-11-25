"""Cleanup command orchestration."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List
from cli import PCT, Find
from libs import common
from libs.logger import get_logger
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)
destroy_container = common.destroy_container

class CleanupError(RuntimeError):
    """Raised when cleanup fails."""
@dataclass

class CleanupPlan:
    """Holds cleanup context."""
    cfg: object
    lxc_service: LXCService = None
    pct_service: PCTService = None

def run_cleanup(cfg):
    """Remove all containers and templates."""
    logger.info("=" * 50)
    logger.info("Cleaning Up Lab Environment")
    logger.info("=" * 50)
    logger.info("\nDestroying ALL containers and templates...")
    # Create LXC service connection
    lxc_service = LXCService(cfg.proxmox_host, cfg.ssh)
    if not lxc_service.connect():
        logger.error("Failed to connect to Proxmox host %s", cfg.proxmox_host)
        raise CleanupError("Failed to connect to Proxmox host")
    pct_service = PCTService(lxc_service)
    plan = CleanupPlan(cfg=cfg, lxc_service=lxc_service, pct_service=pct_service)
    try:
        _destroy_containers(plan)
        _remove_templates(plan)
    finally:
        if lxc_service:
            lxc_service.disconnect()

def _destroy_containers(plan: CleanupPlan):
    cfg = plan.cfg
    logger.info("\nStopping and destroying containers...")
    container_ids = _list_container_ids(plan)
    total = len(container_ids)
    if total == 0:
        logger.info("No containers found")
        return
    logger.info("Found %s containers to destroy: %s", total, ", ".join(container_ids))
    for idx, cid in enumerate(container_ids, 1):
        logger.info("\n[%s/%s] Processing container %s...", idx, total, cid)
        destroy_container(cfg.proxmox_host, cid, cfg=cfg, lxc_service=plan.lxc_service)
    _verify_containers_removed(plan)

def _list_container_ids(plan: CleanupPlan) -> List[str]:
    list_cmd = PCT().status()
    result, exit_code = plan.lxc_service.execute(list_cmd)
    container_ids: List[str] = []
    if result:
        lines = result.strip().split("\n")
        for line in lines[1:]:
            parts = line.split()
            if parts and parts[0].isdigit():
                container_ids.append(parts[0])
    return container_ids

def _verify_containers_removed(plan: CleanupPlan):
    logger.info("\nVerifying all containers are destroyed...")
    remaining_result, exit_code = plan.lxc_service.execute(PCT().status())
    remaining_ids: List[str] = []
    if remaining_result:
        remaining_lines = remaining_result.strip().split("\n")
        for line in remaining_lines[1:]:
            parts = line.split()
            if parts and parts[0].isdigit():
                remaining_ids.append(parts[0])
    if remaining_ids:
        raise CleanupError(f"{len(remaining_ids)} containers still exist: {', '.join(remaining_ids)}")
    logger.info("All containers destroyed")

def _remove_templates(plan: CleanupPlan):
    cfg = plan.cfg
    logger.info("\nRemoving templates...")
    template_dir = cfg.proxmox_template_dir
    logger.info("Cleaning template directory %s...", template_dir)
    count_cmd = Find().directory(template_dir).maxdepth(1).type("f").name("*.tar.zst").count()
    count_result, exit_code = plan.lxc_service.execute(count_cmd)
    template_count = count_result.strip() if count_result else "0"
    logger.info("Removing %s template files...", template_count)
    delete_cmd = Find().directory(template_dir).maxdepth(1).type("f").name("*.tar.zst").delete()
    plan.lxc_service.execute(delete_cmd)
    logger.info("Templates removed")