"""Deployment command orchestration."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import List, Optional

from libs import common
from libs.logger import get_logger
from libs.container import setup_container_base
from ct import apt_cache
from tmpl import load_template_handler
from orchestration import deploy_swarm, setup_glusterfs

logger = get_logger(__name__)

pct_exec = common.pct_exec


class DeployError(RuntimeError):
    """Raised when deployment fails."""


@dataclass
class DeploymentPlan:
    """Holds deployment sequencing information."""

    cfg: object
    apt_cache_container: Optional[object]
    templates: List[object]
    non_swarm_containers: List[object]
    total_steps: int
    step: int = 1


def run_deploy(cfg):
    """Execute the full deployment workflow."""
    logger.info("=" * 50)
    logger.info("Deploying Lab Environment")
    logger.info("=" * 50)

    plan = _build_plan(cfg)

    if plan.apt_cache_container:
        _create_apt_cache(plan)

    _create_templates(plan)
    _create_non_swarm_containers(plan)
    _deploy_swarm_stage(plan)
    _setup_gluster_stage(plan)
    _log_deploy_summary(cfg)


def _build_plan(cfg) -> DeploymentPlan:
    containers = cfg.containers
    apt_cache_container = next(
        (c for c in containers if c.name == cfg.apt_cache_ct), None
    )
    templates = list(cfg.templates)
    non_swarm = [
        c for c in containers if c.type not in ("swarm-manager", "swarm-node")
    ]
    if apt_cache_container:
        non_swarm = [c for c in non_swarm if c.name != cfg.apt_cache_ct]
    total_steps = (
        (1 if apt_cache_container else 0)
        + len(templates)
        + len(non_swarm)
        + 2
    )
    if not apt_cache_container:
        raise DeployError(
            f"apt-cache container '{cfg.apt_cache_ct}' not found in configuration"
        )
    return DeploymentPlan(
        cfg=cfg,
        apt_cache_container=apt_cache_container,
        templates=templates,
        non_swarm_containers=non_swarm,
        total_steps=total_steps,
    )


def _create_apt_cache(plan: DeploymentPlan):
    container_cfg = plan.apt_cache_container
    logger.info(
        "\n[%s/%s] Creating apt-cache container first...",
        plan.step,
        plan.total_steps,
    )
    _create_container_with_base_template(container_cfg, plan)
    _wait_for_apt_cache_ready(plan.cfg, container_cfg)
    plan.step += 1


def _create_container_with_base_template(container_cfg, plan: DeploymentPlan):
    original_template = container_cfg.template
    container_cfg.template = None
    try:
        # Use apt-cache specific create_container which handles installation and configuration
        created = apt_cache.create_container(container_cfg, plan.cfg)
    finally:
        if original_template is not None:
            container_cfg.template = original_template
    if not created:
        raise DeployError("Failed to create apt-cache container")


def _wait_for_apt_cache_ready(cfg, container_cfg):
    logger.info("Verifying apt-cache service is ready...")
    max_attempts = 10
    proxmox_host = cfg.proxmox_host
    container_id = container_cfg.id
    apt_cache_port = cfg.apt_cache_port

    for attempt in range(1, max_attempts + 1):
        service_check = pct_exec(
            proxmox_host,
            container_id,
            "systemctl is-active apt-cacher-ng 2>/dev/null || echo 'inactive'",
            check=False,
            capture_output=True,
            timeout=10,
            cfg=cfg,
        )
        if service_check and "active" in service_check:
            port_check_cmd = (
                f"nc -z localhost {apt_cache_port} 2>/dev/null "
                "&& echo 'port_open' || echo 'port_closed'"
            )
            port_check = pct_exec(
                proxmox_host,
                container_id,
                port_check_cmd,
                check=False,
                capture_output=True,
                timeout=10,
                cfg=cfg,
            )
            if port_check and "port_open" in port_check:
                logger.info(
                    "apt-cache service is ready on %s:%s",
                    container_cfg.ip_address,
                    apt_cache_port,
                )
                return
        else:
            # Service is not active - try to start it and check logs
            if attempt == 1:
                # On first attempt, try to start the service
                start_cmd = f"systemctl start apt-cacher-ng 2>&1"
                start_output = pct_exec(
                    proxmox_host,
                    container_id,
                    start_cmd,
                    check=False,
                    capture_output=True,
                    timeout=10,
                    cfg=cfg,
                )
                if start_output:
                    logger.info("Service start attempt output: %s", start_output)
                # Check service status for errors
                status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1 | head -15"
                status_output = pct_exec(
                    proxmox_host,
                    container_id,
                    status_cmd,
                    check=False,
                    capture_output=True,
                    timeout=10,
                    cfg=cfg,
                )
                if status_output:
                    logger.warning("Service status: %s", status_output)
        if attempt < max_attempts:
            logger.info("Waiting for apt-cache service... (%s/%s)", attempt, max_attempts)
            time.sleep(3)
        else:
            # Get final status and logs before failing
            status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1"
            status_output = pct_exec(
                proxmox_host,
                container_id,
                status_cmd,
                check=False,
                capture_output=True,
                timeout=10,
                cfg=cfg,
            )
            journal_cmd = "journalctl -u apt-cacher-ng --no-pager -n 30 2>&1"
            journal_output = pct_exec(
                proxmox_host,
                container_id,
                journal_cmd,
                check=False,
                capture_output=True,
                timeout=10,
                cfg=cfg,
            )
            error_msg = f"apt-cache service did not become ready in time"
            if status_output:
                error_msg += f"\nService status: {status_output}"
            if journal_output:
                error_msg += f"\nService logs: {journal_output}"
            raise DeployError(error_msg)


def _create_templates(plan: DeploymentPlan):
    for template_cfg in plan.templates:
        create_template_fn = load_template_handler(template_cfg.type)
        if not create_template_fn or not create_template_fn(template_cfg, plan.cfg):
            raise DeployError(f"Failed to create template '{template_cfg.name}'")
        plan.step += 1


def _create_non_swarm_containers(plan: DeploymentPlan):
    for container_cfg in plan.non_swarm_containers:
        # Import container-specific create_container function based on type
        if container_cfg.type == "apt-cache":
            from ct import apt_cache
            create_fn = apt_cache.create_container
        elif container_cfg.type == "dns":
            from ct import dns
            create_fn = dns.create_container
        elif container_cfg.type == "haproxy":
            from ct import haproxy
            create_fn = haproxy.create_container
        elif container_cfg.type == "pgsql":
            from ct import pgsql
            create_fn = pgsql.create_container
        else:
            # Use generic container setup
            create_fn = setup_container_base
        if not create_fn(container_cfg, plan.cfg):
            raise DeployError(f"Failed to create container '{container_cfg.name}'")
        plan.step += 1


def _deploy_swarm_stage(plan: DeploymentPlan):
    logger.info(
        "\n[%s/%s] Deploying Docker Swarm...", plan.step, plan.total_steps
    )
    if not deploy_swarm(plan.cfg):
        raise DeployError("Docker Swarm deployment failed")
    plan.step += 1


def _setup_gluster_stage(plan: DeploymentPlan):
    logger.info(
        "\n[%s/%s] Setting up GlusterFS distributed storage...",
        plan.step,
        plan.total_steps,
    )
    if not setup_glusterfs(plan.cfg):
        raise DeployError("GlusterFS setup failed")


def _log_deploy_summary(cfg):
    logger.info("\n%s", "=" * 50)
    logger.info("Deployment Complete!")
    logger.info("%s", "=" * 50)
    logger.info("\nContainers:")
    for ct in cfg.containers:
        logger.info("  - %s: %s (%s)", ct.id, ct.name, ct.ip_address)

    manager_configs = [c for c in cfg.containers if c.type == "swarm-manager"]
    if manager_configs:
        manager = manager_configs[0]
        logger.info(
            "\nPortainer: https://%s:%s",
            manager.ip_address,
            cfg.portainer_port,
        )

    pgsql_containers = [c for c in cfg.containers if c.type == "pgsql"]
    if pgsql_containers:
        pgsql = pgsql_containers[0]
        params = pgsql.params
        logger.info("PostgreSQL: %s:%s", pgsql.ip_address, params.get("port", 5432))

    haproxy_containers = [c for c in cfg.containers if c.type == "haproxy"]
    if haproxy_containers:
        haproxy = haproxy_containers[0]
        params = haproxy.params
        logger.info(
            "HAProxy: http://%s:%s (Stats: http://%s:%s)",
            haproxy.ip_address,
            params.get("http_port", 80),
            haproxy.ip_address,
            params.get("stats_port", 8404),
        )

    if cfg.glusterfs:
        gluster_cfg = cfg.glusterfs
        logger.info("\nGlusterFS:")
        logger.info("  Volume: %s", gluster_cfg.volume_name)
        logger.info("  Mount: %s on all nodes", gluster_cfg.mount_point)
