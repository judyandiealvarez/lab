"""Deployment command orchestration."""
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import List, Optional
from libs import common
from libs.logger import get_logger
from libs.container_manager import create_container
from tmpl import load_template_handler
from orchestration import deploy_swarm, setup_glusterfs
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)

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
    start_step: int = 1
    end_step: Optional[int] = None
    current_action_step: int = 1

def run_deploy(cfg, start_step: int = 1, end_step: Optional[int] = None):
    """Execute the full deployment workflow."""
    logger.info("=" * 50)
    logger.info("Deploying Lab Environment")
    logger.info("=" * 50)
    plan = _build_plan(cfg, start_step, end_step)
    if plan.apt_cache_container:
        _create_apt_cache(plan)
    _create_templates(plan)
    _create_non_swarm_containers(plan)
    _deploy_swarm_stage(plan)
    _setup_gluster_stage(plan)
    failed_ports = _check_service_ports(cfg)
    _log_deploy_summary(cfg, failed_ports)
    if failed_ports:
        error_msg = "Deployment failed: The following ports are not responding:\n"
        for name, ip, port in failed_ports:
            error_msg += f"  - {name}: {ip}:{port}\n"
        raise DeployError(error_msg)

def _count_actions(container_cfg) -> int:
    """Count actions for a container."""
    return len(container_cfg.actions) if container_cfg.actions else 0

def _build_plan(cfg, start_step: int = 1, end_step: Optional[int] = None) -> DeploymentPlan:
    containers = cfg.containers
    apt_cache_container = next((c for c in containers if c.name == cfg.apt_cache_ct), None)
    templates = list(cfg.templates)
    non_swarm = [c for c in containers if c.type not in ("swarm-manager", "swarm-node")]
    if apt_cache_container:
        non_swarm = [c for c in non_swarm if c.name != cfg.apt_cache_ct]
    # Count total steps: 1 per container/template for creation + actions
    total_steps = 0
    if apt_cache_container:
        total_steps += 1  # Container creation step
        total_steps += _count_actions(apt_cache_container)
    for template in templates:
        total_steps += 1  # Template creation step
        total_steps += _count_actions(template)
    for container in non_swarm:
        total_steps += 1  # Container creation step
        total_steps += _count_actions(container)
    # Swarm containers also have creation + actions
    swarm_containers = [c for c in containers if c.type in ("swarm-manager", "swarm-node")]
    for container in swarm_containers:
        total_steps += 1  # Container creation step
        total_steps += _count_actions(container)
    if not apt_cache_container:
        raise DeployError(f"apt-cache container '{cfg.apt_cache_ct}' not found in configuration")
    if end_step is None:
        end_step = total_steps
    return DeploymentPlan(
        cfg=cfg,
        apt_cache_container=apt_cache_container,
        templates=templates,
        non_swarm_containers=non_swarm,
        total_steps=total_steps,
        start_step=start_step,
        end_step=end_step,
        current_action_step=start_step - 1,
    )

def _create_apt_cache(plan: DeploymentPlan):
    container_cfg = plan.apt_cache_container
    logger.info("\n[%s/%s] Creating apt-cache container first...", plan.step, plan.total_steps)
    _create_container_with_base_template(container_cfg, plan)
    _wait_for_apt_cache_ready(plan.cfg, container_cfg)
    plan.step += 1

def _create_container_with_base_template(container_cfg, plan: DeploymentPlan):
    original_template = container_cfg.template
    container_cfg.template = None
    try:
        # Use common container manager which handles installation and configuration via actions
        created = create_container(container_cfg, plan.cfg, plan=plan)
    finally:
        if original_template is not None:
            container_cfg.template = original_template
    if not created:
        logger.error("=" * 50)
        logger.error("Apt-Cache Container Creation Failed")
        logger.error("=" * 50)
        logger.error("Container: %s", container_cfg.name)
        logger.error("Step: %d", plan.current_action_step)
        logger.error("Error: Failed to create apt-cache container")
        logger.error("=" * 50)
        raise DeployError("Failed to create apt-cache container")

def _wait_for_apt_cache_ready(cfg, container_cfg):
    logger.info("Verifying apt-cache service is ready...")
    max_attempts = 20
    proxmox_host = cfg.proxmox_host
    container_id = container_cfg.id
    apt_cache_port = cfg.apt_cache_port
    for attempt in range(1, max_attempts + 1):
        service_check = pct_exec(
            proxmox_host,
            container_id,
            "systemctl is-active apt-cacher-ng 2>/dev/null || echo 'inactive'",
            check=False,
            timeout=10,
            cfg=cfg,
        )
        if service_check and "active" in service_check:
            port_check_cmd = (
                f"nc -z localhost {apt_cache_port} 2>/dev/null " "&& echo 'port_open' || echo 'port_closed'"
            )
            port_check = pct_exec(
                proxmox_host,
                container_id,
                port_check_cmd,
                check=False,
                timeout=10,
                cfg=cfg,
            )
            if port_check and "port_open" in port_check:
                # Test if apt-cacher-ng can actually fetch from upstream
                test_cmd = (
                    f"timeout 10 wget -qO- 'http://127.0.0.1:{apt_cache_port}/acng-report.html' 2>&1 | "
                    "grep -q 'Apt-Cacher NG' && echo 'working' || echo 'not_working'"
                )
                functionality_test = pct_exec(
                    proxmox_host,
                    container_id,
                    test_cmd,
                    check=False,
                    timeout=15,
                    cfg=cfg,
                )
                if functionality_test and "working" in functionality_test:
                    logger.info("apt-cache service is ready on %s:%s", container_cfg.ip_address, apt_cache_port)
                    return
                elif attempt < max_attempts:
                    logger.debug("apt-cache service not fully ready yet (attempt %s/%s), waiting...", attempt, max_attempts)
                    time.sleep(2)
                    continue
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
                timeout=10,
                cfg=cfg,
            )
            journal_cmd = "journalctl -u apt-cacher-ng --no-pager -n 30 2>&1"
            journal_output = pct_exec(
                proxmox_host,
                container_id,
                journal_cmd,
                check=False,
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
        if not create_template_fn or not create_template_fn(template_cfg, plan.cfg, plan=plan):
            logger.error("=" * 50)
            logger.error("Template Creation Failed")
            logger.error("=" * 50)
            logger.error("Container: %s", template_cfg.name)
            logger.error("Step: %d", plan.current_action_step)
            logger.error("Error: Failed to create template '%s'", template_cfg.name)
            logger.error("=" * 50)
            raise DeployError(f"Failed to create template '{template_cfg.name}'")
        plan.step += 1

def _create_non_swarm_containers(plan: DeploymentPlan):
    for container_cfg in plan.non_swarm_containers:
        # Use common container manager for all container types
        if not create_container(container_cfg, plan.cfg, plan=plan):
            logger.error("=" * 50)
            logger.error("Container Creation Failed")
            logger.error("=" * 50)
            logger.error("Container: %s", container_cfg.name)
            logger.error("Step: %d", plan.current_action_step)
            logger.error("Error: Failed to create container '%s'", container_cfg.name)
            logger.error("=" * 50)
            raise DeployError(f"Failed to create container '{container_cfg.name}'")
        plan.step += 1

def _deploy_swarm_stage(plan: DeploymentPlan):
    logger.info("\n[%s/%s] Deploying Docker Swarm...", plan.step, plan.total_steps)
    if not deploy_swarm(plan.cfg):
        raise DeployError("Docker Swarm deployment failed")
    plan.step += 1

def _setup_gluster_stage(plan: DeploymentPlan):
    logger.info("\n[%s/%s] Setting up GlusterFS distributed storage...", plan.step, plan.total_steps)
    if not setup_glusterfs(plan.cfg):
        raise DeployError("GlusterFS setup failed")

def _check_service_ports(cfg):
    """Check if all service ports are responding"""
    logger.info("\nChecking service ports...")
    import time
    from libs.common import ssh_exec
    # Wait a bit for services to fully start
    time.sleep(5)
    failed_ports = []
    # Check apt-cache
    apt_cache_ct = next((c for c in cfg.containers if c.name == cfg.apt_cache_ct), None)
    if apt_cache_ct:
        port = cfg.services.apt_cache.port or 3142
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {apt_cache_ct.ip_address} {port} 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ apt-cache: %s:%s", apt_cache_ct.ip_address, port)
        else:
            logger.error("  ✗ apt-cache: %s:%s - NOT RESPONDING", apt_cache_ct.ip_address, port)
            failed_ports.append(("apt-cache", apt_cache_ct.ip_address, port))
    # Check PostgreSQL
    pgsql_ct = next((c for c in cfg.containers if c.type == "pgsql"), None)
    if pgsql_ct:
        port = cfg.services.postgresql.port if cfg.services.postgresql else 5432
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {pgsql_ct.ip_address} {port} 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ PostgreSQL: %s:%s", pgsql_ct.ip_address, port)
        else:
            logger.error("  ✗ PostgreSQL: %s:%s - NOT RESPONDING", pgsql_ct.ip_address, port)
            failed_ports.append(("PostgreSQL", pgsql_ct.ip_address, port))
    # Check HAProxy
    haproxy_ct = next((c for c in cfg.containers if c.type == "haproxy"), None)
    if haproxy_ct:
        http_port = cfg.services.haproxy.http_port if cfg.services.haproxy else 80
        stats_port = cfg.services.haproxy.stats_port if cfg.services.haproxy else 8404
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {haproxy_ct.ip_address} {http_port} 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ HAProxy HTTP: %s:%s", haproxy_ct.ip_address, http_port)
        else:
            logger.error("  ✗ HAProxy HTTP: %s:%s - NOT RESPONDING", haproxy_ct.ip_address, http_port)
            failed_ports.append(("HAProxy HTTP", haproxy_ct.ip_address, http_port))
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {haproxy_ct.ip_address} {stats_port} 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ HAProxy Stats: %s:%s", haproxy_ct.ip_address, stats_port)
        else:
            logger.error("  ✗ HAProxy Stats: %s:%s - NOT RESPONDING", haproxy_ct.ip_address, stats_port)
            failed_ports.append(("HAProxy Stats", haproxy_ct.ip_address, stats_port))
    # Check DNS (both TCP and UDP)
    dns_ct = next((c for c in cfg.containers if c.type == "dns"), None)
    if dns_ct:
        port = dns_ct.params.get("dns_port", 53)
        result_tcp = ssh_exec(cfg.proxmox_host, f"nc -zv {dns_ct.ip_address} {port} 2>&1", check=False, cfg=cfg)
        result_udp = ssh_exec(cfg.proxmox_host, f"nc -zuv {dns_ct.ip_address} {port} 2>&1", check=False, cfg=cfg)
        if (result_tcp and ("open" in result_tcp.lower() or "succeeded" in result_tcp.lower())) or \
           (result_udp and ("open" in result_udp.lower() or "succeeded" in result_udp.lower())):
            logger.info("  ✓ DNS: %s:%s", dns_ct.ip_address, port)
        else:
            logger.error("  ✗ DNS: %s:%s - NOT RESPONDING", dns_ct.ip_address, port)
            failed_ports.append(("DNS", dns_ct.ip_address, port))
    # Check Docker Swarm
    swarm_manager = next((c for c in cfg.containers if c.type == "swarm-manager"), None)
    if swarm_manager:
        port = cfg.services.docker_swarm.port or 2377
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {swarm_manager.ip_address} {port} 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ Docker Swarm: %s:%s", swarm_manager.ip_address, port)
        else:
            logger.error("  ✗ Docker Swarm: %s:%s - NOT RESPONDING", swarm_manager.ip_address, port)
            failed_ports.append(("Docker Swarm", swarm_manager.ip_address, port))
    # Check Portainer
    if swarm_manager and cfg.services.portainer:
        port = cfg.services.portainer.port or 9443
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {swarm_manager.ip_address} {port} 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ Portainer: %s:%s", swarm_manager.ip_address, port)
        else:
            logger.error("  ✗ Portainer: %s:%s - NOT RESPONDING", swarm_manager.ip_address, port)
            failed_ports.append(("Portainer", swarm_manager.ip_address, port))
    # Check GlusterFS
    if swarm_manager and cfg.glusterfs:
        result = ssh_exec(cfg.proxmox_host, f"nc -zv {swarm_manager.ip_address} 24007 2>&1", check=False, cfg=cfg)
        if result and ("open" in result.lower() or "succeeded" in result.lower()):
            logger.info("  ✓ GlusterFS: %s:24007", swarm_manager.ip_address)
        else:
            logger.error("  ✗ GlusterFS: %s:24007 - NOT RESPONDING", swarm_manager.ip_address)
            failed_ports.append(("GlusterFS", swarm_manager.ip_address, 24007))
    return failed_ports

def _log_deploy_summary(cfg, failed_ports=None):
    logger.info("\n%s", "=" * 50)
    if failed_ports:
        logger.info("Deployment Complete (with port failures)")
    else:
        logger.info("Deployment Complete!")
    logger.info("%s", "=" * 50)
    logger.info("\nContainers:")
    for ct in cfg.containers:
        logger.info("  - %s: %s (%s)", ct.id, ct.name, ct.ip_address)
    manager_configs = [c for c in cfg.containers if c.type == "swarm-manager"]
    if manager_configs:
        manager = manager_configs[0]
        logger.info("\nPortainer: https://%s:%s", manager.ip_address, cfg.portainer_port)
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
    if failed_ports:
        logger.info("\n⚠ Port Status:")
        logger.info("  The following ports are NOT responding:")
        for name, ip, port in failed_ports:
            logger.info("    ✗ %s: %s:%s", name, ip, port)
    else:
        logger.info("\n✓ All service ports are responding")