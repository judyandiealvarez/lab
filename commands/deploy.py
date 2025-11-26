"""Deploy command orchestration."""
from __future__ import annotations
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING
from libs import common
from libs.logger import get_logger
from libs.container_manager import create_container
from libs.command import Command
from tmpl import load_template_handler
from orchestration import deploy_swarm, setup_glusterfs
if TYPE_CHECKING:
    from services.lxc import LXCService
    from services.pct import PCTService
logger = get_logger(__name__)

class DeployError(RuntimeError):
    """Raised when deployment fails."""


@dataclass
class Deploy(Command):
    """Holds deployment sequencing information."""
    apt_cache_container: Optional[object] = field(default=None)
    templates: List[object] = field(default_factory=list)
    non_swarm_containers: List[object] = field(default_factory=list)
    total_steps: int = 0
    step: int = 1
    start_step: int = 1
    end_step: Optional[int] = None
    current_action_step: int = 1
    lxc_service: Optional["LXCService"] = field(default=None)
    pct_service: Optional["PCTService"] = field(default=None)

    def run(self, args):
        """Execute the deployment workflow."""
        import traceback
        start_step = getattr(args, 'start_step', 1)
        end_step = getattr(args, 'end_step', None)
        try:
            self.start_step = start_step
            self.end_step = end_step
            self._run_deploy()
        except DeployError as err:
            logger.error("Error during deployment: %s", err)
            logger.error(traceback.format_exc())
            sys.exit(1)

    def _run_deploy(self):
        """Build action list from config and execute them"""
        plan = self._build_plan()
        
        # Report the plan first
        self._log_deploy_plan()
        
        # Execute actions per container (so services are available)
        logger.info("=" * 50)
        logger.info("Executing Deployment")
        logger.info("=" * 50)
        
        # 1. Apt-cache container: create container + its actions
        if plan.apt_cache_container:
            self._execute_container_actions(plan, plan.apt_cache_container, "apt-cache")
            if plan.end_step is not None and plan.current_action_step >= plan.end_step:
                logger.info("Reached end step %d, stopping deployment", plan.end_step)
                failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
                _log_deploy_summary(self.cfg, failed_ports)
                if failed_ports:
                    error_msg = "Deploy failed: The following ports are not responding:\n"
                    for name, ip, port in failed_ports:
                        error_msg += f"  - {name}: {ip}:{port}\n"
                    raise DeployError(error_msg)
                return
        
        # 2. Templates: create container + template's actions
        for template_cfg in plan.templates:
            self._execute_container_actions(plan, template_cfg, template_cfg.name)
            if plan.end_step is not None and plan.current_action_step >= plan.end_step:
                logger.info("Reached end step %d, stopping deployment", plan.end_step)
                failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
                _log_deploy_summary(self.cfg, failed_ports)
                if failed_ports:
                    error_msg = "Deploy failed: The following ports are not responding:\n"
                    for name, ip, port in failed_ports:
                        error_msg += f"  - {name}: {ip}:{port}\n"
                    raise DeployError(error_msg)
                return
        
        # 3. Containers: create container + container's actions
        for container_cfg in plan.non_swarm_containers:
            self._execute_container_actions(plan, container_cfg, container_cfg.name)
            if plan.end_step is not None and plan.current_action_step >= plan.end_step:
                logger.info("Reached end step %d, stopping deployment", plan.end_step)
                failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
                _log_deploy_summary(self.cfg, failed_ports)
                if failed_ports:
                    error_msg = "Deploy failed: The following ports are not responding:\n"
                    for name, ip, port in failed_ports:
                        error_msg += f"  - {name}: {ip}:{port}\n"
                    raise DeployError(error_msg)
                return
        
        # 4. Swarm containers: create container + container's actions
        containers = self.cfg.containers
        swarm_containers = [c for c in containers if c.type in ("swarm-manager", "swarm-node")]
        for container_cfg in swarm_containers:
            self._execute_container_actions(plan, container_cfg, container_cfg.name)
            if plan.end_step is not None and plan.current_action_step >= plan.end_step:
                logger.info("Reached end step %d, stopping deployment", plan.end_step)
                failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
                _log_deploy_summary(self.cfg, failed_ports)
                if failed_ports:
                    error_msg = "Deploy failed: The following ports are not responding:\n"
                    for name, ip, port in failed_ports:
                        error_msg += f"  - {name}: {ip}:{port}\n"
                    raise DeployError(error_msg)
                return
        
        # 5. Setup swarm action if we have swarm containers
        if swarm_containers:
            from actions.setup_swarm import SetupSwarmAction
            if plan:
                plan.current_action_step += 1
                if plan.current_action_step < plan.start_step:
                    logger.info("Skipping setup swarm (step %d < start_step %d)", 
                              plan.current_action_step, plan.start_step)
                elif plan.end_step is not None and plan.current_action_step > plan.end_step:
                    logger.info("Reached end step %d, stopping deployment", plan.end_step)
                else:
                    overall_pct = int((plan.current_action_step / plan.total_steps) * 100)
                    logger.info("=" * 50)
                    logger.info("[Overall: %d%%] [Step: %d/%d] Executing: swarm - setup swarm", 
                              overall_pct, plan.current_action_step, plan.total_steps)
                    logger.info("=" * 50)
                    setup_swarm_action = SetupSwarmAction(
                        ssh_service=None,
                        apt_service=None,
                        pct_service=None,
                        container_id=None,
                        cfg=self.cfg,
                        container_cfg=None,
                    )
                    setup_swarm_action.plan = plan
                    if not setup_swarm_action.execute():
                        raise DeployError("Failed to execute setup swarm action")
            
            # Check if we should stop after swarm
            if plan and plan.end_step is not None and plan.current_action_step >= plan.end_step:
                logger.info("Reached end step %d, stopping deployment", plan.end_step)
                failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
                _log_deploy_summary(self.cfg, failed_ports)
                if failed_ports:
                    error_msg = "Deploy failed: The following ports are not responding:\n"
                    for name, ip, port in failed_ports:
                        error_msg += f"  - {name}: {ip}:{port}\n"
                    raise DeployError(error_msg)
                return
        
        # 6. Setup GlusterFS if configured
        if self.cfg.glusterfs and swarm_containers:
            if plan:
                plan.current_action_step += 1
                if plan.current_action_step < plan.start_step:
                    logger.info("Skipping GlusterFS setup (step %d < start_step %d)", 
                              plan.current_action_step, plan.start_step)
                elif plan.end_step is not None and plan.current_action_step > plan.end_step:
                    logger.info("Reached end step %d, stopping deployment", plan.end_step)
                else:
                    overall_pct = int((plan.current_action_step / plan.total_steps) * 100)
                    logger.info("=" * 50)
                    logger.info("[Overall: %d%%] [Step: %d/%d] Executing: GlusterFS setup", 
                              overall_pct, plan.current_action_step, plan.total_steps)
                    logger.info("=" * 50)
                    if not setup_glusterfs(self.cfg):
                        raise DeployError("GlusterFS setup failed")
        
        # Check service ports
        failed_ports = self._check_service_ports()
        _log_deploy_summary(self.cfg, failed_ports)
        if failed_ports:
            error_msg = "Deploy failed: The following ports are not responding:\n"
            for name, ip, port in failed_ports:
                error_msg += f"  - {name}: {ip}:{port}\n"
            raise DeployError(error_msg)
    
    def _execute_container_actions(self, plan: "Deploy", container_cfg, container_name: str):
        """Execute create container action, then set up services and execute container's actions"""
        from actions.create_container import CreateContainerAction
        from actions.registry import get_action_class
        from services.ssh import SSHService
        from services.apt import APTService
        from services.lxc import LXCService
        from services.pct import PCTService
        from libs.config import SSHConfig
        import time
        
        # 1. Execute create container action
        if plan:
            plan.current_action_step += 1
            if plan.current_action_step < plan.start_step:
                logger.info("Skipping container '%s' creation (step %d < start_step %d)", 
                          container_name, plan.current_action_step, plan.start_step)
                return
            if plan.end_step is not None and plan.current_action_step > plan.end_step:
                logger.info("Reached end step %d, stopping deployment", plan.end_step)
                return
            overall_pct = int((plan.current_action_step / plan.total_steps) * 100)
            logger.info("=" * 50)
            logger.info("[Overall: %d%%] [Step: %d/%d] Executing: %s - create container", 
                      overall_pct, plan.current_action_step, plan.total_steps, container_name)
            logger.info("=" * 50)
        
        create_action = CreateContainerAction(
            ssh_service=None,
            apt_service=None,
            pct_service=None,
            container_id=None,
            cfg=self.cfg,
            container_cfg=container_cfg,
        )
        create_action.plan = plan
        
        try:
            if not create_action.execute():
                raise DeployError(f"Failed to create container: {container_name}")
            logger.info("Container '%s' created successfully", container_name)
        except Exception as exc:
            logger.error("Failed to create container '%s': %s", container_name, exc)
            raise DeployError(f"Failed to create container '{container_name}': {exc}")
        
        # 2. Set up SSH service for this container
        container_id = str(container_cfg.id)
        ip_address = container_cfg.ip_address
        default_user = self.cfg.users.default_user
        
        logger.info("Setting up SSH connection to container %s...", container_name)
        container_ssh_config = SSHConfig(
            connect_timeout=self.cfg.ssh.connect_timeout,
            batch_mode=self.cfg.ssh.batch_mode,
            default_exec_timeout=self.cfg.ssh.default_exec_timeout,
            read_buffer_size=self.cfg.ssh.read_buffer_size,
            poll_interval=self.cfg.ssh.poll_interval,
            default_username=default_user,
            look_for_keys=self.cfg.ssh.look_for_keys,
            allow_agent=self.cfg.ssh.allow_agent,
            verbose=self.cfg.ssh.verbose,
        )
        ssh_service = SSHService(f"{default_user}@{ip_address}", container_ssh_config)
        if not ssh_service.connect():
            raise DeployError(f"Failed to connect to container {container_name} via SSH")
        
        # Wait a moment for SSH to be fully ready
        time.sleep(2)
        
        # 3. Set up APT service
        apt_service = APTService(ssh_service)
        
        # 4. Set up LXC and PCT services
        lxc_service = LXCService(self.cfg.proxmox_host, self.cfg.ssh)
        if not lxc_service.connect():
            ssh_service.disconnect()
            raise DeployError(f"Failed to connect to Proxmox host {self.cfg.proxmox_host}")
        pct_service = PCTService(lxc_service)
        
        try:
            # 5. Execute container's actions with services
            action_names = container_cfg.actions if container_cfg.actions else []
            for action_name in action_names:
                if plan:
                    plan.current_action_step += 1
                    if plan.current_action_step < plan.start_step:
                        continue
                    if plan.end_step is not None and plan.current_action_step > plan.end_step:
                        logger.info("Reached end step %d, stopping action execution", plan.end_step)
                        return
                    overall_pct = int((plan.current_action_step / plan.total_steps) * 100)
                    logger.info("=" * 50)
                    logger.info("[Overall: %d%%] [Step: %d/%d] Executing: %s - %s", 
                              overall_pct, plan.current_action_step, plan.total_steps, container_name, action_name)
                    logger.info("=" * 50)
                
                action_class = get_action_class(action_name)
                action = action_class(
                    ssh_service=ssh_service,
                    apt_service=apt_service,
                    pct_service=pct_service,
                    container_id=container_id,
                    cfg=self.cfg,
                    container_cfg=container_cfg,
                )
                action.plan = plan
                
                try:
                    if not action.execute():
                        raise DeployError(f"Failed to execute action '{action_name}' for container '{container_name}'")
                    logger.info("Action '%s' for container '%s' completed successfully", action_name, container_name)
                except Exception as exc:
                    logger.error("Exception executing action '%s' for container '%s': %s", action_name, container_name, exc)
                    logger.error("Exception details:", exc_info=True)
                    raise DeployError(f"Exception executing action '{action_name}' for container '{container_name}': {exc}")
        finally:
            # Clean up services
            ssh_service.disconnect()
            lxc_service.disconnect()

    def _run_deploy2(self):
        """Execute the full deployment workflow."""
        logger.info("=" * 50)
        logger.info("Deploying Lab Environment")
        logger.info("=" * 50)
        plan = self._build_plan()
        self._log_deploy_plan()

        # 1) Apt-cache container (first stage)
        if plan.apt_cache_container:
            self._create_apt_cache(plan)
            # If we've already reached or passed the requested end_step, stop here
            if plan.end_step is not None and plan.current_action_step >= plan.end_step:
                logger.info(
                    "Reached end_step %d after apt-cache stage, stopping deployment pipeline",
                    plan.end_step,
                )
                failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
                _log_deploy_summary(self.cfg, failed_ports)
                if failed_ports:
                    error_msg = "Deploy failed: The following ports are not responding:\n"
                    for name, ip, port in failed_ports:
                        error_msg += f"  - {name}: {ip}:{port}\n"
                    raise DeployError(error_msg)
                return

        # 2) Templates
        self._create_templates(plan)
        if plan.end_step is not None and plan.current_action_step >= plan.end_step:
            logger.info(
                "Reached end_step %d after template stage, stopping deployment pipeline",
                plan.end_step,
            )
            failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
            _log_deploy_summary(self.cfg, failed_ports)
            if failed_ports:
                error_msg = "Deploy failed: The following ports are not responding:\n"
                for name, ip, port in failed_ports:
                    error_msg += f"  - {name}: {ip}:{port}\n"
                raise DeployError(error_msg)
            return

        # 3) Non-swarm containers
        self._create_non_swarm_containers(plan)
        if plan.end_step is not None and plan.current_action_step >= plan.end_step:
            logger.info(
                "Reached end_step %d after non-swarm stage, stopping deployment pipeline",
                plan.end_step,
            )
            failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
            _log_deploy_summary(self.cfg, failed_ports)
            if failed_ports:
                error_msg = "Deploy failed: The following ports are not responding:\n"
                for name, ip, port in failed_ports:
                    error_msg += f"  - {name}: {ip}:{port}\n"
                raise DeployError(error_msg)
            return

        # 4) Swarm + Gluster stages only run if we haven't hit end_step yet
        self._deploy_swarm_stage(plan)
        if plan.end_step is not None and plan.current_action_step >= plan.end_step:
            logger.info(
                "Reached end_step %d after swarm stage, stopping deployment pipeline",
                plan.end_step,
            )
            failed_ports = self._check_service_ports() if plan.end_step == plan.total_steps else []
            _log_deploy_summary(self.cfg, failed_ports)
            if failed_ports:
                error_msg = "Deploy failed: The following ports are not responding:\n"
                for name, ip, port in failed_ports:
                    error_msg += f"  - {name}: {ip}:{port}\n"
                raise DeployError(error_msg)
            return

        self._setup_gluster_stage(plan)
        failed_ports = self._check_service_ports()
        _log_deploy_summary(self.cfg, failed_ports)
        if failed_ports:
            error_msg = "Deploy failed: The following ports are not responding:\n"
            for name, ip, port in failed_ports:
                error_msg += f"  - {name}: {ip}:{port}\n"
            raise DeployError(error_msg)

    def _count_actions(self, container_cfg) -> int:
        """Count actions for a container."""
        return len(container_cfg.actions) if container_cfg.actions else 0

    def _log_deploy_plan(self):
        """Log a numbered list of all deployment steps, marking which will run."""
        steps: list[tuple[int, str]] = []
        step_num = 1

        # Apt-cache container (first, if present)
        if self.apt_cache_container:
            c = self.apt_cache_container
            steps.append((step_num, f"{c.name}: create container"))
            step_num += 1
            for action in (getattr(c, "actions", None) or []):
                steps.append((step_num, f"{c.name}: {action}"))
                step_num += 1

        # Templates
        for tmpl in self.templates:
            steps.append((step_num, f"{tmpl.name}: create template"))
            step_num += 1
            for action in (getattr(tmpl, "actions", None) or []):
                steps.append((step_num, f"{tmpl.name}: {action}"))
                step_num += 1

        # Non-swarm containers
        for c in self.non_swarm_containers:
            steps.append((step_num, f"{c.name}: create container"))
            step_num += 1
            for action in (getattr(c, "actions", None) or []):
                steps.append((step_num, f"{c.name}: {action}"))
                step_num += 1

        # Swarm containers (managers + workers)
        containers = self.cfg.containers
        swarm_containers = [c for c in containers if c.type in ("swarm-manager", "swarm-node")]
        for c in swarm_containers:
            steps.append((step_num, f"{c.name}: create container"))
            step_num += 1
            for action in (getattr(c, "actions", None) or []):
                steps.append((step_num, f"{c.name}: {action}"))
                step_num += 1

        logger.info("")
        end_step_display = self.end_step if self.end_step is not None else self.total_steps
        logger.info(
            "Deploy plan (total %d steps, running %d-%d):",
            self.total_steps,
            self.start_step,
            end_step_display,
        )
        for num, label in steps:
            end_step = self.end_step if self.end_step is not None else self.total_steps
            if self.start_step <= num <= end_step:
                marker = "RUN"
            else:
                marker = "skip"
            logger.info("  [%2d] %-4s %s", num, marker, label)

    def _build_plan(self) -> "Deploy":
        cfg = self.cfg
        start_step = self.start_step
        end_step = self.end_step
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
            total_steps += self._count_actions(apt_cache_container)
        for template in templates:
            total_steps += 1  # Template creation step
            total_steps += self._count_actions(template)
        for container in non_swarm:
            total_steps += 1  # Container creation step
            total_steps += self._count_actions(container)
        # Swarm containers also have creation + actions
        swarm_containers = [c for c in containers if c.type in ("swarm-manager", "swarm-node")]
        for container in swarm_containers:
            total_steps += 1  # Container creation step
            total_steps += self._count_actions(container)
        # Add swarm setup step if we have swarm containers
        if swarm_containers:
            total_steps += 1  # Swarm setup step
        # Add GlusterFS setup step if configured
        if cfg.glusterfs and swarm_containers:
            total_steps += 1  # GlusterFS setup step
        if not apt_cache_container:
            raise DeployError(f"apt-cache container '{cfg.apt_cache_ct}' not found in configuration")
        if end_step is None:
            end_step = total_steps
        self.apt_cache_container = apt_cache_container
        self.templates = templates
        self.non_swarm_containers = non_swarm
        self.total_steps = total_steps
        self.current_action_step = start_step - 1
        return self

    def _create_apt_cache(self, plan: "Deploy"):
        container_cfg = plan.apt_cache_container
        logger.info("\n[%s/%s] Creating apt-cache container first...", plan.step, plan.total_steps)
        self._create_container_with_base_template(container_cfg, plan)
        plan.step += 1

    def _create_container_with_base_template(self, container_cfg, plan: "Deploy"):
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

    def _create_templates(self, plan: "Deploy"):
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

    def _create_non_swarm_containers(self, plan: "Deploy"):
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

    def _deploy_swarm_stage(self, plan: "Deploy"):
        logger.info("[%s/%s] Deploying Docker Swarm...", plan.step, plan.total_steps)
        if not deploy_swarm(plan.cfg):
            raise DeployError("Docker Swarm deployment failed")
        plan.step += 1

    def _setup_gluster_stage(self, plan: "Deploy"):
        logger.info("[%s/%s] Setting up GlusterFS distributed storage...", plan.step, plan.total_steps)
        if not setup_glusterfs(plan.cfg):
            raise DeployError("GlusterFS setup failed")

    def _check_service_ports(self):
        """Check if all service ports are responding"""
        logger.info("Checking service ports...")
        import time
        # Wait a bit for services to fully start
        time.sleep(5)
        failed_ports = []
        # Connect LXC service if not already connected
        if not self.lxc_service or not self.lxc_service.is_connected():
            if not self.lxc_service.connect():
                logger.error("Failed to connect to Proxmox host %s", self.cfg.proxmox_host)
                return failed_ports
        # Check apt-cache
        apt_cache_ct = next((c for c in self.cfg.containers if c.name == self.cfg.apt_cache_ct), None)
        if apt_cache_ct:
            port = self.cfg.services.apt_cache.port or 3142
            result, _ = self.lxc_service.execute(f"nc -zv {apt_cache_ct.ip_address} {port} 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ apt-cache: %s:%s", apt_cache_ct.ip_address, port)
            else:
                logger.error("  ✗ apt-cache: %s:%s - NOT RESPONDING", apt_cache_ct.ip_address, port)
                failed_ports.append(("apt-cache", apt_cache_ct.ip_address, port))
        # Check PostgreSQL
        pgsql_ct = next((c for c in self.cfg.containers if c.type == "pgsql"), None)
        if pgsql_ct:
            port = self.cfg.services.postgresql.port if self.cfg.services.postgresql else 5432
            result, _ = self.lxc_service.execute(f"nc -zv {pgsql_ct.ip_address} {port} 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ PostgreSQL: %s:%s", pgsql_ct.ip_address, port)
            else:
                logger.error("  ✗ PostgreSQL: %s:%s - NOT RESPONDING", pgsql_ct.ip_address, port)
                failed_ports.append(("PostgreSQL", pgsql_ct.ip_address, port))
        # Check HAProxy
        haproxy_ct = next((c for c in self.cfg.containers if c.type == "haproxy"), None)
        if haproxy_ct:
            http_port = self.cfg.services.haproxy.http_port if self.cfg.services.haproxy else 80
            stats_port = self.cfg.services.haproxy.stats_port if self.cfg.services.haproxy else 8404
            result, _ = self.lxc_service.execute(f"nc -zv {haproxy_ct.ip_address} {http_port} 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ HAProxy HTTP: %s:%s", haproxy_ct.ip_address, http_port)
            else:
                logger.error("  ✗ HAProxy HTTP: %s:%s - NOT RESPONDING", haproxy_ct.ip_address, http_port)
                failed_ports.append(("HAProxy HTTP", haproxy_ct.ip_address, http_port))
            result, _ = self.lxc_service.execute(f"nc -zv {haproxy_ct.ip_address} {stats_port} 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ HAProxy Stats: %s:%s", haproxy_ct.ip_address, stats_port)
            else:
                logger.error("  ✗ HAProxy Stats: %s:%s - NOT RESPONDING", haproxy_ct.ip_address, stats_port)
                failed_ports.append(("HAProxy Stats", haproxy_ct.ip_address, stats_port))
        # Check DNS (both TCP and UDP)
        dns_ct = next((c for c in self.cfg.containers if c.type == "dns"), None)
        if dns_ct:
            port = dns_ct.params.get("dns_port", 53)
            result_tcp, _ = self.lxc_service.execute(f"nc -zv {dns_ct.ip_address} {port} 2>&1")
            result_udp, _ = self.lxc_service.execute(f"nc -zuv {dns_ct.ip_address} {port} 2>&1")
            if (result_tcp and ("open" in result_tcp.lower() or "succeeded" in result_tcp.lower())) or \
               (result_udp and ("open" in result_udp.lower() or "succeeded" in result_udp.lower())):
                logger.info("  ✓ DNS: %s:%s", dns_ct.ip_address, port)
            else:
                logger.error("  ✗ DNS: %s:%s - NOT RESPONDING", dns_ct.ip_address, port)
                failed_ports.append(("DNS", dns_ct.ip_address, port))
        # Check Docker Swarm
        swarm_manager = next((c for c in self.cfg.containers if c.type == "swarm-manager"), None)
        if swarm_manager:
            port = self.cfg.services.docker_swarm.port or 2377
            result, _ = self.lxc_service.execute(f"nc -zv {swarm_manager.ip_address} {port} 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ Docker Swarm: %s:%s", swarm_manager.ip_address, port)
            else:
                logger.error("  ✗ Docker Swarm: %s:%s - NOT RESPONDING", swarm_manager.ip_address, port)
                failed_ports.append(("Docker Swarm", swarm_manager.ip_address, port))
        # Check Portainer
        if swarm_manager and self.cfg.services.portainer:
            port = self.cfg.services.portainer.port or 9443
            result, _ = self.lxc_service.execute(f"nc -zv {swarm_manager.ip_address} {port} 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ Portainer: %s:%s", swarm_manager.ip_address, port)
            else:
                logger.error("  ✗ Portainer: %s:%s - NOT RESPONDING", swarm_manager.ip_address, port)
                failed_ports.append(("Portainer", swarm_manager.ip_address, port))
        # Check GlusterFS
        if swarm_manager and self.cfg.glusterfs:
            result, _ = self.lxc_service.execute(f"nc -zv {swarm_manager.ip_address} 24007 2>&1")
            if result and ("open" in result.lower() or "succeeded" in result.lower()):
                logger.info("  ✓ GlusterFS: %s:24007", swarm_manager.ip_address)
            else:
                logger.error("  ✗ GlusterFS: %s:24007 - NOT RESPONDING", swarm_manager.ip_address)
                failed_ports.append(("GlusterFS", swarm_manager.ip_address, 24007))
        return failed_ports

def _log_deploy_summary(cfg, failed_ports=None):
    logger.info("\n%s", "=" * 50)
    if failed_ports:
        logger.info("Deploy Complete (with port failures)")
    else:
        logger.info("Deploy Complete!")
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