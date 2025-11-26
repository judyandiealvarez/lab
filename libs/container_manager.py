"""
Container Manager - common container creation and management
"""
import logging
import time
from libs.config import ContainerConfig, LabConfig
from libs.container import destroy_container
from cli import FileOps, User
from services import SSHService, APTService, LXCService, PCTService, TemplateService
from actions.registry import get_action_class
logger = logging.getLogger(__name__)

class Container:
    """Common container manager for all container types"""

    def __init__(self, container_cfg: ContainerConfig, cfg: LabConfig, plan=None):
        """Initialize container manager"""
        self.container_cfg = container_cfg
        self.cfg = cfg
        self.container_id: str | None = None
        self.plan = plan
        self.ssh_service: SSHService | None = None
        self.apt_service: APTService | None = None
        self.pct_service: PCTService | None = None
        self.lxc_service: LXCService | None = None

    def _setup_container_with_pct(self) -> bool:
        """Setup container using PCTService"""
        proxmox_host = self.cfg.proxmox_host
        container_id = self.container_cfg.id
        ip_address = self.container_cfg.ip_address
        hostname = self.container_cfg.hostname
        gateway = self.cfg.gateway
        # Determine template to use
        # If template field is "base" or None, use base template from template-config
        # Otherwise, use the template name from templates section
        if self.container_cfg.template == "base" or not self.container_cfg.template:
            # Use base template from template-config
            template_name = None  # None means use base template
        else:
            # Use template from templates section
            template_name = self.container_cfg.template
        # Create LXC service for proxmox host
        self.lxc_service = LXCService(proxmox_host, self.cfg.ssh)
        if not self.lxc_service.connect():
            logger.error("Failed to connect to Proxmox host %s", proxmox_host)
            return False
        # Create services
        self.pct_service = PCTService(self.lxc_service)
        template_service = TemplateService(self.lxc_service)
        # Destroy if exists
        self.pct_service.destroy(container_id, force=True)
        # Get template path
        template_path = template_service.get_template_path(template_name, self.cfg)
        # Validate template file exists and is readable
        if not template_service.validate_template(template_path):
            logger.error("Template file %s is missing or not readable", template_path)
            base_template = template_service.get_base_template(self.cfg)
            template_path = f"{self.cfg.proxmox_template_dir}/{base_template}"
            logger.warning("Falling back to base template: %s", template_path)
        # Check if container already exists
        from libs.container import container_exists, destroy_container
        logger.info("Checking if container %s already exists...", container_id)
        container_already_exists = container_exists(proxmox_host, container_id, cfg=self.cfg)
        # Only destroy and recreate if:
        # 1. Container doesn't exist, OR
        # 2. We're starting from step 1 (full creation)
        if not container_already_exists or (self.plan and self.plan.start_step == 1):
            if container_already_exists:
                destroy_container(proxmox_host, container_id, cfg=self.cfg, lxc_service=self.lxc_service)
        # Get container resources
        resources = self.container_cfg.resources
        if not resources:
            from libs.config import ContainerResources
            resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)
        # Determine if container should be privileged
        # Swarm containers and containers using docker templates need privileged
        is_docker_template = self.container_cfg.template and "docker" in self.container_cfg.template.lower()
        unprivileged = (
            self.container_cfg.type not in ("swarm-manager", "swarm-node")
            and not is_docker_template
        )
        # Create container only if it doesn't exist or we're starting from step 1
        if not container_already_exists or (self.plan and self.plan.start_step == 1):
            logger.info("Creating container %s from template...", container_id)
            output, exit_code = self.pct_service.create(
                container_id=container_id,
                template_path=template_path,
                hostname=hostname,
                memory=resources.memory,
                swap=resources.swap,
                cores=resources.cores,
                ip_address=ip_address,
                gateway=gateway,
                bridge=self.cfg.proxmox_bridge,
                storage=self.cfg.proxmox_storage,
                rootfs_size=resources.rootfs_size,
                unprivileged=unprivileged,
                ostype="ubuntu",
                arch="amd64",
            )
            if exit_code is not None and exit_code != 0:
                current_step = self.plan.current_action_step if self.plan else 0
                logger.error("=" * 50)
                logger.error("Container Creation Failed")
                logger.error("=" * 50)
                logger.error("Container: %s", self.container_cfg.name)
                logger.error("Step: %d", current_step)
                logger.error("Error: Failed to create container %s: %s", container_id, output)
                logger.error("=" * 50)
                return False
        else:
            logger.info("Container %s already exists, skipping creation", container_id)
            # Verify container is running
            status_cmd = f"pct status {container_id}"
            status_output, _ = self.ssh_service.execute(status_cmd)
            if "running" not in status_output:
                logger.info("Starting existing container %s...", container_id)
                start_cmd = f"pct start {container_id}"
                self.ssh_service.execute(start_cmd)
                import time
                time.sleep(3)
            # Set container_id for later use
            self.container_id = container_id
            # Connect to container via SSH for actions
            from services.ssh import SSHConfig
            default_user = self.cfg.users.default_user
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
            self.ssh_service = SSHService(ip_address, container_ssh_config)
            if not self.ssh_service.connect():
                logger.error("Failed to connect to container %s at %s", container_id, ip_address)
                return False
            # Wait a moment for SSH to be ready
            import time
            time.sleep(2)
            # Skip to actions - container is already set up
            logger.info("Container %s is ready, proceeding to actions", container_id)
            return True
        # Set container features BEFORE starting (nesting required for systemd-networkd in unprivileged containers)
        logger.info("Setting container features...")
        output, exit_code = self.pct_service.set_features(container_id, nesting=True, keyctl=True, fuse=True)
        if exit_code is not None and exit_code != 0:
            logger.warning("Failed to set container features: %s", output)
        # Start container
        logger.info("Starting container %s...", container_id)
        output, exit_code = self.pct_service.start(container_id)
        if exit_code is not None and exit_code != 0:
            current_step = self.plan.current_action_step if self.plan else 0
            logger.error("=" * 50)
            logger.error("Container Start Failed")
            logger.error("=" * 50)
            logger.error("Container: %s", self.container_cfg.name)
            logger.error("Step: %d", current_step)
            logger.error("Error: Failed to start container %s: %s", container_id, output)
            logger.error("=" * 50)
            return False
        # Bring up networking by pinging external host
        logger.info("Bringing up network interface...")
        ping_cmd = "ping -c 1 8.8.8.8"
        output, exit_code = self.pct_service.execute(container_id, ping_cmd, timeout=10)
        if exit_code is not None and exit_code != 0:
            logger.warning("Ping to 8.8.8.8 failed (network may still be initializing): %s", output)
        else:
            logger.info("Network interface is up and reachable")
        # Setup users and SSH (using pct_exec via PCTService) before waiting
        import shlex
        for user_cfg in self.cfg.users.users:
            username = user_cfg.name
            sudo_group = user_cfg.sudo_group
            # Create user if it doesn't exist
            check_cmd = User().username(username).check_exists()
            add_cmd = User().username(username).shell("/bin/bash").groups([sudo_group]).create_home(True).add()
            user_check_cmd = f"{check_cmd} 2>&1 || {add_cmd}"
            output, exit_code = self.pct_service.execute(container_id, user_check_cmd)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to create user %s: %s", username, output)
                return False
            # Set password if provided
            if user_cfg.password:
                # Use chpasswd to set password non-interactively
                password_cmd = f"echo {shlex.quote(f'{username}:{user_cfg.password}')} | chpasswd"
                output, exit_code = self.pct_service.execute(container_id, password_cmd)
                if exit_code is not None and exit_code != 0:
                    logger.error("Failed to set password for user %s: %s", username, output)
                    return False
                logger.info("Password set for user %s", username)
            # Configure passwordless sudo
            sudoers_path = f"/etc/sudoers.d/{username}"
            sudoers_content = f"{username} ALL=(ALL) NOPASSWD: ALL\n"
            sudoers_write_cmd = FileOps().write(sudoers_path, sudoers_content)
            output, exit_code = self.pct_service.execute(container_id, sudoers_write_cmd)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to write sudoers file for user %s: %s", username, output)
                return False
            sudoers_chmod_cmd = FileOps().chmod(sudoers_path, "440")
            output, exit_code = self.pct_service.execute(container_id, sudoers_chmod_cmd)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to secure sudoers file for user %s: %s", username, output)
                return False
        # Use first user for SSH setup
        default_user = self.cfg.users.default_user
        # Setup SSH key
        if not self.pct_service.setup_ssh_key(container_id, ip_address, self.cfg):
            logger.error("Failed to setup SSH key")
            return False
        # Ensure SSH service is installed and running
        if not self.pct_service.ensure_ssh_service_running(container_id, self.cfg):
            logger.error("Failed to ensure SSH service is running")
            return False
        # Wait for container to be ready (includes SSH connectivity verification, up to 10 min)
        logger.info("Waiting for container to be ready with SSH connectivity (up to 10 minutes)...")
        if not self.pct_service.wait_for_container(container_id, ip_address, self.cfg, username=default_user):
            logger.error("Container %s did not become ready within 10 minutes", container_id)
            return False
        self.container_id = container_id
        # Don't disconnect lxc_service here - keep it connected for actions
        # It will be disconnected in create() after actions complete
        return True

    def create(self) -> bool:
        """Create container with actions"""
        # Treat container creation as a step
        if self.plan:
            self.plan.current_action_step += 1
            # Check if we should skip this step (before start_step)
            if self.plan.current_action_step < self.plan.start_step:
                logger.info("Skipping container '%s' creation (step %d < start_step %d)", 
                          self.container_cfg.name, self.plan.current_action_step, self.plan.start_step)
                return True
            # Check if we should stop (after end_step)
            if self.plan.end_step is not None and self.plan.current_action_step > self.plan.end_step:
                logger.info("Reached end step %d, stopping container creation", self.plan.end_step)
                return True
            # If start_step > 1 and container exists, skip creation and go straight to actions
            if self.plan.start_step > 1:
                from libs.container import container_exists
                container_id = str(self.container_cfg.id)
                proxmox_host = self.cfg.proxmox_host
                if container_exists(proxmox_host, container_id, cfg=self.cfg):
                    logger.info("Container '%s' already exists and start_step is %d, skipping creation and proceeding to actions", 
                              self.container_cfg.name, self.plan.start_step)
                    # Still need to setup SSH connection for actions
                    self.container_id = container_id
                    ip_address = self.container_cfg.ip_address
                    from services.ssh import SSHConfig
                    default_user = self.cfg.users.default_user
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
                    self.ssh_service = SSHService(ip_address, container_ssh_config)
                    if not self.ssh_service.connect():
                        logger.error("Failed to connect to container %s at %s", container_id, ip_address)
                        return False
                    time.sleep(2)
                    # Proceed to actions
                    return True
            # Log container creation start
            overall_pct = int((self.plan.current_action_step / self.plan.total_steps) * 100)
            logger.info("=" * 50)
            logger.info("[Overall: %d%%] [Container '%s': 0%%] [Step: %d] Starting container creation", 
                      overall_pct, self.container_cfg.name, self.plan.current_action_step)
            logger.info("=" * 50)
        if not self._setup_container_with_pct():
            return False
        # Connect to container via SSH (not via pct exec)
        container_ip = self.container_cfg.ip_address
        default_user = self.cfg.users.default_user
        container_ssh_host = f"{default_user}@{container_ip}"
        self.ssh_service = SSHService(container_ssh_host, self.cfg.ssh)
        # Wait a moment for SSH service to be fully ready
        time.sleep(3)
        # Connect to container
        if not self.ssh_service.connect():
            current_step = self.plan.current_action_step if self.plan else 0
            logger.error("=" * 50)
            logger.error("SSH Connection Failed")
            logger.error("=" * 50)
            logger.error("Container: %s", self.container_cfg.name)
            logger.error("Step: %d", current_step)
            logger.error("Error: Failed to establish SSH connection to container %s", container_ip)
            logger.error("=" * 50)
            if self.lxc_service:
                destroy_container(self.cfg.proxmox_host, self.container_id, cfg=self.cfg, lxc_service=self.lxc_service)
                self.lxc_service.disconnect()
            else:
                destroy_container(self.cfg.proxmox_host, self.container_id, cfg=self.cfg)
            return False
        # Create APT service
        self.apt_service = APTService(self.ssh_service)
        try:
            # Parse actions from container config
            action_names = self.container_cfg.actions if self.container_cfg.actions else []
            if not action_names:
                logger.warning("No actions specified in container config, skipping action execution")
                return True
            # Build action instances from config
            actions = []
            for action_name in action_names:
                try:
                    action_class = get_action_class(action_name)
                    # Create action instance with required services
                    action = action_class(
                        ssh_service=self.ssh_service,
                        apt_service=self.apt_service,
                        pct_service=self.pct_service,
                        container_id=self.container_id,
                        cfg=self.cfg,
                        container_cfg=self.container_cfg,
                    )
                    actions.append(action)
                except ValueError as e:
                    current_step = self.plan.current_action_step if self.plan else 0
                    logger.error("=" * 50)
                    logger.error("Action Creation Failed")
                    logger.error("=" * 50)
                    logger.error("Container: %s", self.container_cfg.name)
                    logger.error("Step: %d", current_step)
                    logger.error("Action Name: %s", action_name)
                    logger.error("Error: %s", e)
                    logger.error("=" * 50)
                    if self.lxc_service:
                        self.lxc_service.disconnect()
                    return False
            # Execute actions
            logger.info("Executing %d actions for container '%s'", len(actions), self.container_cfg.name)
            for idx, action in enumerate(actions, 1):
                # Increment step counter for this action
                if self.plan:
                    self.plan.current_action_step += 1
                # Check if we should skip this action (before start_step)
                if self.plan and self.plan.current_action_step < self.plan.start_step:
                    continue
                # Check if we should stop (after end_step)
                if self.plan and self.plan.end_step is not None and self.plan.current_action_step > self.plan.end_step:
                    logger.info("Reached end step %d, stopping action execution", self.plan.end_step)
                    return True
                # Calculate percentages
                overall_pct = 0
                container_pct = 0
                if self.plan:
                    overall_pct = int((self.plan.current_action_step / self.plan.total_steps) * 100)
                    container_pct = int((idx / len(actions)) * 100)
                    logger.info("=" * 50)
                    logger.info("[Overall: %d%%] [Container '%s': %d%%] [Step: %d] Starting action: %s", 
                              overall_pct, self.container_cfg.name, container_pct, self.plan.current_action_step, action.description)
                    logger.info("=" * 50)
                else:
                    logger.info("[%d/%d] Running action: %s", idx, len(actions), action.description)
                try:
                    if not action.execute():
                        current_step = self.plan.current_action_step if self.plan else idx
                        logger.error("=" * 50)
                        logger.error("Action Execution Failed")
                        logger.error("=" * 50)
                        logger.error("Container: %s", self.container_cfg.name)
                        logger.error("Step: %d", current_step)
                        logger.error("Action: %s", action.description)
                        logger.error("=" * 50)
                        if self.lxc_service:
                            self.lxc_service.disconnect()
                        return False
                    logger.info("[%d/%d] Action '%s' completed successfully", idx, len(actions), action.description)
                except Exception as exc:
                    current_step = self.plan.current_action_step if self.plan else idx
                    logger.error("=" * 50)
                    logger.error("Action Execution Exception")
                    logger.error("=" * 50)
                    logger.error("Container: %s", self.container_cfg.name)
                    logger.error("Step: %d", current_step)
                    logger.error("Action: %s", action.description)
                    logger.error("Error: %s", exc)
                    logger.error("=" * 50)
                    logger.error("Exception details:", exc_info=True)
                    if self.lxc_service:
                        self.lxc_service.disconnect()
                    return False
            logger.info("Container '%s' created successfully", self.container_cfg.name)
            return True
        finally:
            if self.ssh_service:
                self.ssh_service.disconnect()
            # Disconnect lxc_service after all actions complete
            if self.lxc_service:
                self.lxc_service.disconnect()

# Backward compatibility function
def create_container(container_cfg: ContainerConfig, cfg: LabConfig, plan=None) -> bool:
    """Create container (backward compatibility wrapper)"""
    container = Container(container_cfg, cfg, plan=plan)
    return container.create()

