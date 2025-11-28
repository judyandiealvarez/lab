"""
PCT Service - uses LXC service to execute PCT CLI commands
"""
import base64
import logging
import subprocess
import time
from pathlib import Path
from typing import Optional
from .lxc import LXCService
from cli.pct import PCT
logger = logging.getLogger(__name__)

class PCTService:
    """Service for executing PCT commands using LXC service"""
    # Configuration constants
    DEFAULT_SHELL = "bash"
    BASE64_DECODE_CMD = "base64 -d"

    def __init__(self, lxc_service: LXCService, shell: str = None):
        """
        Initialize PCT service
        Args:
            lxc_service: LXC service instance with SSH connection
            shell: Shell to use for command execution (default: bash)
        """
        self.lxc = lxc_service
        self.shell = shell or self.DEFAULT_SHELL

    def _encode_command(self, command: str) -> str:
        """
        Encode command using base64 to avoid quote escaping issues
        Args:
            command: Command to encode
        Returns:
            Base64 encoded command
        """
        encoded = base64.b64encode(command.encode("utf-8")).decode("ascii")
        return encoded

    def _build_pct_exec_command(self, container_id: str, command: str) -> str:
        """
        Build pct exec command string
        Args:
            container_id: Container ID
            command: Command to execute in container
        Returns:
            Full pct exec command string
        """
        encoded_cmd = self._encode_command(command)
        return (
            f"pct exec {container_id} -- {self.shell} -c "
            f'"echo {encoded_cmd} | {self.BASE64_DECODE_CMD} | {self.shell}"'
        )

    def execute(self, container_id: str, command: str, timeout: Optional[int] = None, sudo: bool = False) -> tuple[Optional[str], Optional[int]]:
        """
        Execute command in container via pct exec (always shows output interactively and captures it)
        Args:
            container_id: Container ID
            command: Command to execute
            timeout: Command timeout in seconds
            sudo: Whether to run command with sudo
        Returns:
            Tuple of (output, exit_code). output is always captured
        """
        if sudo:
            command = f"sudo -n {command}"
        logger.debug("Running in container %s: %s", container_id, command)
        pct_cmd = self._build_pct_exec_command(container_id, command)
        return self.lxc.execute(pct_cmd, timeout=timeout)

    def create(
        self,
        container_id: str,
        template_path: str,
        hostname: str,
        memory: int,
        swap: int,
        cores: int,
        ip_address: str,
        gateway: str,
        bridge: str,
        storage: str,
        rootfs_size: int,
        unprivileged: bool = True,
        ostype: str = "ubuntu",
        arch: str = "amd64",
    ) -> tuple[Optional[str], Optional[int]]:
        """
        Create container using pct create
        Args:
            container_id: Container ID
            template_path: Path to template
            hostname: Container hostname
            memory: Memory in MB
            swap: Swap in MB
            cores: Number of CPU cores
            ip_address: IP address
            gateway: Gateway IP
            bridge: Network bridge
            storage: Storage name
            rootfs_size: Root filesystem size in GB
            unprivileged: Whether container is unprivileged
            ostype: OS type
            arch: Architecture
        Returns:
            Tuple of (output, exit_code)
        """
        cmd = (
            PCT()
            .container_id(container_id)
            .create(
                template_path=template_path,
                hostname=hostname,
                memory=memory,
                swap=swap,
                cores=cores,
                ip_address=ip_address,
                gateway=gateway,
                bridge=bridge,
                storage=storage,
                rootfs_size=rootfs_size,
                unprivileged=unprivileged,
                ostype=ostype,
                arch=arch,
            )
        )
        # Remove 2>&1 from command since we handle it in execute
        cmd = cmd.replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def start(self, container_id: str) -> tuple[Optional[str], Optional[int]]:
        """
        Start container using pct start
        Args:
            container_id: Container ID
        Returns:
            Tuple of (output, exit_code)
        """
        cmd = PCT().container_id(container_id).start().replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def stop(self, container_id: str, force: bool = False) -> tuple[Optional[str], Optional[int]]:
        """
        Stop container using pct stop
        Args:
            container_id: Container ID
            force: Whether to force stop
        Returns:
            Tuple of (output, exit_code)
        """
        pct = PCT().container_id(container_id)
        if force:
            pct.force()
        cmd = pct.stop().replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def status(self, container_id: Optional[str] = None) -> tuple[Optional[str], Optional[int]]:
        """
        Get container status using pct status
        Args:
            container_id: Container ID (None for list all)
        Returns:
            Tuple of (output, exit_code)
        """
        pct = PCT()
        if container_id:
            pct.container_id(container_id)
        cmd = pct.status().replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def destroy(self, container_id: str, force: bool = False) -> tuple[Optional[str], Optional[int]]:
        """
        Destroy container using pct destroy
        Args:
            container_id: Container ID
            force: Whether to force destroy
        Returns:
            Tuple of (output, exit_code)
        """
        pct = PCT().container_id(container_id)
        if force:
            pct.force()
        cmd = pct.destroy().replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def set_features(
        self,
        container_id: str,
        nesting: bool = True,
        keyctl: bool = True,
        fuse: bool = True,
    ) -> tuple[Optional[str], Optional[int]]:
        """
        Set container features using pct set --features
        Args:
            container_id: Container ID
            nesting: Enable nesting
            keyctl: Enable keyctl
            fuse: Enable fuse
        Returns:
            Tuple of (output, exit_code)
        """
        pct = PCT().container_id(container_id)
        pct.nesting(nesting)
        pct.keyctl(keyctl)
        pct.fuse(fuse)
        cmd = pct.set_features().replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def config(self, container_id: str) -> tuple[Optional[str], Optional[int]]:
        """
        Get container configuration using pct config
        Args:
            container_id: Container ID
        Returns:
            Tuple of (output, exit_code)
        """
        cmd = PCT().container_id(container_id).config().replace(" 2>&1", "")
        return self.lxc.execute(cmd)

    def wait_for_container(
        self,
        container_id: str,
        ip_address: str,
        cfg,
        max_attempts: Optional[int] = None,
        sleep_interval: Optional[int] = None,
        username: Optional[str] = None,
    ) -> bool:
        """
        Wait for container to be ready, including SSH connectivity verification
        Args:
            container_id: Container ID
            ip_address: Container IP address
            cfg: Lab configuration
            max_attempts: Maximum number of attempts (default: calculated for 10 min)
            sleep_interval: Sleep interval between attempts (default from config)
            username: SSH username for verification (optional, uses default from config if not provided)
        Returns:
            True if container is ready and SSH is accessible, False otherwise
        """
        # Calculate max_attempts for 10 minute timeout
        if max_attempts is None:
            if sleep_interval is None:
                sleep_interval = cfg.waits.container_ready_sleep if cfg and hasattr(cfg, "waits") else 3
            # 10 minutes = 600 seconds, calculate attempts based on sleep_interval
            max_attempts = max(int(600 / sleep_interval), 1)
        if sleep_interval is None:
            sleep_interval = cfg.waits.container_ready_sleep if cfg and hasattr(cfg, "waits") else 3
        if username is None:
            username = cfg.users.default_user if cfg and hasattr(cfg, "users") else "root"
        start_time = time.time()
        max_wait_time = 600  # 10 minutes in seconds
        for i in range(1, max_attempts + 1):
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                logger.error("Container readiness check exceeded 10 minute timeout")
                return False
            status, _ = self.status(container_id)
            if status and "running" in status:
                # Try pct exec (most reliable - works from Proxmox host)
                try:
                    test_output, exit_code = self.execute(container_id, "echo test", timeout=5)
                    if exit_code == 0 and test_output == "test":
                        logger.debug("Container is up (pct exec working)")
                    else:
                        logger.debug("pct exec not working yet, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                        time.sleep(sleep_interval)
                        continue
                except (OSError, subprocess.SubprocessError):
                    logger.debug("pct exec failed, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                    time.sleep(sleep_interval)
                    continue
                # Container is running and pct exec works, now verify SSH connectivity
                # Check if we can reach the container from local machine (for SSH)
                try:
                    ping_check = subprocess.run(f"ping -c 1 -W 2 {ip_address}", shell=True, timeout=5, check=False)
                    if ping_check.returncode == 0:
                        # Local machine can reach container, check if port 22 is open
                        try:
                            import socket
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(3)
                            port_result = sock.connect_ex((ip_address, 22))
                            sock.close()
                            if port_result == 0:
                                # Port 22 is open, verify SSH
                                if self.verify_ssh_connectivity(container_id, ip_address, username, cfg):
                                    logger.info("Container is ready and SSH is accessible!")
                                    return True
                                else:
                                    logger.debug("SSH verification failed, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                                    time.sleep(sleep_interval)
                                    continue
                            else:
                                logger.debug("Port 22 not reachable yet, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                                time.sleep(sleep_interval)
                                continue
                        except (OSError, socket.error):
                            logger.debug("Port 22 check failed, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                            time.sleep(sleep_interval)
                            continue
                    else:
                        # Local machine cannot reach container yet, wait and retry
                        logger.debug("Container not reachable from local machine yet, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                        time.sleep(sleep_interval)
                        continue
                except (subprocess.TimeoutExpired, OSError):
                    logger.debug("Ping check failed, waiting... (attempt %s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
                    time.sleep(sleep_interval)
                    continue
            logger.debug("Container not running yet, waiting... (%s/%s, elapsed: %.1fs)", i, max_attempts, elapsed)
            time.sleep(sleep_interval)
        logger.error("Container did not become ready within 10 minutes")
        return False

    def verify_ssh_connectivity(self, container_id: str, ip_address: str, username: str, cfg) -> bool:
        """
        Verify SSH connectivity by actually attempting a connection
        Args:
            container_id: Container ID
            ip_address: Container IP address
            username: SSH username
            cfg: Lab configuration
        Returns:
            True if SSH connection works, False otherwise
        """
        from .ssh import SSHService
        from libs.config import SSHConfig
        # Actually test SSH connection with longer timeout for initial connection
        test_host = f"{username}@{ip_address}"
        # Use longer timeout for verification (network might still be stabilizing)
        verify_ssh_config = SSHConfig(
            connect_timeout=max(cfg.ssh.connect_timeout * 2, 20),
            batch_mode=cfg.ssh.batch_mode,
            default_exec_timeout=cfg.ssh.default_exec_timeout,
            read_buffer_size=cfg.ssh.read_buffer_size,
            poll_interval=cfg.ssh.poll_interval,
            default_username=cfg.ssh.default_username,
            look_for_keys=cfg.ssh.look_for_keys,
            allow_agent=cfg.ssh.allow_agent,
            verbose=cfg.ssh.verbose if hasattr(cfg.ssh, "verbose") else False,
        )
        test_ssh = SSHService(test_host, verify_ssh_config)
        logger.info("Testing SSH connection to %s...", test_host)
        if test_ssh.connect():
            # Test that we can execute a command
            output, exit_code = test_ssh.execute("echo 'SSH connection test successful'", timeout=5
            )
            test_ssh.disconnect()
            if exit_code == 0 and output:
                logger.info("SSH connectivity verified - connection successful")
                return True
            else:
                logger.error("SSH connection established but command execution failed: %s (exit_code: %s)", output, exit_code)
                return False
        else:
            logger.error("SSH connection test failed - cannot connect to %s", test_host)
            return False

    def setup_ssh_key(self, container_id: str, ip_address: str, cfg) -> bool:
        """
        Setup SSH key in container
        Args:
            container_id: Container ID
            ip_address: Container IP address
            cfg: Lab configuration
        Returns:
            True if SSH key setup successful, False otherwise
        """
        # Get SSH public key
        key_paths = [
            Path.home() / ".ssh" / "id_rsa.pub",
            Path.home() / ".ssh" / "id_ed25519.pub",
        ]
        ssh_key = None
        for key_path in key_paths:
            if key_path.exists():
                ssh_key = key_path.read_text().strip()
                break
        if not ssh_key:
            logger.error("No SSH key found")
            return False
        # Remove old host key
        subprocess.run(f"ssh-keygen -R {ip_address} 2>/dev/null", shell=True, check=False)
        # Base64 encode the key to avoid any shell escaping problems
        key_b64 = base64.b64encode(ssh_key.encode("utf-8")).decode("ascii")
        # Add to all configured users (only if they exist)
        if cfg and hasattr(cfg, "users") and hasattr(cfg.users, "users"):
            for user_cfg in cfg.users.users:
                username = user_cfg.name
                # Check if user exists before setting up SSH keys
                check_user_cmd = f"id -u {username} >/dev/null 2>&1 && echo 'exists' || echo 'missing'"
                check_output, _ = self.execute(container_id, check_user_cmd)
                if check_output and "exists" in check_output:
                    user_cmd = (
                        f"mkdir -p /home/{username}/.ssh && echo {key_b64} | base64 -d > "
                        f"/home/{username}/.ssh/authorized_keys && "
                        f"chmod 600 /home/{username}/.ssh/authorized_keys && "
                        f"chown {username}:{username} /home/{username}/.ssh/authorized_keys && "
                        f"chown -R {username}:{username} /home/{username}/.ssh && "
                        f"chmod 700 /home/{username}/.ssh"
                    )
                    self.execute(container_id, user_cmd)
                else:
                    logger.debug("User %s does not exist, skipping SSH key setup", username)
        else:
            # Backward compatibility: use default_user
            default_user = cfg.users.default_user if cfg and hasattr(cfg, "users") else "jaal"
            user_cmd = (
                f"mkdir -p /home/{default_user}/.ssh && echo {key_b64} | base64 -d > "
                f"/home/{default_user}/.ssh/authorized_keys && "
                f"chmod 600 /home/{default_user}/.ssh/authorized_keys && "
                f"chown {default_user}:{default_user} /home/{default_user}/.ssh/authorized_keys && "
                f"chown -R {default_user}:{default_user} /home/{default_user}/.ssh && "
                f"chmod 700 /home/{default_user}/.ssh"
            )
            self.execute(container_id, user_cmd)
        # Add to root user
        root_cmd = (
            f"mkdir -p /root/.ssh && echo {key_b64} | base64 -d > "
            f"/root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
        )
        self.execute(container_id, root_cmd)
        # Verify the key file exists
        default_user = cfg.users.default_user if cfg and hasattr(cfg, "users") else "jaal"
        verify_cmd = (
            f"test -f /home/{default_user}/.ssh/authorized_keys && "
            f"test -f /root/.ssh/authorized_keys && echo 'keys_exist' || echo 'keys_missing'"
        )
        verify_output, _ = self.execute(container_id, verify_cmd)
        if verify_output and "keys_exist" in verify_output:
            logger.info("SSH key setup verified successfully")
            return True
        logger.error("SSH key verification failed: %s", verify_output)
        return False

    def ensure_ssh_service_running(self, container_id: str, cfg) -> bool:
        """
        Ensure SSH service is installed and running in container
        Args:
            container_id: Container ID
            cfg: Lab configuration
        Returns:
            True if SSH service is running, False otherwise
        """
        from cli import SystemCtl, Apt
        # Check if openssh-server is installed
        check_ssh_cmd = "dpkg -l | grep -q '^ii.*openssh-server' || echo 'not_installed'"
        check_output, _ = self.execute(container_id, check_ssh_cmd)
        if check_output and "not_installed" in check_output:
            logger.info("openssh-server not installed, installing...")
            # First update apt
            update_cmd = Apt().quiet().update()
            output, exit_code = self.execute(container_id, update_cmd, timeout=300)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to update apt: %s", output)
                return False
            # Install openssh-server
            install_cmd = Apt().quiet().install(["openssh-server"])
            output, exit_code = self.execute(container_id, install_cmd, timeout=300)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to install openssh-server: %s", output)
                return False
            logger.info("openssh-server installed successfully")
        # Ensure SSH service is enabled and started
        enable_cmd = SystemCtl().service("ssh").enable()
        start_cmd = SystemCtl().service("ssh").start()
        # Enable SSH service
        output, exit_code = self.execute(container_id, enable_cmd)
        if exit_code is not None and exit_code != 0:
            logger.warning("Failed to enable SSH service: %s", output)
        # Start SSH service
        output, exit_code = self.execute(container_id, start_cmd)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to start SSH service: %s", output)
            return False
        # Wait a moment for service to start
        time.sleep(3)
        # Check if SSH service is active
        status_cmd = SystemCtl().service("ssh").is_active()
        status_output, exit_code = self.execute(container_id, status_cmd)
        if exit_code == 0 and SystemCtl.parse_is_active(status_output):
            logger.info("SSH service is running")
            # DIAGNOSTIC: Check what SSH is actually listening on
            listen_cmd = "ss -tlnp | grep ':22 ' || netstat -tlnp 2>/dev/null | grep ':22 ' || echo 'not_listening'"
            listen_output, _ = self.execute(container_id, listen_cmd)
            logger.info("SSH listening check: %s", listen_output)
            # DIAGNOSTIC: Check SSH config
            sshd_config_check = "grep -E '^ListenAddress|^#ListenAddress' /etc/ssh/sshd_config 2>/dev/null | head -5 || echo 'config_check_failed'"
            sshd_config_output, _ = self.execute(container_id, sshd_config_check)
            logger.info("SSH config check: %s", sshd_config_output)
            # DIAGNOSTIC: Check if SSH can accept connections
            test_connection_cmd = (
                "timeout 1 bash -c '</dev/tcp/127.0.0.1/22' 2>&1 && echo 'localhost_ok' || echo 'localhost_failed'"
            )
            test_output, _ = self.execute(container_id, test_connection_cmd)
            logger.info("SSH localhost connection test: %s", test_output)
            return True
        logger.error("SSH service is not active after start attempt")
        # DIAGNOSTIC: Get detailed status
        status_detail_cmd = "systemctl status ssh --no-pager -l 2>&1 | head -20"
        status_detail, _ = self.execute(container_id, status_detail_cmd)
        logger.error("SSH service status details: %s", status_detail)
        return False

    def create_and_setup_container(self, container_cfg, cfg, plan=None):
        """
        Create and setup container with all actions - main entry point for container management
        Args:
            container_cfg: ContainerConfig instance
            cfg: LabConfig instance
            plan: Optional deployment plan
        Returns:
            tuple: (ssh_service, apt_service) if successful, (None, None) if failed
        """
        from libs.config import ContainerConfig, LabConfig, ContainerResources
        from libs.common import container_exists, destroy_container
        from services import SSHService, APTService, TemplateService
        from services.ssh import SSHConfig
        from cli import FileOps, User
        from actions.registry import get_action_class
        import shlex
        
        container_id = str(container_cfg.id)
        ip_address = container_cfg.ip_address
        hostname = container_cfg.hostname
        gateway = cfg.gateway
        
        # Treat container creation as a step
        if plan:
            plan.current_action_step += 1
            # Check if we should skip this step (before start_step)
            if plan.current_action_step < plan.start_step:
                logger.info("Skipping container '%s' creation (step %d < start_step %d)", 
                          container_cfg.name, plan.current_action_step, plan.start_step)
                # Still need to connect for actions if container exists
                if container_exists(cfg.proxmox_host, container_id, cfg=cfg):
                    default_user = cfg.users.default_user
                    container_ssh_config = SSHConfig(
                        connect_timeout=cfg.ssh.connect_timeout,
                        batch_mode=cfg.ssh.batch_mode,
                        default_exec_timeout=cfg.ssh.default_exec_timeout,
                        read_buffer_size=cfg.ssh.read_buffer_size,
                        poll_interval=cfg.ssh.poll_interval,
                        default_username=default_user,
                        look_for_keys=cfg.ssh.look_for_keys,
                        allow_agent=cfg.ssh.allow_agent,
                        verbose=cfg.ssh.verbose,
                    )
                    ssh_service = SSHService(f"{default_user}@{ip_address}", container_ssh_config)
                    if ssh_service.connect():
                        apt_service = APTService(ssh_service)
                        return (ssh_service, apt_service)
                return (None, None)
            # Check if we should stop (after end_step)
            if plan.end_step is not None and plan.current_action_step > plan.end_step:
                logger.info("Reached end step %d, stopping container creation", plan.end_step)
                return (None, None)
            # If start_step > 1 and container exists, skip creation and go straight to actions
            if plan.start_step > 1:
                if container_exists(cfg.proxmox_host, container_id, cfg=cfg):
                    logger.info("Container '%s' already exists and start_step is %d, skipping creation and proceeding to actions", 
                              container_cfg.name, plan.start_step)
                    default_user = cfg.users.default_user
                    container_ssh_config = SSHConfig(
                        connect_timeout=cfg.ssh.connect_timeout,
                        batch_mode=cfg.ssh.batch_mode,
                        default_exec_timeout=cfg.ssh.default_exec_timeout,
                        read_buffer_size=cfg.ssh.read_buffer_size,
                        poll_interval=cfg.ssh.poll_interval,
                        default_username=default_user,
                        look_for_keys=cfg.ssh.look_for_keys,
                        allow_agent=cfg.ssh.allow_agent,
                        verbose=cfg.ssh.verbose,
                    )
                    ssh_service = SSHService(f"{default_user}@{ip_address}", container_ssh_config)
                    if not ssh_service.connect():
                        logger.error("Failed to connect to container %s at %s", container_id, ip_address)
                        return (None, None)
                    time.sleep(2)
                    apt_service = APTService(ssh_service)
                    return (ssh_service, apt_service)
            # Log container creation start
            overall_pct = int((plan.current_action_step / plan.total_steps) * 100)
            logger.info("=" * 50)
            logger.info("[Overall: %d%%] [Container '%s': 0%%] [Step: %d] Starting container creation", 
                      overall_pct, container_cfg.name, plan.current_action_step)
            logger.info("=" * 50)
        
        # Setup container
        if not self._setup_container(container_cfg, cfg, plan):
            return (None, None)
        
        # Connect to container via SSH
        default_user = cfg.users.default_user
        container_ssh_host = f"{default_user}@{ip_address}"
        ssh_service = SSHService(container_ssh_host, cfg.ssh)
        # Wait a moment for SSH service to be fully ready
        time.sleep(3)
        # Connect to container
        if not ssh_service.connect():
            current_step = plan.current_action_step if plan else 0
            logger.error("=" * 50)
            logger.error("SSH Connection Failed")
            logger.error("=" * 50)
            logger.error("Container: %s", container_cfg.name)
            logger.error("Step: %d", current_step)
            logger.error("Error: Failed to establish SSH connection to container %s", ip_address)
            logger.error("=" * 50)
            destroy_container(cfg.proxmox_host, container_id, cfg=cfg, lxc_service=self.lxc)
            return (None, None)
        
        # Create APT service
        apt_service = APTService(ssh_service)
        
        try:
            # Parse actions from container config
            action_names = container_cfg.actions if container_cfg.actions else []
            if not action_names:
                logger.warning("No actions specified in container config, skipping action execution")
                return (ssh_service, apt_service)
            
            # Build action instances from config
            actions = []
            for action_name in action_names:
                try:
                    action_class = get_action_class(action_name)
                    # Create action instance with required services
                    action = action_class(
                        ssh_service=ssh_service,
                        apt_service=apt_service,
                        pct_service=self,
                        container_id=container_id,
                        cfg=cfg,
                        container_cfg=container_cfg,
                    )
                    # Pass plan to action if available
                    if plan and hasattr(action, "plan"):
                        action.plan = plan
                    actions.append(action)
                except ValueError as e:
                    current_step = plan.current_action_step if plan else 0
                    logger.error("=" * 50)
                    logger.error("Action Creation Failed")
                    logger.error("=" * 50)
                    logger.error("Container: %s", container_cfg.name)
                    logger.error("Step: %d", current_step)
                    logger.error("Action Name: %s", action_name)
                    logger.error("Error: %s", e)
                    logger.error("=" * 50)
                    return (None, None)
            
            # Execute actions
            logger.info("Executing %d actions for container '%s'", len(actions), container_cfg.name)
            for idx, action in enumerate(actions, 1):
                # Increment step counter for this action
                if plan:
                    plan.current_action_step += 1
                # Check if we should skip this action (before start_step)
                if plan and plan.current_action_step < plan.start_step:
                    continue
                # Check if we should stop (after end_step)
                if plan and plan.end_step is not None and plan.current_action_step > plan.end_step:
                    logger.info("Reached end step %d, stopping action execution", plan.end_step)
                    return (ssh_service, apt_service)
                # Calculate percentages
                overall_pct = 0
                container_pct = 0
                if plan:
                    overall_pct = int((plan.current_action_step / plan.total_steps) * 100)
                    container_pct = int((idx / len(actions)) * 100)
                    logger.info("=" * 50)
                    logger.info("[Overall: %d%%] [Container '%s': %d%%] [Step: %d] Starting action: %s", 
                              overall_pct, container_cfg.name, container_pct, plan.current_action_step, action.description)
                    logger.info("=" * 50)
                else:
                    logger.info("[%d/%d] Running action: %s", idx, len(actions), action.description)
                try:
                    if not action.execute():
                        current_step = plan.current_action_step if plan else idx
                        logger.error("=" * 50)
                        logger.error("Action Execution Failed")
                        logger.error("=" * 50)
                        logger.error("Container: %s", container_cfg.name)
                        logger.error("Step: %d", current_step)
                        logger.error("Action: %s", action.description)
                        logger.error("=" * 50)
                        return (None, None)
                    logger.info("[%d/%d] Action '%s' completed successfully", idx, len(actions), action.description)
                except Exception as exc:
                    current_step = plan.current_action_step if plan else idx
                    logger.error("=" * 50)
                    logger.error("Action Execution Exception")
                    logger.error("=" * 50)
                    logger.error("Container: %s", container_cfg.name)
                    logger.error("Step: %d", current_step)
                    logger.error("Action: %s", action.description)
                    logger.error("Error: %s", exc)
                    logger.error("=" * 50)
                    logger.error("Exception details:", exc_info=True)
                    return (None, None)
            logger.info("Container '%s' created successfully", container_cfg.name)
            return (ssh_service, apt_service)
        except Exception as exc:
            logger.error("Exception in container setup: %s", exc, exc_info=True)
            ssh_service.disconnect()
            return (None, None)

    def _setup_container(self, container_cfg, cfg, plan=None) -> bool:
        """
        Setup container using PCTService - internal method
        Args:
            container_cfg: ContainerConfig instance
            cfg: LabConfig instance
            plan: Optional deployment plan
        Returns:
            True if successful, False otherwise
        """
        from libs.config import ContainerResources
        from libs.common import container_exists, destroy_container
        from services import TemplateService
        from cli import FileOps, User
        import shlex
        
        container_id = str(container_cfg.id)
        ip_address = container_cfg.ip_address
        hostname = container_cfg.hostname
        gateway = cfg.gateway
        
        # Determine template to use
        if container_cfg.template == "base" or not container_cfg.template:
            template_name = None  # None means use base template
        else:
            template_name = container_cfg.template
        
        # Destroy if exists (only if start_step == 1)
        self.destroy(container_id, force=True)
        
        # Get template path
        template_service = TemplateService(self.lxc)
        template_path = template_service.get_template_path(template_name, cfg)
        # Validate template file exists and is readable
        if not template_service.validate_template(template_path):
            logger.error("Template file %s is missing or not readable", template_path)
            base_template = template_service.get_base_template(cfg)
            template_path = f"{cfg.proxmox_template_dir}/{base_template}"
            logger.warning("Falling back to base template: %s", template_path)
        
        # Check if container already exists
        logger.info("Checking if container %s already exists...", container_id)
        container_already_exists = container_exists(cfg.proxmox_host, container_id, cfg=cfg)
        
        # Only destroy and recreate if:
        # 1. Container doesn't exist, OR
        # 2. We're starting from step 1 (full creation)
        if not container_already_exists or (plan and plan.start_step == 1):
            if container_already_exists:
                destroy_container(cfg.proxmox_host, container_id, cfg=cfg, lxc_service=self.lxc)
        
        # Get container resources
        resources = container_cfg.resources
        if not resources:
            resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)
        
        # Determine if container should be privileged from config (default to False if not specified)
        should_be_privileged = container_cfg.privileged if container_cfg.privileged is not None else False
        should_be_nested = container_cfg.nested if container_cfg.nested is not None else True
        unprivileged = not should_be_privileged
        
        # Create container only if it doesn't exist or we're starting from step 1
        if not container_already_exists or (plan and plan.start_step == 1):
            logger.info("Creating container %s from template...", container_id)
            output, exit_code = self.create(
                container_id=container_id,
                template_path=template_path,
                hostname=hostname,
                memory=resources.memory,
                swap=resources.swap,
                cores=resources.cores,
                ip_address=ip_address,
                gateway=gateway,
                bridge=cfg.proxmox_bridge,
                storage=cfg.proxmox_storage,
                rootfs_size=resources.rootfs_size,
                unprivileged=unprivileged,
                ostype="ubuntu",
                arch="amd64",
            )
            if exit_code is not None and exit_code != 0:
                current_step = plan.current_action_step if plan else 0
                logger.error("=" * 50)
                logger.error("Container Creation Failed")
                logger.error("=" * 50)
                logger.error("Container: %s", container_cfg.name)
                logger.error("Step: %d", current_step)
                logger.error("Error: Failed to create container %s: %s", container_id, output)
                logger.error("=" * 50)
                return False
        else:
            logger.info("Container %s already exists, skipping creation", container_id)
            # Verify container is running
            status_output, _ = self.status(container_id)
            if status_output and "running" not in status_output:
                logger.info("Starting existing container %s...", container_id)
                self.start(container_id)
                time.sleep(3)
            # Bring up network interface (it may be DOWN when container is restarted)
            logger.info("Bringing up network interface...")
            ping_cmd = "ping -c 1 8.8.8.8"
            output, exit_code = self.execute(container_id, ping_cmd, timeout=10)
            if exit_code is not None and exit_code != 0:
                logger.warning("Ping to 8.8.8.8 failed (network may still be initializing): %s", output)
            else:
                logger.info("Network interface is up and reachable")
            # For existing containers, ensure SSH is properly configured before connecting
            default_user = cfg.users.default_user
            # Setup SSH key (in case it's missing or changed)
            if not self.setup_ssh_key(container_id, ip_address, cfg):
                logger.error("Failed to setup SSH key for existing container %s", container_id)
                return False
            # Ensure SSH service is installed and running
            if not self.ensure_ssh_service_running(container_id, cfg):
                logger.error("Failed to ensure SSH service is running for existing container %s", container_id)
                return False
            # Wait for container to be ready with SSH connectivity
            logger.info("Waiting for container %s to be ready with SSH connectivity (up to 10 minutes)...", container_id)
            if not self.wait_for_container(container_id, ip_address, cfg, username=default_user):
                logger.error("Container %s did not become ready within 10 minutes", container_id)
                return False
            # Skip to actions - container is already set up
            logger.info("Container %s is ready, proceeding to actions", container_id)
            return True
        
        # Set container features BEFORE starting (use nested from config)
        logger.info("Setting container features...")
        output, exit_code = self.set_features(container_id, nesting=should_be_nested, keyctl=True, fuse=True)
        if exit_code is not None and exit_code != 0:
            logger.warning("Failed to set container features: %s", output)
        
        # Start container
        logger.info("Starting container %s...", container_id)
        output, exit_code = self.start(container_id)
        if exit_code is not None and exit_code != 0:
            current_step = plan.current_action_step if plan else 0
            logger.error("=" * 50)
            logger.error("Container Start Failed")
            logger.error("=" * 50)
            logger.error("Container: %s", container_cfg.name)
            logger.error("Step: %d", current_step)
            logger.error("Error: Failed to start container %s: %s", container_id, output)
            logger.error("=" * 50)
            return False
        
        # Bring up networking by pinging external host
        logger.info("Bringing up network interface...")
        ping_cmd = "ping -c 1 8.8.8.8"
        output, exit_code = self.execute(container_id, ping_cmd, timeout=10)
        if exit_code is not None and exit_code != 0:
            logger.warning("Ping to 8.8.8.8 failed (network may still be initializing): %s", output)
        else:
            logger.info("Network interface is up and reachable")
        
        # Setup users and SSH before waiting
        for user_cfg in cfg.users.users:
            username = user_cfg.name
            sudo_group = user_cfg.sudo_group
            # Create user if it doesn't exist
            check_cmd = User().username(username).check_exists()
            add_cmd = User().username(username).shell("/bin/bash").groups([sudo_group]).create_home(True).add()
            user_check_cmd = f"{check_cmd} 2>&1 || {add_cmd}"
            output, exit_code = self.execute(container_id, user_check_cmd)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to create user %s: %s", username, output)
                return False
            # Set password if provided
            if user_cfg.password:
                password_cmd = f"echo {shlex.quote(f'{username}:{user_cfg.password}')} | chpasswd"
                output, exit_code = self.execute(container_id, password_cmd)
                if exit_code is not None and exit_code != 0:
                    logger.error("Failed to set password for user %s: %s", username, output)
                    return False
                logger.info("Password set for user %s", username)
            # Configure passwordless sudo
            sudoers_path = f"/etc/sudoers.d/{username}"
            sudoers_content = f"{username} ALL=(ALL) NOPASSWD: ALL\n"
            sudoers_write_cmd = FileOps().write(sudoers_path, sudoers_content)
            output, exit_code = self.execute(container_id, sudoers_write_cmd)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to write sudoers file for user %s: %s", username, output)
                return False
            sudoers_chmod_cmd = FileOps().chmod(sudoers_path, "440")
            output, exit_code = self.execute(container_id, sudoers_chmod_cmd)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to secure sudoers file for user %s: %s", username, output)
                return False
        
        # Use first user for SSH setup
        default_user = cfg.users.default_user
        # Setup SSH key
        if not self.setup_ssh_key(container_id, ip_address, cfg):
            logger.error("Failed to setup SSH key")
            return False
        # Ensure SSH service is installed and running
        if not self.ensure_ssh_service_running(container_id, cfg):
            logger.error("Failed to ensure SSH service is running")
            return False
        # Wait for container to be ready (includes SSH connectivity verification, up to 10 min)
        logger.info("Waiting for container to be ready with SSH connectivity (up to 10 minutes)...")
        if not self.wait_for_container(container_id, ip_address, cfg, username=default_user):
            logger.error("Container %s did not become ready within 10 minutes", container_id)
            return False
        return True