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
        logger.info("Running in container %s: %s", container_id, command)
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