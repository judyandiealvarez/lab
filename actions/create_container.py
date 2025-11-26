"""
Action to create a container without executing actions
"""
import logging
import time
from cli import FileOps, User
from services import LXCService, PCTService, SSHService, TemplateService
from libs.container import container_exists, destroy_container
from .base import Action

logger = logging.getLogger(__name__)


class CreateContainerAction(Action):
    """Create container without executing actions"""
    description = "create container"

    def execute(self) -> bool:
        """Create container using PCTService"""
        if not self.container_cfg or not self.cfg:
            logger.error("Container config or lab config is missing")
            return False

        proxmox_host = self.cfg.proxmox_host
        container_id = str(self.container_cfg.id)
        ip_address = self.container_cfg.ip_address
        hostname = self.container_cfg.hostname
        gateway = self.cfg.gateway

        # Determine template to use
        if self.container_cfg.template == "base" or not self.container_cfg.template:
            template_name = None  # None means use base template
        else:
            template_name = self.container_cfg.template

        # Create LXC service for proxmox host
        lxc_service = LXCService(proxmox_host, self.cfg.ssh)
        if not lxc_service.connect():
            logger.error("Failed to connect to Proxmox host %s", proxmox_host)
            return False

        try:
            # Create services
            pct_service = PCTService(lxc_service)
            template_service = TemplateService(lxc_service)

            # Destroy if exists
            pct_service.destroy(container_id, force=True)

            # Get template path
            template_path = template_service.get_template_path(template_name, self.cfg)

            # Validate template file exists and is readable
            if not template_service.validate_template(template_path):
                logger.error("Template file %s is missing or not readable", template_path)
                base_template = template_service.get_base_template(self.cfg)
                template_path = f"{self.cfg.proxmox_template_dir}/{base_template}"
                logger.warning("Falling back to base template: %s", template_path)

            # Check if container already exists
            logger.info("Checking if container %s already exists...", container_id)
            container_already_exists = container_exists(proxmox_host, container_id, cfg=self.cfg)

            if container_already_exists:
                logger.info("Container %s already exists, destroying it first...", container_id)
                destroy_container(proxmox_host, container_id, cfg=self.cfg, lxc_service=lxc_service)

            # Get container resources
            resources = self.container_cfg.resources
            if not resources:
                from libs.config import ContainerResources
                resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)

            # Determine if container should be privileged
            is_docker_template = self.container_cfg.template and "docker" in self.container_cfg.template.lower()
            unprivileged = (
                self.container_cfg.type not in ("swarm-manager", "swarm-node")
                and not is_docker_template
            )

            # Create container
            logger.info("Creating container %s from template...", container_id)
            output, exit_code = pct_service.create(
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
                logger.error("Failed to create container %s: %s", container_id, output)
                return False

            # Set container features BEFORE starting
            logger.info("Setting container features...")
            output, exit_code = pct_service.set_features(container_id, nesting=True, keyctl=True, fuse=True)
            if exit_code is not None and exit_code != 0:
                logger.warning("Failed to set container features: %s", output)

            # Start container
            logger.info("Starting container %s...", container_id)
            output, exit_code = pct_service.start(container_id)
            if exit_code is not None and exit_code != 0:
                logger.error("Failed to start container %s: %s", container_id, output)
                return False

            # Bring up networking by pinging external host
            logger.info("Bringing up network interface...")
            ping_cmd = "ping -c 1 8.8.8.8"
            output, exit_code = pct_service.execute(container_id, ping_cmd, timeout=10)
            if exit_code is not None and exit_code != 0:
                logger.warning("Ping to 8.8.8.8 failed (network may still be initializing): %s", output)
            else:
                logger.info("Network interface is up and reachable")

            # Setup users and SSH
            import shlex
            for user_cfg in self.cfg.users.users:
                username = user_cfg.name
                sudo_group = user_cfg.sudo_group

                # Create user if it doesn't exist
                check_cmd = User().username(username).check_exists()
                add_cmd = User().username(username).shell("/bin/bash").groups([sudo_group]).create_home(True).add()
                user_check_cmd = f"{check_cmd} 2>&1 || {add_cmd}"
                output, exit_code = pct_service.execute(container_id, user_check_cmd)
                if exit_code is not None and exit_code != 0:
                    logger.error("Failed to create user %s: %s", username, output)
                    return False

                # Set password if provided
                if user_cfg.password:
                    password_cmd = f"echo {shlex.quote(f'{username}:{user_cfg.password}')} | chpasswd"
                    output, exit_code = pct_service.execute(container_id, password_cmd)
                    if exit_code is not None and exit_code != 0:
                        logger.error("Failed to set password for user %s: %s", username, output)
                        return False
                    logger.info("Password set for user %s", username)

                # Configure passwordless sudo
                sudoers_path = f"/etc/sudoers.d/{username}"
                sudoers_content = f"{username} ALL=(ALL) NOPASSWD: ALL\n"
                sudoers_write_cmd = FileOps().write(sudoers_path, sudoers_content)
                output, exit_code = pct_service.execute(container_id, sudoers_write_cmd)
                if exit_code is not None and exit_code != 0:
                    logger.error("Failed to write sudoers file for user %s: %s", username, output)
                    return False

                sudoers_chmod_cmd = FileOps().chmod(sudoers_path, "440")
                output, exit_code = pct_service.execute(container_id, sudoers_chmod_cmd)
                if exit_code is not None and exit_code != 0:
                    logger.error("Failed to secure sudoers file for user %s: %s", username, output)
                    return False

            # Use first user for SSH setup
            default_user = self.cfg.users.default_user

            # Setup SSH key
            if not pct_service.setup_ssh_key(container_id, ip_address, self.cfg):
                logger.error("Failed to setup SSH key")
                return False

            # Ensure SSH service is installed and running
            if not pct_service.ensure_ssh_service_running(container_id, self.cfg):
                logger.error("Failed to ensure SSH service is running")
                return False

            # Wait for container to be ready (includes SSH connectivity verification, up to 10 min)
            logger.info("Waiting for container to be ready with SSH connectivity (up to 10 minutes)...")
            if not pct_service.wait_for_container(container_id, ip_address, self.cfg, username=default_user):
                logger.error("Container %s did not become ready within 10 minutes", container_id)
                return False

            logger.info("Container %s created successfully", container_id)
            return True

        finally:
            lxc_service.disconnect()

