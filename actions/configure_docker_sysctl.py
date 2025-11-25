"""
Configure sysctl for Docker action
"""
import logging
from .base import Action
logger = logging.getLogger(__name__)

class ConfigureDockerSysctlAction(Action):
    """Action to configure sysctl for Docker containers"""
    description = "docker sysctl configuration"

    def execute(self) -> bool:
        """Configure sysctl for Docker"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        logger.info("Configuring sysctl for Docker containers...")
        sysctl_cmd = (
            "sysctl -w net.ipv4.ip_unprivileged_port_start=0 2>/dev/null || true; "
            "echo 'net.ipv4.ip_unprivileged_port_start=0' >> /etc/sysctl.conf 2>/dev/null || true"
        )
        output, exit_code = self.ssh_service.execute(sysctl_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.warning("Sysctl configuration had issues: %s", output[-200:] if output else "No output")
        return True

