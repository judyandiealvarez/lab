"""
Disable systemd-resolved action to free port 53 for DNS server
"""
import logging
from cli import FileOps, SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class DisableSystemdResolvedAction(Action):
    """Action to disable systemd-resolved and configure resolv.conf"""
    description = "disable systemd resolved"

    def execute(self) -> bool:
        """Disable systemd-resolved and configure resolv.conf"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        logger.info("Disabling systemd-resolved to free port 53...")
        # Stop and disable systemd-resolved
        stop_cmd = SystemCtl().service("systemd-resolved").stop()
        output, exit_code = self.ssh_service.execute(stop_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.warning("Failed to stop systemd-resolved: %s", output)
        disable_cmd = SystemCtl().service("systemd-resolved").disable()
        output, exit_code = self.ssh_service.execute(disable_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.warning("Failed to disable systemd-resolved: %s", output)
        # Remove symlink and create static resolv.conf
        logger.info("Configuring static resolv.conf...")
        remove_symlink_cmd = "rm -f /etc/resolv.conf"
        self.ssh_service.execute(remove_symlink_cmd, sudo=True)
        resolv_content = "nameserver 8.8.8.8\nnameserver 1.1.1.1\nnameserver 8.8.4.4\n"
        write_resolv_cmd = FileOps().write("/etc/resolv.conf", resolv_content)
        output, exit_code = self.ssh_service.execute(write_resolv_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to write resolv.conf: %s", output)
            return False
        # Verify systemd-resolved is stopped
        status_cmd = SystemCtl().service("systemd-resolved").is_active()
        status, exit_code = self.ssh_service.execute(status_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            logger.warning("systemd-resolved is still active")
        else:
            logger.info("systemd-resolved is stopped and disabled")
        return True

