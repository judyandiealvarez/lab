"""
Enable apt-cacher-ng service action
"""
import logging
import time
from cli import SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class EnableCacheServiceAction(Action):
    """Action to enable and start apt-cacher-ng service"""
    description = "apt-cacher-ng service enablement"

    def execute(self) -> bool:
        """Enable and start apt-cacher-ng service"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        # Enable the service first
        enable_cmd = SystemCtl().service("apt-cacher-ng").enable()
        output, exit_code = self.ssh_service.execute(enable_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.warning("enable apt-cacher-ng service had issues: %s", output)
        # Use restart to ensure config changes are applied (if service was already running)
        restart_cmd = SystemCtl().service("apt-cacher-ng").restart()
        output, exit_code = self.ssh_service.execute(restart_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("restart apt-cacher-ng service failed with exit code %s", exit_code)
            if output:
                logger.error("restart output: %s", output.splitlines()[-1])
            return False
        # Wait a moment for service to start
        time.sleep(2)
        is_active_cmd = SystemCtl().service("apt-cacher-ng").is_active()
        status, exit_code = self.ssh_service.execute(is_active_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            # Verify service stays active
            time.sleep(2)
            status2, exit_code2 = self.ssh_service.execute(is_active_cmd, sudo=True)
            if exit_code2 == 0 and SystemCtl.parse_is_active(status2):
                return True
            # Service started but stopped - check why
            status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1 | head -20"
            status_output, _ = self.ssh_service.execute(status_cmd, sudo=True)
            logger.error("apt-cacher-ng service started but stopped. Status: %s", status_output)
            return False
        # Service didn't start - check why
        status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1 | head -20"
        status_output, _ = self.ssh_service.execute(status_cmd, sudo=True)
        logger.error("apt-cacher-ng service failed to start. Status: %s", status_output)
        return False

