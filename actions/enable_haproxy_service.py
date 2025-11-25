"""
Enable HAProxy service action
"""
import logging
from cli import SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class EnableHaproxyServiceAction(Action):
    """Action to enable and start HAProxy service"""
    description = "haproxy service enablement"

    def execute(self) -> bool:
        """Enable and start HAProxy service"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        # Validate config before starting
        validate_cmd = "haproxy -c -f /etc/haproxy/haproxy.cfg 2>&1"
        validate_output, exit_code = self.ssh_service.execute(validate_cmd, sudo=True)
        if exit_code is None or exit_code != 0:
            logger.error("HAProxy config validation command failed")
            return False
        if validate_output and ("Fatal errors found" in validate_output or "[ALERT]" in validate_output):
            logger.error("HAProxy config validation failed: %s", validate_output)
            return False
        # Restart HAProxy to apply configuration changes (restart handles enable/start if not running)
        restart_cmd = SystemCtl().service("haproxy").restart()
        output, exit_code = self.ssh_service.execute(restart_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("restart haproxy service failed with exit code %s", exit_code)
            if output:
                logger.error("restart haproxy service output: %s", output.splitlines()[-1])
            # Get detailed error from systemctl
            status_cmd = "systemctl status haproxy.service --no-pager -l 2>&1 | head -20"
            status_output, _ = self.ssh_service.execute(status_cmd, sudo=True)
            logger.error("HAProxy service restart failed. Status: %s", status_output)
            return False
        # Also ensure it's enabled
        enable_cmd = SystemCtl().service("haproxy").enable()
        self.ssh_service.execute(enable_cmd, sudo=True)
        # Wait a moment for service to fully start
        import time
        time.sleep(2)
        status_cmd = SystemCtl().service("haproxy").is_active()
        status, exit_code = self.ssh_service.execute(status_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            return True
        logger.error("HAProxy service is not active after restart")
        return False

