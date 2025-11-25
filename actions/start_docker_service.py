"""
Start Docker service action
"""
import logging
import time
from cli import SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class StartDockerServiceAction(Action):
    """Action to start Docker service"""
    description = "docker service start"

    def execute(self) -> bool:
        """Start Docker service"""
        if not self.ssh_service or not self.cfg:
            logger.error("SSH service or config not initialized")
            return False
        logger.info("Ensuring Docker service is running...")
        # If service already active, skip start
        is_active_cmd = SystemCtl().service("docker").is_active()
        current_status, current_exit = self.ssh_service.execute(is_active_cmd, sudo=True)
        if current_exit == 0 and SystemCtl.parse_is_active(current_status):
            logger.info("Docker service already active, skipping start")
            return True
        logger.info("Docker service not active, starting socket and triggering activation...")
        # Docker uses socket activation, so we need to start docker.socket first
        socket_cmd = SystemCtl().service("docker.socket").enable_and_start()
        socket_output, socket_exit = self.ssh_service.execute(socket_cmd, sudo=True)
        if socket_exit is not None and socket_exit != 0:
            logger.error("Failed to start docker.socket with exit code %s", socket_exit)
            if socket_output:
                logger.error("docker.socket start output: %s", socket_output[-500:])
            return False
        # Wait for socket to be ready
        time.sleep(2)
        # Enable docker.service (but don't start it - socket activation will handle that)
        enable_cmd = SystemCtl().service("docker").enable()
        enable_output, enable_exit = self.ssh_service.execute(enable_cmd, sudo=True)
        if enable_exit is not None and enable_exit != 0:
            logger.warning("Failed to enable docker.service: %s", enable_output)
        # Trigger docker.service by making a connection to the socket
        # This will cause systemd to start docker.service via socket activation
        logger.info("Triggering Docker service via socket activation...")
        # Use docker version to trigger socket activation (this will fail if service isn't ready, but that's OK - it triggers activation)
        trigger_cmd = "docker version >/dev/null 2>&1 || true"
        trigger_output, trigger_exit = self.ssh_service.execute(trigger_cmd, sudo=True)
        # Wait for service to be activated
        time.sleep(3)
        # Verify Docker service is running
        is_active_cmd = SystemCtl().service("docker").is_active()
        status, exit_code = self.ssh_service.execute(is_active_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            logger.info("Docker service is running")
            return True
        logger.error("Docker service failed to start via socket activation")
        # Get detailed error from systemctl status
        status_cmd = SystemCtl().service("docker").status()
        status_output, _ = self.ssh_service.execute(status_cmd, sudo=True)
        logger.error("Docker service status:\n%s", status_output)
        # Get detailed error from journal
        journal_cmd = "journalctl -u docker.service -n 50 --no-pager"
        journal_output, _ = self.ssh_service.execute(journal_cmd, sudo=True)
        logger.error("Docker service journal logs:\n%s", journal_output)
        return False

