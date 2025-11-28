"""
Install GlusterFS server action
"""
import logging
from .base import Action
from cli import Apt

logger = logging.getLogger(__name__)


class InstallGlusterfsAction(Action):
    """Install GlusterFS server packages"""
    description = "glusterfs server installation"

    def execute(self) -> bool:
        """Install GlusterFS server and client packages"""
        logger.info("Installing GlusterFS server and client packages...")
        
        # Update apt first
        update_cmd = Apt.update_cmd()
        update_output, exit_code = self.pct_service.execute(
            str(self.container_id), update_cmd, timeout=600
        )
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to update apt: %s", update_output)
            return False
        
        # Install GlusterFS packages
        install_cmd = Apt.install_cmd(["glusterfs-server", "glusterfs-client"])
        install_output, exit_code = self.pct_service.execute(
            str(self.container_id), install_cmd, timeout=300
        )
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to install GlusterFS: %s", install_output)
            return False
        
        # Enable and start glusterd service
        enable_cmd = "systemctl enable glusterd && systemctl start glusterd"
        enable_output, exit_code = self.pct_service.execute(
            str(self.container_id), enable_cmd, timeout=30
        )
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to start glusterd: %s", enable_output)
            return False
        
        logger.info("GlusterFS server installed and started successfully")
        return True

