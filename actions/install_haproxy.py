"""
Install HAProxy action
"""
import logging
from cli import Apt
from .base import Action
logger = logging.getLogger(__name__)

class InstallHaproxyAction(Action):
    """Action to install HAProxy package"""
    description = "haproxy installation"

    def execute(self) -> bool:
        """Install HAProxy package"""
        if not self.apt_service:
            logger.error("APT service not initialized")
            return False
        logger.info("Installing haproxy package...")
        install_cmd = Apt().install(["haproxy"])
        output = self.apt_service.execute(install_cmd)
        if output is None:
            logger.error("haproxy installation failed")
            return False
        return True

