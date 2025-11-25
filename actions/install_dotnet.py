"""
Install .NET SDK action
"""
import logging
from cli import Apt
from .base import Action
logger = logging.getLogger(__name__)

class InstallDotnetAction(Action):
    """Action to install .NET SDK"""
    description = "dotnet installation"

    def execute(self) -> bool:
        """Install .NET SDK"""
        if not self.apt_service:
            logger.error("APT service not initialized")
            return False
        logger.info("Installing .NET SDK...")
        install_cmd = Apt().install(["dotnet-sdk-8.0"])
        output = self.apt_service.execute(install_cmd)
        if output is None:
            logger.error("dotnet SDK installation failed")
            return False
        return True

