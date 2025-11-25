"""
Install PostgreSQL action
"""
import logging
from cli import Apt
from .base import Action
logger = logging.getLogger(__name__)

class InstallPostgresqlAction(Action):
    """Action to install PostgreSQL package"""
    description = "postgresql installation"

    def execute(self) -> bool:
        """Install PostgreSQL package"""
        if not self.apt_service:
            logger.error("APT service not initialized")
            return False
        # Get version from params
        version = "17"
        if hasattr(self, "_container_cfg") and self._container_cfg:
            params = self._container_cfg.params or {}
            version = str(params.get("version", "17"))
        logger.info("Installing PostgreSQL %s package...", version)
        install_cmd = Apt().install([f"postgresql-{version}", "postgresql-contrib"])
        output = self.apt_service.execute(install_cmd)
        if output is None:
            logger.error("PostgreSQL installation failed")
            return False
        return True

