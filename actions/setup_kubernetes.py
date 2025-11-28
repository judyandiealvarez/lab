"""
Setup Kubernetes action
"""
import logging
from orchestration import deploy_kubernetes
from .base import Action

logger = logging.getLogger(__name__)

class SetupKubernetesAction(Action):
    """Action to set up Kubernetes (k3s) cluster"""
    description = "setup kubernetes"

    def execute(self) -> bool:
        """Execute Kubernetes deployment."""
        if not self.cfg:
            logger.error("Lab configuration is missing for SetupKubernetesAction.")
            return False

        logger.info("Deploying Kubernetes (k3s) cluster...")
        if not deploy_kubernetes(self.cfg):
            logger.error("Kubernetes deployment failed.")
            return False

        logger.info("Kubernetes deployment completed successfully.")
        return True

