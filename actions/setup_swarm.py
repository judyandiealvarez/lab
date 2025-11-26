"""
Action to setup Docker Swarm
"""
import logging
from orchestration import deploy_swarm
from .base import Action

logger = logging.getLogger(__name__)


class SetupSwarmAction(Action):
    """Setup Docker Swarm"""
    description = "setup swarm"

    def execute(self) -> bool:
        """Setup Docker Swarm"""
        if not self.cfg:
            logger.error("Lab config is missing")
            return False

        if not deploy_swarm(self.cfg):
            logger.error("Docker Swarm deployment failed")
            return False

        logger.info("Docker Swarm setup completed successfully")
        return True

