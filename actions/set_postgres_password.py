"""
Set PostgreSQL password action
"""
import logging
from .base import Action
logger = logging.getLogger(__name__)

class SetPostgresPasswordAction(Action):
    """Action to set PostgreSQL password"""
    description = "postgresql password setup"

    def execute(self) -> bool:
        """Set PostgreSQL password"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        # Get password from params
        password = "postgres"
        if hasattr(self, "container_cfg") and self.container_cfg:
            params = self.container_cfg.params or {}
            password = params.get("password", "postgres")
        # Use local socket connection (requires local entry in pg_hba.conf)
        command = f"sudo -n -u postgres psql -c \"ALTER USER postgres WITH PASSWORD '{password}';\" 2>&1"
        output, exit_code = self.ssh_service.execute(command, sudo=False)
        if exit_code is not None and exit_code != 0:
            logger.error("set postgres password failed with exit code %s", exit_code)
            if output:
                logger.error("set postgres password output: %s", output.splitlines()[-1])
            return False
        return True

