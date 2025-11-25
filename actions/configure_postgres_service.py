"""
Configure PostgreSQL service action
"""
import logging
import time
from cli import SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class ConfigurePostgresServiceAction(Action):
    """Action to configure and start PostgreSQL service"""
    description = "postgresql service configuration"

    def execute(self) -> bool:
        """Configure and start PostgreSQL service"""
        if not self.ssh_service or not self.cfg:
            logger.error("SSH service or config not initialized")
            return False
        # Get PostgreSQL version from container params
        version = "17"
        if hasattr(self, "container_cfg") and self.container_cfg:
            params = self.container_cfg.params or {}
            version = str(params.get("version", "17"))
        # Start the cluster service (postgresql@VERSION-main)
        cluster_service = f"postgresql@{version}-main"
        start_cmd = SystemCtl().service(cluster_service).enable_and_start()
        output, exit_code = self.ssh_service.execute(start_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("start postgresql cluster service failed with exit code %s", exit_code)
            if output:
                logger.error("start postgresql cluster service output: %s", output.splitlines()[-1])
            return False
        time.sleep(self.cfg.waits.service_start)
        is_active_cmd = SystemCtl().service(cluster_service).is_active()
        status, exit_code = self.ssh_service.execute(is_active_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            # Verify PostgreSQL is listening on all interfaces
            port_check_cmd = "ss -tlnp | grep :5432 | grep -v '127.0.0.1' || echo 'not_listening'"
            port_output, _ = self.ssh_service.execute(port_check_cmd, sudo=True)
            if "not_listening" not in port_output and ":5432" in port_output:
                logger.info("PostgreSQL is listening on all interfaces")
            else:
                logger.warning("PostgreSQL may not be listening on external interfaces")
            return True
        logger.error("PostgreSQL cluster service is not active")
        return False

