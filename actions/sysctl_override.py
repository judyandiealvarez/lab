"""
Systemd sysctl override action
"""
import logging
from cli import FileOps
from .base import Action
logger = logging.getLogger(__name__)

class SysctlOverrideAction(Action):
    """Action to configure systemd sysctl override"""
    description = "systemd sysctl override"

    def execute(self) -> bool:
        """Configure systemd sysctl override"""
        if not self.pct_service or not self.container_id:
            logger.error("PCT service or container ID not available")
            return False
        mkdir_cmd = FileOps().mkdir("/etc/systemd/system/systemd-sysctl.service.d", parents=True)
        output, exit_code = self.pct_service.execute(self.container_id, mkdir_cmd)
        if exit_code is not None and exit_code != 0:
            logger.error("create sysctl override directory failed with exit code %s", exit_code)
            if output:
                logger.error("create sysctl override directory output: %s", output.splitlines()[-1])
            return False
        override_cmd = FileOps().write(
            "/etc/systemd/system/systemd-sysctl.service.d/override.conf",
            "[Service]\nImportCredential=\n",
        )
        output, exit_code = self.pct_service.execute(self.container_id, override_cmd)
        if exit_code is not None and exit_code != 0:
            logger.error("write sysctl override failed with exit code %s", exit_code)
            if output:
                logger.error("write sysctl override output: %s", output.splitlines()[-1])
            return False
        reload_cmd = (
            "systemctl daemon-reload && "
            "systemctl stop systemd-sysctl.service 2>/dev/null || true && "
            "systemctl start systemd-sysctl.service 2>/dev/null || true"
        )
        output, exit_code = self.pct_service.execute(self.container_id, reload_cmd)
        if exit_code is not None and exit_code != 0:
            logger.error("reload systemd-sysctl failed with exit code %s", exit_code)
            if output:
                logger.error("reload systemd-sysctl output: %s", output.splitlines()[-1])
            return False
        return True

