"""
Enable and start SSH service action
"""
from .base import Action
from cli import SystemCtl

class EnableSshServiceAction(Action):
    """Enable and start SSH service"""
    description = "SSH service enablement"

    def execute(self) -> bool:
        """Enable and start SSH service"""
        enable_cmd = SystemCtl().service("ssh").enable()
        output, exit_code = self.pct_service.execute(self.container_id, enable_cmd)
        if exit_code is not None and exit_code != 0:
            self.logger.error("Failed to enable SSH service: %s", output)
            return False
        start_cmd = SystemCtl().service("ssh").start()
        output, exit_code = self.pct_service.execute(self.container_id, start_cmd)
        if exit_code is not None and exit_code != 0:
            self.logger.error("Failed to start SSH service: %s", output)
            return False
        return True

