"""
Disable automatic apt units action
"""
import logging
from .base import Action
logger = logging.getLogger(__name__)

class DisableAptUnitsAction(Action):
    """Action to disable automatic apt units"""
    description = "disable automatic apt units"

    def execute(self) -> bool:
        """Disable automatic apt units"""
        command = (
            "for unit in apt-daily.service apt-daily.timer "
            "apt-daily-upgrade.service apt-daily-upgrade.timer; do "
            'systemctl stop "$unit" 2>/dev/null || true; '
            'systemctl disable "$unit" 2>/dev/null || true; '
            'systemctl mask "$unit" 2>/dev/null || true; '
            "done"
        )
        output, exit_code = self.ssh_service.execute(command, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("disable automatic apt units failed with exit code %s", exit_code)
            if output:
                logger.error("disable automatic apt units output: %s", output.splitlines()[-1])
            return False
        return True

