"""
Fix apt sources action
"""
import logging
from .base import Action
from cli import Sed
logger = logging.getLogger(__name__)

class FixAptSourcesAction(Action):
    """Fix apt sources (replace oracular with plucky, old-releases with archive)"""
    description = "apt sources fix"

    def execute(self) -> bool:
        """Fix apt sources"""
        sed_cmds = [
            Sed().replace("/etc/apt/sources.list", "oracular", "plucky"),
            Sed().replace("/etc/apt/sources.list", "old-releases.ubuntu.com", "archive.ubuntu.com")
        ]
        all_succeeded = True
        for sed_cmd in sed_cmds:
            output, exit_code = self.pct_service.execute(self.container_id, sed_cmd)
            if exit_code is not None and exit_code != 0:
                # If file doesn't exist, that's OK (Ubuntu 25.04 might use sources.list.d)
                if "No such file or directory" in output or "can't read" in output:
                    logger.info("sources.list not found (may use sources.list.d), skipping fix")
                else:
                    logger.error("Failed to fix apt sources: %s", output)
                    all_succeeded = False
        return all_succeeded

