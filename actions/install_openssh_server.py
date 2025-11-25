"""
Install openssh-server action
"""
from .base import Action
from cli import Apt

class InstallOpensshServerAction(Action):
    """Install openssh-server package"""
    description = "openssh-server installation"

    def execute(self) -> bool:
        """Install openssh-server"""
        install_cmd = Apt().install(["openssh-server"])
        output, exit_code = self.pct_service.execute(self.container_id, install_cmd)
        if exit_code is not None and exit_code != 0:
            self.logger.error("Failed to install openssh-server: %s", output)
            return False
        return True

