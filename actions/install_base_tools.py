"""
Install base tools action
"""
from .base import Action
from cli import Apt

class InstallBaseToolsAction(Action):
    """Install minimal base tools"""
    description = "base tools installation"

    def execute(self) -> bool:
        """Install base tools"""
        install_cmd = Apt().install(["ca-certificates", "curl"])
        output = self.apt_service.execute(install_cmd)
        if output is None:
            self.logger.error("Failed to install base tools")
            return False
        return True

