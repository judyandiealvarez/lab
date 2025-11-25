"""
Install apt-cacher-ng action
"""
import logging
from cli import Apt, AptCommands
from .base import Action
logger = logging.getLogger(__name__)

class InstallAptCacherAction(Action):
    """Action to install apt-cacher-ng package"""
    description = "apt-cacher-ng installation"

    def execute(self) -> bool:
        """Install apt-cacher-ng package"""
        if not self.apt_service or not self.ssh_service:
            logger.error("Services not initialized")
            return False
        logger.info("Installing apt-cacher-ng package...")
        install_cmd = Apt().install(["apt-cacher-ng"])
        output = self.apt_service.execute(install_cmd)
        if output is None:
            logger.error("apt-cacher-ng installation failed")
            # Verify if package was actually installed despite error
            check_cmd = AptCommands.command_exists_check_cmd("apt-cacher-ng")
            check_output, exit_code = self.ssh_service.execute(check_cmd)
            if exit_code == 0 and AptCommands.parse_command_exists(check_output):
                logger.warning("apt-cacher-ng binary exists despite installation error, treating as success")
                return True
            return False
        # Verify binary exists
        check_cmd = AptCommands.command_exists_check_cmd("apt-cacher-ng")
        check_output, exit_code = self.ssh_service.execute(check_cmd)
        if exit_code != 0 or not AptCommands.parse_command_exists(check_output):
            logger.error("apt-cacher-ng binary not found after installation")
            return False
        # Verify service unit exists
        service_check_cmd = "systemctl list-unit-files apt-cacher-ng.service 2>&1 | grep -q apt-cacher-ng.service && echo 'exists' || echo 'missing'"
        service_check, exit_code = self.ssh_service.execute(service_check_cmd)
        if exit_code != 0 or not service_check or "exists" not in service_check:
            logger.error("apt-cacher-ng service unit not found after installation. " "Check: %s", service_check)
            # Check if package is actually installed
            dpkg_check = "dpkg -l | grep apt-cacher-ng 2>&1"
            dpkg_output, _ = self.ssh_service.execute(dpkg_check)
            logger.error("dpkg status: %s", dpkg_output)
            return False
        return True

