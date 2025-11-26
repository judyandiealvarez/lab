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
        # Pre-configure debconf to prevent interactive prompts
        debconf_selections = (
            "apt-cacher-ng apt-cacher-ng/tunnelenable boolean false\n"
            "apt-cacher-ng apt-cacher-ng/bindaddress string 0.0.0.0\n"
        )
        debconf_cmd = f"echo '{debconf_selections}' | debconf-set-selections"
        debconf_output, debconf_exit = self.ssh_service.execute(debconf_cmd, sudo=True)
        if debconf_exit != 0:
            logger.warning("Failed to set debconf selections: %s", debconf_output)
        # Disable preconfiguration entirely and use dpkg options to prevent prompts
        dpkg_options = {
            "Dpkg::Options::": "--force-confdef --force-confold",
            "DPkg::Pre-Install-Pkgs": "",
        }
        # Use apt-get instead of apt for better non-interactive support
        install_cmd = Apt().use_apt_get().options(dpkg_options).install(["apt-cacher-ng"])
        output = self.apt_service.execute(f"DEBIAN_PRIORITY=critical DEBIAN_FRONTEND=noninteractive {install_cmd} < /dev/null")
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

