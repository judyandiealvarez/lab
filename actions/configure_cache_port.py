"""
Configure apt-cacher-ng port action
"""
import logging
import time
from cli import Sed, FileOps, SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class ConfigureCachePortAction(Action):
    """Action to configure apt-cacher-ng port"""
    description = "apt-cacher-ng port configuration"

    def execute(self) -> bool:
        """Configure apt-cacher-ng port"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        port = self.cfg.apt_cache_port
        config_file = "/etc/apt-cacher-ng/acng.conf"
        # First, try to uncomment and replace if Port line is commented
        uncomment_cmd = Sed().flags("").replace(config_file, "^#\\s*Port:.*", f"Port: {port}")
        output, exit_code = self.ssh_service.execute(uncomment_cmd, sudo=True)
        # If that didn't work, try replacing uncommented Port line
        if exit_code is not None and exit_code != 0:
            replace_cmd = Sed().flags("").replace(config_file, "^Port:.*", f"Port: {port}")
            output, exit_code = self.ssh_service.execute(replace_cmd, sudo=True)
        # If still no match, append the Port line
        if exit_code is not None and exit_code != 0:
            append_cmd = FileOps().append().write(config_file, f"Port: {port}\n")
            output, exit_code = self.ssh_service.execute(append_cmd, sudo=True)
            if exit_code is not None and exit_code != 0:
                logger.error("append apt-cacher-ng port failed with exit code %s", exit_code)
                return False
        # Configure timeout and retry settings to handle slow connections
        logger.info("Configuring apt-cacher-ng timeout and retry settings...")
        timeout_settings = [
            ("DlMaxRetries", "5"),
            ("NetworkTimeout", "120"),
            ("DisconnectTimeout", "30"),
        ]
        for setting_name, setting_value in timeout_settings:
            # Check if setting already exists (commented or not)
            check_cmd = f"grep -E '^#?{setting_name}:' {config_file} || echo 'not_found'"
            check_output, _ = self.ssh_service.execute(check_cmd, sudo=True)
            if "not_found" in check_output:
                # Append new setting
                append_cmd = FileOps().append().write(config_file, f"{setting_name}: {setting_value}\n")
                output, exit_code = self.ssh_service.execute(append_cmd, sudo=True)
                if exit_code is not None and exit_code != 0:
                    logger.warning("Failed to add %s: %s", setting_name, output)
            else:
                # Replace existing setting (commented or not)
                replace_cmd = Sed().flags("").replace(config_file, f"^#?{setting_name}:.*", f"{setting_name}: {setting_value}")
                output, exit_code = self.ssh_service.execute(replace_cmd, sudo=True)
                if exit_code is not None and exit_code != 0:
                    logger.warning("Failed to update %s: %s", setting_name, output)
        # Restart service to apply configuration changes
        logger.info("Restarting apt-cacher-ng service to apply configuration changes...")
        restart_cmd = SystemCtl().service("apt-cacher-ng").restart()
        output, exit_code = self.ssh_service.execute(restart_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("restart apt-cacher-ng service failed with exit code %s", exit_code)
            if output:
                logger.error("restart output: %s", output.splitlines()[-1])
            return False
        # Wait for service to be ready
        time.sleep(2)
        # Verify service is active after restart
        is_active_cmd = SystemCtl().service("apt-cacher-ng").is_active()
        status, exit_code = self.ssh_service.execute(is_active_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            return True
        logger.error("apt-cacher-ng service is not active after restart")
        return False

