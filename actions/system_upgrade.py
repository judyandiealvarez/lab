"""
System upgrade action
"""
import logging
from cli import Apt, CommandWrapper
from .base import Action
logger = logging.getLogger(__name__)

class SystemUpgradeAction(Action):
    """Action to run system upgrade"""
    description = "system upgrade"

    def execute(self) -> bool:
        """Run system upgrade"""
        if not self.apt_service:
            logger.error("APT service not initialized")
            return False
        # Run apt update
        update_cmd = Apt().quiet().update()
        output = self.apt_service.execute(update_cmd)
        if output is None:
            logger.error("apt update failed due to apt lock contention")
            return False
        # Check if output contains actual error indicators, not just warnings
        result = CommandWrapper.parse_result(output)
        if result.has_error:
            error_msg_lower = (result.error_message or "").lower()
            output_lower = (output or "").lower()
            success_indicators = (
                "setting up" in output_lower[-1000:]
                or "processing triggers" in output_lower[-1000:]
                or "created symlink" in output_lower[-1000:]
                or "0 upgraded" in output_lower[-1000:]
                or "0 newly installed" in output_lower[-1000:]
            )
            logger_failure = "returned error: 256" in error_msg_lower and (
                "logger:" in output_lower or "logging to syslog failed" in output_lower
            )
            apparmor_warning = (
                "apparmor_parser" in output_lower
                and "access denied" in output_lower
                and "policy admin privileges" in output_lower
            )
            if (logger_failure or apparmor_warning) and success_indicators:
                logger.warning(
                    "apt update reported warnings (logger/syslog or AppArmor parser) but "
                    "package operation succeeded, treating as success"
                )
            else:
                logger.error("apt update failed: %s - %s", result.error_type.value, result.error_message)
                if output:
                    logger.error("Full command output (last 1000 chars): %s", output[-1000:])
                return False
        # Run distribution upgrade (interactive, no quiet mode)
        # Add Dpkg options to prevent interactive prompts and handle stdin issues
        # Use force-confdef and force-confold to automatically handle configuration file prompts
        dpkg_options = {
            "Dpkg::Options::": "--force-confdef --force-confold",
        }
        upgrade_cmd = Apt().dist_upgrade().options(dpkg_options).upgrade()
        # Execute with DEBIAN_PRIORITY=critical to skip preconfiguration that requires stdin
        # This prevents dpkg-preconfigure from trying to read from stdin
        full_cmd = f"DEBIAN_PRIORITY=critical {upgrade_cmd}"
        output = self.apt_service.execute(full_cmd)
        if output is None:
            logger.error("distribution upgrade failed due to apt lock contention")
            return False
        # Check if output contains actual error indicators, not just warnings
        result = CommandWrapper.parse_result(output)
        if result.has_error:
            error_msg_lower = (result.error_message or "").lower()
            output_lower = (output or "").lower()
            success_indicators = (
                "setting up" in output_lower[-1000:]
                or "processing triggers" in output_lower[-1000:]
                or "created symlink" in output_lower[-1000:]
                or "0 upgraded" in output_lower[-1000:]
                or "0 newly installed" in output_lower[-1000:]
            )
            logger_failure = "returned error: 256" in error_msg_lower and (
                "logger:" in output_lower or "logging to syslog failed" in output_lower
            )
            apparmor_warning = (
                "apparmor_parser" in output_lower
                and "access denied" in output_lower
                and "policy admin privileges" in output_lower
            )
            if (logger_failure or apparmor_warning) and success_indicators:
                logger.warning(
                    "distribution upgrade reported warnings (logger/syslog or AppArmor parser) but "
                    "package operation succeeded, treating as success"
                )
                return True
            logger.error("distribution upgrade failed: %s - %s", result.error_type.value, result.error_message)
            if output:
                logger.error("Full command output (last 1000 chars): %s", output[-1000:])
            return False
        return True

