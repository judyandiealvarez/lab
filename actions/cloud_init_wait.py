"""
Cloud-init wait action
"""
import logging
from cli import Command, CloudInit
from .base import Action
logger = logging.getLogger(__name__)

class CloudInitWaitAction(Action):
    """Action to wait for cloud-init to complete"""
    description = "cloud-init wait"

    def execute(self) -> bool:
        """Wait for cloud-init to complete"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False

        # Check if cloud-init exists
        exists_cmd = Command().command("cloud-init").exists()
        exists_output, exit_code = self.ssh_service.execute(exists_cmd, timeout=10)

        # skip if cloud-init is not installed
        if exit_code is None or exit_code != 0:
            logger.info("cloud-init not found, skipping")
            return True

        # Wait for cloud-init to complete
        wait_cmd = CloudInit().log_file("/tmp/cloud-init-wait.log").wait()
        wait_output, wait_exit_code = self.ssh_service.execute(wait_cmd, timeout=180)

        if wait_exit_code is not None and wait_exit_code != 0:
            logger.warning("cloud-init wait failed with exit code %s", wait_exit_code)
            if wait_output:                
                logger.warning("cloud-init wait output: %s", wait_output[-500:])

        # Clean cloud-init logs
        clean_cmd = CloudInit().suppress_output().clean(logs=True)
        clean_output, clean_exit_code = self.ssh_service.execute(clean_cmd, timeout=30)
        if clean_exit_code is not None and clean_exit_code != 0:
            logger.warning("cloud-init clean failed with exit code %s", clean_exit_code)
            if clean_output:
                logger.warning("cloud-init clean output: %s", clean_output[-500:])

        return True

