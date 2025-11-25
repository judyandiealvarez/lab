"""
Configure LXC sysctl access action
"""
import logging
from .base import Action
logger = logging.getLogger(__name__)

class ConfigureLxcSysctlAccessAction(Action):
    """Action to configure LXC container for sysctl access"""
    description = "lxc sysctl access configuration"

    def execute(self) -> bool:
        """Configure LXC container for sysctl access"""
        if not self.pct_service or not self.container_id:
            logger.error("PCT service or container ID not available")
            return False
        logger.info("Configuring LXC container for sysctl access...")
        # This needs to be done via pct set commands on the Proxmox host
        # These commands need to run on the Proxmox host, not inside the container
        sysctl_device_cmd = f"pct set {self.container_id} -lxc.cgroup2.devices.allow 'c 10:200 rwm' 2>/dev/null || true"
        sysctl_mount_cmd = f"pct set {self.container_id} -lxc.mount.auto 'proc:rw sys:rw' 2>/dev/null || true"
        # Access lxc_service through pct_service
        if hasattr(self.pct_service, 'lxc') and self.pct_service.lxc:
            output1, exit_code1 = self.pct_service.lxc.execute(sysctl_device_cmd)
            output2, exit_code2 = self.pct_service.lxc.execute(sysctl_mount_cmd)
            if exit_code1 is not None and exit_code1 != 0:
                logger.warning("Sysctl device configuration had issues: %s", output1[-200:] if output1 else "No output")
            if exit_code2 is not None and exit_code2 != 0:
                logger.warning("Sysctl mount configuration had issues: %s", output2[-200:] if output2 else "No output")
        else:
            logger.error("LXC service not available in PCT service")
            return False
        return True

