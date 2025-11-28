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
        # pct set doesn't support lxc.mount.auto, so we need to edit the config file directly
        # The config file is at /etc/pve/lxc/<id>.conf on the Proxmox host
        config_file = f"/etc/pve/lxc/{self.container_id}.conf"
        
        # Access lxc_service through pct_service
        if hasattr(self.pct_service, 'lxc') and self.pct_service.lxc:
            # Check what k3s LXC requirements are already configured
            check_cmd = f"grep -E '^lxc.(mount.auto|apparmor.profile|cap.drop|cgroup2.devices.allow)' {config_file} 2>/dev/null || echo 'not_found'"
            check_output, _ = self.pct_service.lxc.execute(check_cmd)
            
            needs_restart = False
            # Add required k3s LXC configurations according to official requirements
            configs_to_add = []
            
            # Check and add lxc.apparmor.profile: unconfined
            if not check_output or "lxc.apparmor.profile" not in check_output:
                configs_to_add.append("lxc.apparmor.profile: unconfined")
                needs_restart = True
            
            # Check and add lxc.cap.drop: (empty = don't drop any capabilities)
            if not check_output or "lxc.cap.drop:" not in check_output:
                configs_to_add.append("lxc.cap.drop:")
                needs_restart = True
            
            # Check and add lxc.mount.auto: proc:rw sys:rw
            if not check_output or "lxc.mount.auto" not in check_output:
                configs_to_add.append("lxc.mount.auto: proc:rw sys:rw")
                needs_restart = True
            
            # Check and add lxc.cgroup2.devices.allow: c 1:11 rwm (for /dev/kmsg)
            if not check_output or "lxc.cgroup2.devices.allow" not in check_output:
                configs_to_add.append("lxc.cgroup2.devices.allow: c 1:11 rwm")
                needs_restart = True
            
            if configs_to_add:
                logger.info("Adding k3s LXC configuration requirements...")
                for config_line in configs_to_add:
                    add_cmd = f"echo '{config_line}' >> {config_file}"
                    add_output, add_exit = self.pct_service.lxc.execute(add_cmd)
                    if add_exit is not None and add_exit != 0:
                        logger.error("Failed to add {config_line}: %s", add_output[-200:] if add_output else "No output")
                        return False
                    logger.info("Added: %s", config_line)
                
                if needs_restart:
                    logger.info("Container needs to be restarted for k3s LXC configuration to take effect...")
                    restart_cmd = f"pct stop {self.container_id} && sleep 2 && pct start {self.container_id}"
                    restart_output, restart_exit = self.pct_service.lxc.execute(restart_cmd)
                    if restart_exit is not None and restart_exit != 0:
                        logger.warning("Container restart had issues: %s", restart_output[-200:] if restart_output else "No output")
                    else:
                        logger.info("Container restarted to apply k3s LXC configuration")
                        import time
                        time.sleep(5)  # Wait for container to start
            else:
                logger.info("All k3s LXC configuration requirements already present")
            
            # Ensure /dev/kmsg is a symlink to /dev/console (official k3s LXC requirement)
            # This needs to be done inside the container after it starts
            logger.info("Ensuring /dev/kmsg is configured as symlink to /dev/console (k3s LXC requirement)...")
            kmsg_cmd = f"pct exec {self.container_id} -- bash -c 'rm -f /dev/kmsg && ln -sf /dev/console /dev/kmsg && ls -l /dev/kmsg'"
            kmsg_output, kmsg_exit = self.pct_service.lxc.execute(kmsg_cmd)
            if kmsg_exit is not None and kmsg_exit != 0:
                logger.warning("Failed to create /dev/kmsg symlink: %s", kmsg_output[-200:] if kmsg_output else "No output")
            else:
                logger.info("/dev/kmsg symlink configured successfully")
        else:
            logger.error("LXC service not available in PCT service")
            return False
        return True

