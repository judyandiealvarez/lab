"""
GlusterFS command wrapper
"""
import logging
from typing import Optional, List
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class Gluster(CommandWrapper):
    """Wrapper for GlusterFS commands - generates command strings"""
    
    @staticmethod
    def find_gluster_cmd() -> str:
        """Generate command to find gluster command path"""
        return (
            "dpkg -L glusterfs-client 2>/dev/null | grep -E '/bin/gluster$|/sbin/gluster$' | head -1 || "
            "command -v gluster 2>/dev/null || "
            "which gluster 2>/dev/null || "
            "find /usr /usr/sbin /usr/bin -name gluster -type f 2>/dev/null | head -1 || "
            "test -x /usr/sbin/gluster && echo /usr/sbin/gluster || "
            "test -x /usr/bin/gluster && echo /usr/bin/gluster || "
            "echo 'gluster'"
        )
    
    @staticmethod
    def peer_probe_cmd(gluster_cmd: str, hostname: str) -> str:
        """Generate command to probe a peer node"""
        return f"{gluster_cmd} peer probe {hostname} 2>&1"
    
    @staticmethod
    def peer_status_cmd(gluster_cmd: str) -> str:
        """Generate command to get peer status"""
        return f"{gluster_cmd} peer status 2>&1"
    
    @staticmethod
    def volume_create_cmd(gluster_cmd: str, volume_name: str, 
                         replica_count: int, bricks: List[str], force: bool = True) -> str:
        """Generate command to create a GlusterFS volume"""
        bricks_str = " ".join(bricks)
        force_flag = "force" if force else ""
        return f"{gluster_cmd} volume create {volume_name} replica {replica_count} {bricks_str} {force_flag} 2>&1"
    
    @staticmethod
    def volume_start_cmd(gluster_cmd: str, volume_name: str) -> str:
        """Generate command to start a GlusterFS volume"""
        return f"{gluster_cmd} volume start {volume_name} 2>&1"
    
    @staticmethod
    def volume_status_cmd(gluster_cmd: str, volume_name: str) -> str:
        """Generate command to get volume status"""
        return f"{gluster_cmd} volume status {volume_name} 2>&1"
    
    @staticmethod
    def volume_info_cmd(gluster_cmd: str, volume_name: str) -> str:
        """Generate command to get volume information"""
        return f"{gluster_cmd} volume info {volume_name} 2>&1"
    
    @staticmethod
    def volume_exists_check_cmd(gluster_cmd: str, volume_name: str) -> str:
        """Generate command to check if volume exists"""
        return f"{gluster_cmd} volume info {volume_name} >/dev/null 2>&1 && echo yes || echo no"
    
    @staticmethod
    def is_installed_check_cmd(gluster_cmd: str = "gluster") -> str:
        """Generate command to check if GlusterFS is installed"""
        return f"command -v {gluster_cmd} >/dev/null 2>&1 && echo installed || echo not_installed"
    
    @staticmethod
    def parse_is_installed(output: Optional[str]) -> bool:
        """Parse output to check if GlusterFS is installed"""
        if not output:
            return False
        return "installed" in output.lower()
    
    @staticmethod
    def parse_volume_exists(output: Optional[str]) -> bool:
        """Parse output to check if volume exists"""
        if not output:
            return False
        return "yes" in output.lower()
