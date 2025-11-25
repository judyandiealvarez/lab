"""
GlusterFS command wrapper with fluent API
"""
import logging
from typing import Optional, List
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class Gluster(CommandWrapper):
    """Wrapper for GlusterFS commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._gluster_cmd: str = "gluster"
        self._force: bool = True

    def gluster_cmd(self, cmd: str) -> "Gluster":
        """Set gluster command path (returns self for chaining)."""
        self._gluster_cmd = cmd
        return self

    def force(self, value: bool = True) -> "Gluster":
        """Set force flag (returns self for chaining)."""
        self._force = value
        return self

    def find_gluster(self) -> str:
        """Generate command to find gluster command path"""
        parts = [
            ("dpkg -L glusterfs-client 2>/dev/null " "| grep -E '/bin/gluster$|/sbin/gluster$' | head -1"),
            "command -v gluster 2>/dev/null",
            "which gluster 2>/dev/null",
            ("find /usr /usr/sbin /usr/bin -name gluster -type f 2>/dev/null " "| head -1"),
            "test -x /usr/sbin/gluster && echo /usr/sbin/gluster",
            "test -x /usr/bin/gluster && echo /usr/bin/gluster",
            "echo 'gluster'",
        ]
        return " || ".join(parts)

    def peer_probe(self, hostname: str) -> str:
        """Generate command to probe a peer node"""
        return f"{self._gluster_cmd} peer probe {hostname} 2>&1"

    def peer_status(self) -> str:
        """Generate command to get peer status"""
        return f"{self._gluster_cmd} peer status 2>&1"

    def volume_create(self, volume_name: str, replica_count: int, bricks: List[str]) -> str:
        """Generate command to create a GlusterFS volume"""
        bricks_str = " ".join(bricks)
        force_flag = "force" if self._force else ""
        return " ".join(
            [
                self._gluster_cmd,
                "volume",
                "create",
                volume_name,
                "replica",
                str(replica_count),
                bricks_str,
                force_flag,
                "2>&1",
            ]
        ).strip()

    def volume_start(self, volume_name: str) -> str:
        """Generate command to start a GlusterFS volume"""
        return f"{self._gluster_cmd} volume start {volume_name} 2>&1"

    def volume_status(self, volume_name: str) -> str:
        """Generate command to get volume status"""
        return f"{self._gluster_cmd} volume status {volume_name} 2>&1"

    def volume_info(self, volume_name: str) -> str:
        """Generate command to get volume information"""
        return f"{self._gluster_cmd} volume info {volume_name} 2>&1"

    def volume_exists_check(self, volume_name: str) -> str:
        """Generate command to check if volume exists"""
        return f"{self._gluster_cmd} volume info {volume_name} >/dev/null 2>&1 && echo yes || echo no"

    def is_installed_check(self) -> str:
        """Generate command to check if GlusterFS is installed"""
        return f"command -v {self._gluster_cmd} >/dev/null 2>&1 && echo installed || echo not_installed"
    @staticmethod

    def parse_is_installed(output: Optional[str]) -> bool:
        """Parse output to check if GlusterFS is installed"""
        return CommandWrapper.contains_token(output, "installed")
    @staticmethod

    def parse_volume_exists(output: Optional[str]) -> bool:
        """Parse output to check if volume exists"""
        return CommandWrapper.contains_token(output, "yes")