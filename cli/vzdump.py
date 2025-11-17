"""
Vzdump command wrapper for template creation
"""

import logging
from typing import Optional
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class Vzdump(CommandWrapper):
    """Wrapper for vzdump commands - generates command strings"""

    @staticmethod
    def create_template_cmd(
        container_id: str, dumpdir: str, compress: str = "zstd", mode: str = "stop"
    ) -> str:
        """Generate command to create template from container using vzdump"""
        return (
            f"vzdump {container_id} --dumpdir {dumpdir} "
            f"--compress {compress} --mode {mode} 2>&1"
        )

    @staticmethod
    def find_archive_cmd(dumpdir: str, container_id: str) -> str:
        """Generate command to find the most recent archive file for a container"""
        return (
            f"ls -t {dumpdir}/vzdump-lxc-{container_id}-*.tar.zst 2>/dev/null | head -1"
        )

    @staticmethod
    def get_archive_size_cmd(archive_path: str) -> str:
        """Generate command to get archive file size in bytes"""
        return f"stat -c%s '{archive_path}' 2>/dev/null || echo '0'"

    @staticmethod
    def parse_archive_size(output: Optional[str]) -> Optional[int]:
        """Parse output to get archive file size"""
        if not output:
            return None
        try:
            return int(output.strip())
        except ValueError:
            return None
