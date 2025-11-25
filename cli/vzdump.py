"""
Vzdump command wrapper with fluent API
"""
import logging
from typing import Optional
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class Vzdump(CommandWrapper):
    """Wrapper for vzdump commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._compress: str = "zstd"
        self._mode: str = "stop"

    def compress(self, value: str) -> "Vzdump":
        """Set compression format (returns self for chaining)."""
        self._compress = value
        return self

    def mode(self, value: str) -> "Vzdump":
        """Set dump mode (returns self for chaining)."""
        self._mode = value
        return self

    def create_template(self, container_id: str, dumpdir: str) -> str:
        """Generate command to create template from container using vzdump"""
        return f"vzdump {container_id} --dumpdir {dumpdir} " f"--compress {self._compress} --mode {self._mode} 2>&1"

    def find_archive(self, dumpdir: str, container_id: str) -> str:
        """Generate command to find the most recent archive file for a container"""
        return f"ls -t {dumpdir}/vzdump-lxc-{container_id}-*.tar.zst 2>/dev/null | head -1"

    def get_archive_size(self, archive_path: str) -> str:
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