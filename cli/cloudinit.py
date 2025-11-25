"""
Cloud-init command wrapper with fluent API
"""
import logging
from typing import Optional
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class CloudInit(CommandWrapper):
    """Wrapper for cloud-init commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._log_file: Optional[str] = None
        self._suppress_output: bool = False

    def log_file(self, path: str) -> "CloudInit":
        """Set log file path for output redirection (returns self for chaining)."""
        self._log_file = path
        return self

    def suppress_output(self, value: bool = True) -> "CloudInit":
        """Suppress output by redirecting to /dev/null (returns self for chaining)."""
        self._suppress_output = value
        return self

    def status(self, wait: bool = False) -> str:
        """Generate command to get cloud-init status"""
        cmd = "cloud-init status"
        if wait:
            cmd += " --wait"
        if self._log_file:
            cmd += f" >{self._log_file} 2>&1"
        elif self._suppress_output:
            cmd += " >/dev/null 2>&1"
        else:
            cmd += " 2>&1"
        return cmd

    def clean(self, logs: bool = False, seed: bool = False, machine_id: bool = False) -> str:
        """Generate command to clean cloud-init data"""
        cmd = "cloud-init clean"
        flags = []
        if logs:
            flags.append("--logs")
        if seed:
            flags.append("--seed")
        if machine_id:
            flags.append("--machine-id")
        if flags:
            cmd += " " + " ".join(flags)
        if self._suppress_output:
            cmd += " >/dev/null 2>&1"
        else:
            cmd += " 2>&1"
        return cmd

    def wait(self, log_file: Optional[str] = None) -> str:
        """Generate command to wait for cloud-init to complete"""
        cmd = "cloud-init status --wait"
        if log_file:
            cmd += f" >{log_file} 2>&1"
        elif self._log_file:
            cmd += f" >{self._log_file} 2>&1"
        elif self._suppress_output:
            cmd += " >/dev/null 2>&1"
        else:
            cmd += " 2>&1"
        return cmd

    @staticmethod
    def parse_status(output: Optional[str]) -> Optional[str]:
        """Parse cloud-init status output to get current status"""
        if not output:
            return None
        output_lower = output.lower().strip()
        if "status:" in output_lower:
            parts = output_lower.split("status:", 1)
            if len(parts) > 1:
                return parts[1].strip().split()[0] if parts[1].strip() else None
        return output_lower if output_lower else None

