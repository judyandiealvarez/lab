"""
Systemctl command wrapper with fluent API
"""
import logging
from typing import Optional
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class SystemCtl(CommandWrapper):
    """Wrapper for systemctl commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._service: Optional[str] = None
        self._no_pager: bool = True

    def service(self, name: str) -> "SystemCtl":
        """Set service name (returns self for chaining)."""
        self._service = name
        return self

    def no_pager(self, value: bool = True) -> "SystemCtl":
        """Use --no-pager flag (returns self for chaining)."""
        self._no_pager = value
        return self

    def enable(self) -> str:
        """Generate command to enable a service"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl enable {self._service} 2>&1"
    def disable(self) -> str:
        """Generate command to disable a service"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl disable {self._service} 2>&1"

    def start(self) -> str:
        """Generate command to start a service"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl start {self._service} 2>&1"

    def stop(self) -> str:
        """Generate command to stop a service"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl stop {self._service} 2>&1"

    def restart(self) -> str:
        """Generate command to restart a service"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl restart {self._service} 2>&1"

    def enable_and_start(self) -> str:
        """Generate command to enable and start a service"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl enable {self._service} && systemctl start {self._service} 2>&1"

    def is_active(self) -> str:
        """Generate command to check if service is active"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl is-active {self._service} 2>/dev/null || echo inactive"

    def is_enabled(self) -> str:
        """Generate command to check if service is enabled"""
        if not self._service:
            raise ValueError("Service name must be set")
        return f"systemctl is-enabled {self._service} 2>/dev/null || echo disabled"

    def daemon_reload(self) -> str:
        """Generate command to reload systemd daemon"""
        return "systemctl daemon-reload 2>&1"

    def status(self) -> str:
        """Generate command to get service status"""
        if not self._service:
            raise ValueError("Service name must be set")
        pager_flag = " --no-pager" if self._no_pager else ""
        return f"systemctl status {self._service}{pager_flag} 2>&1"

    @staticmethod

    def parse_is_active(output: Optional[str]) -> bool:
        """Parse output to check if service is active"""
        if not output:
            return False
        return "active" in output.lower() and "inactive" not in output.lower()
    @staticmethod

    def parse_is_enabled(output: Optional[str]) -> bool:
        """Parse output to check if service is enabled"""
        if not output:
            return False
        return "enabled" in output.lower()