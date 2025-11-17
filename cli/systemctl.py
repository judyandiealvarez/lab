"""
Systemctl command wrapper
"""

import logging
from typing import Optional
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class SystemCtl(CommandWrapper):
    """Wrapper for systemctl commands - generates command strings"""

    @staticmethod
    def enable_cmd(service: str) -> str:
        """Generate command to enable a service"""
        return f"systemctl enable {service} 2>&1"

    @staticmethod
    def start_cmd(service: str) -> str:
        """Generate command to start a service"""
        return f"systemctl start {service} 2>&1"

    @staticmethod
    def stop_cmd(service: str) -> str:
        """Generate command to stop a service"""
        return f"systemctl stop {service} 2>&1"

    @staticmethod
    def restart_cmd(service: str) -> str:
        """Generate command to restart a service"""
        return f"systemctl restart {service} 2>&1"

    @staticmethod
    def enable_and_start_cmd(service: str) -> str:
        """Generate command to enable and start a service"""
        return f"systemctl enable {service} && systemctl start {service} 2>&1"

    @staticmethod
    def is_active_check_cmd(service: str) -> str:
        """Generate command to check if service is active"""
        return f"systemctl is-active {service} 2>/dev/null || echo inactive"

    @staticmethod
    def is_enabled_check_cmd(service: str) -> str:
        """Generate command to check if service is enabled"""
        return f"systemctl is-enabled {service} 2>/dev/null || echo disabled"

    @staticmethod
    def daemon_reload_cmd() -> str:
        """Generate command to reload systemd daemon"""
        return "systemctl daemon-reload 2>&1"

    @staticmethod
    def status_cmd(service: str) -> str:
        """Generate command to get service status"""
        return f"systemctl status {service} --no-pager 2>&1"

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
