"""
LXC Service - maintains persistent SSH connection to Proxmox host
"""
import logging
from typing import Optional
from libs.config import LabConfig, SSHConfig
from .ssh import SSHService
logger = logging.getLogger(__name__)

class LXCService:
    """Service that maintains a persistent SSH connection to Proxmox host"""
    def __init__(self, proxmox_host: str, ssh_config: SSHConfig):
        """
        Initialize LXC service with SSH connection
        Args:
            proxmox_host: Proxmox host (format: user@host or just host)
            ssh_config: SSH configuration
        """
        self.proxmox_host = proxmox_host
        self.ssh_config = ssh_config
        self._ssh_service = SSHService(proxmox_host, ssh_config)

    def connect(self) -> bool:
        """
        Establish SSH connection to Proxmox host
        Returns:
            True if connection successful, False otherwise
        """
        return self._ssh_service.connect()

    def disconnect(self):
        """Close SSH connection"""
        self._ssh_service.disconnect()

    def is_connected(self) -> bool:
        """Check if SSH connection is active"""
        return self._ssh_service.is_connected()

    def execute(self, command: str, timeout: Optional[int] = None) -> tuple[Optional[str], Optional[int]]:
        """
        Execute command via SSH connection (always shows output interactively and captures it)
        Args:
            command: Command to execute
            timeout: Command timeout in seconds
        Returns:
            Tuple of (output, exit_code). output is always captured
        """
        return self._ssh_service.execute(command, timeout=timeout)

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    @classmethod

    def from_config(cls, cfg: LabConfig) -> "LXCService":
        """
        Create LXCService from LabConfig
        Args:
            cfg: Lab configuration
        Returns:
            LXCService instance
        """
        return cls(cfg.proxmox_host, cfg.ssh)