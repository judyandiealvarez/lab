"""
PCT (Proxmox Container Toolkit) command wrapper with fluent API
"""
import logging
from typing import Optional
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class PCT(CommandWrapper):
    """Wrapper for PCT commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._container_id: Optional[str] = None
        self._force: bool = False
        self._nesting: bool = True
        self._keyctl: bool = True
        self._fuse: bool = True
        self._firewall: bool = False  # Default to disabled for SSH access

    def container_id(self, cid: str) -> "PCT":
        """Set container ID (returns self for chaining)."""
        self._container_id = cid
        return self

    def force(self, value: bool = True) -> "PCT":
        """Set force flag (returns self for chaining)."""
        self._force = value
        return self

    def nesting(self, value: bool = True) -> "PCT":
        """Set nesting feature (returns self for chaining)."""
        self._nesting = value
        return self

    def keyctl(self, value: bool = True) -> "PCT":
        """Set keyctl feature (returns self for chaining)."""
        self._keyctl = value
        return self

    def fuse(self, value: bool = True) -> "PCT":
        """Set fuse feature (returns self for chaining)."""
        self._fuse = value
        return self

    def firewall(self, value: bool = False) -> "PCT":
        """Set firewall setting (returns self for chaining)."""
        self._firewall = value
        return self

    def create(
        self,
        template_path: str,
        hostname: str,
        memory: int,
        swap: int,
        cores: int,
        ip_address: str,
        gateway: str,
        bridge: str,
        storage: str,
        rootfs_size: int,
        unprivileged: bool = True,
        ostype: str = "ubuntu",
        arch: str = "amd64",
    ) -> str:
        """Generate command to create a container"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        net0_parts = ",".join(
            [
                "name=eth0",
                f"bridge={bridge}",
                f"firewall={'1' if self._firewall else '0'}",
                f"gw={gateway}",
                f"ip={ip_address}/24",
                "ip6=dhcp",
                "type=veth",
            ]
        )
        return " ".join(
            [
                "pct",
                "create",
                str(self._container_id),
                template_path,
                f"--hostname {hostname}",
                f"--memory {memory}",
                f"--swap {swap}",
                f"--cores {cores}",
                f"--net0 {net0_parts}",
                f"--rootfs {storage}:{rootfs_size}",
                f"--unprivileged {'1' if unprivileged else '0'}",
                f"--ostype {ostype}",
                f"--arch {arch}",
                "2>&1",
            ]
        )

    def start(self) -> str:
        """Generate command to start a container"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        return f"pct start {self._container_id} 2>&1"

    def stop(self) -> str:
        """Generate command to stop a container"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        force_flag = " --force" if self._force else ""
        return f"pct stop {self._container_id}{force_flag} 2>&1"

    def status(self) -> str:
        """Generate command to get container status"""
        if self._container_id:
            return f"pct status {self._container_id} 2>&1"
        return "pct list 2>&1"

    def destroy(self) -> str:
        """Generate command to destroy a container"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        force_flag = " --force" if self._force else ""
        return f"pct destroy {self._container_id}{force_flag} 2>&1"

    def set_option(self, option: str, value: str) -> str:
        """Generate command to set container option"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        return f"pct set {self._container_id} {option} {value} 2>&1"

    def set_features(self) -> str:
        """Generate command to set container features"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        features = []
        if self._nesting:
            features.append("nesting=1")
        if self._keyctl:
            features.append("keyctl=1")
        if self._fuse:
            features.append("fuse=1")
        features_str = ",".join(features)
        return f"pct set {self._container_id} --features {features_str} 2>&1"

    def config(self) -> str:
        """Generate command to get container configuration"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        return f"pct config {self._container_id} 2>&1"

    def exists(self) -> str:
        """Generate command to check if pct exists"""
        return "command -v pct >/dev/null 2>&1"

    def exists_check(self) -> str:
        """Generate command to check if container exists"""
        if not self._container_id:
            raise ValueError("Container ID must be set")
        return f"test -f /etc/pve/lxc/{self._container_id}.conf && echo exists || echo missing"
    @staticmethod

    def parse_status_output(output: Optional[str], container_id: str) -> bool:
        """Parse status output to check if container is running"""
        del container_id  # container identifier not needed for parsing
        if not output:
            return False
        return "running" in output.lower()