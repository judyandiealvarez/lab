"""
PCT (Proxmox Container Toolkit) command wrapper
"""

import logging
from typing import Optional
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class PCT(CommandWrapper):
    """Wrapper for PCT commands - generates command strings"""

    @staticmethod
    def create_cmd(  # pylint: disable=too-many-arguments
        container_id: str,
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
        net0_parts = ",".join(
            [
                "name=eth0",
                f"bridge={bridge}",
                "firewall=1",
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
                str(container_id),
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

    @staticmethod
    def start_cmd(container_id: str) -> str:
        """Generate command to start a container"""
        return f"pct start {container_id} 2>&1"

    @staticmethod
    def stop_cmd(container_id: str, force: bool = False) -> str:
        """Generate command to stop a container"""
        force_flag = " --force" if force else ""
        return f"pct stop {container_id}{force_flag} 2>&1"

    @staticmethod
    def status_cmd(container_id: Optional[str] = None) -> str:
        """Generate command to get container status"""
        if container_id:
            return f"pct status {container_id} 2>&1"
        return "pct list 2>&1"

    @staticmethod
    def destroy_cmd(container_id: str, force: bool = False) -> str:
        """Generate command to destroy a container"""
        force_flag = " --force" if force else ""
        return f"pct destroy {container_id}{force_flag} 2>&1"

    @staticmethod
    def set_cmd(container_id: str, option: str, value: str) -> str:
        """Generate command to set container option"""
        return f"pct set {container_id} {option} {value} 2>&1"

    @staticmethod
    def set_features_cmd(
        container_id: str, nesting: bool = True, keyctl: bool = True, fuse: bool = True
    ) -> str:
        """Generate command to set container features"""
        features = []
        if nesting:
            features.append("nesting=1")
        if keyctl:
            features.append("keyctl=1")
        if fuse:
            features.append("fuse=1")
        features_str = ",".join(features)
        return f"pct set {container_id} --features {features_str} 2>&1"

    @staticmethod
    def config_cmd(container_id: str) -> str:
        """Generate command to get container configuration"""
        return f"pct config {container_id} 2>&1"

    @staticmethod
    def exists_check_cmd(container_id: str) -> str:
        """Generate command to check if container exists"""
        return (
            f"test -f /etc/pve/lxc/{container_id}.conf && echo exists || echo missing"
        )

    @staticmethod
    def parse_status_output(output: Optional[str], container_id: str) -> bool:
        """Parse status output to check if container is running"""
        del container_id  # container identifier not needed for parsing
        if not output:
            return False
        return "running" in output.lower()
