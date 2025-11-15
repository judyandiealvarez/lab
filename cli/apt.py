"""
APT/APT-GET command wrapper
"""
import logging
from typing import List, Optional
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class Apt(CommandWrapper):
    """Wrapper for APT/APT-GET commands - generates command strings"""
    
    @staticmethod
    def update_cmd(quiet: bool = False) -> str:
        """Generate command to update package lists"""
        quiet_flag = "-qq" if quiet else ""
        return f"DEBIAN_FRONTEND=noninteractive apt update {quiet_flag} 2>&1"
    
    @staticmethod
    def upgrade_cmd(dist_upgrade: bool = False) -> str:
        """Generate command to upgrade packages"""
        cmd_type = "dist-upgrade" if dist_upgrade else "upgrade"
        return f"DEBIAN_FRONTEND=noninteractive apt {cmd_type} -y 2>&1"
    
    @staticmethod
    def install_cmd(packages: List[str], no_install_recommends: bool = False) -> str:
        """Generate command to install packages"""
        packages_str = " ".join(packages)
        no_recommends = "--no-install-recommends" if no_install_recommends else ""
        return f"DEBIAN_FRONTEND=noninteractive apt install -y {no_recommends} {packages_str} 2>&1"
    
    @staticmethod
    def remove_cmd(packages: List[str]) -> str:
        """Generate command to remove packages"""
        packages_str = " ".join(packages)
        return f"DEBIAN_FRONTEND=noninteractive apt remove -y {packages_str} 2>&1"
    
    @staticmethod
    def fix_broken_cmd() -> str:
        """Generate command to fix broken packages"""
        return "DEBIAN_FRONTEND=noninteractive apt --fix-broken install -y 2>&1"
    
    @staticmethod
    def clean_cmd() -> str:
        """Generate command to clean package cache"""
        return "apt clean 2>&1"
    
    @staticmethod
    def is_installed_check_cmd(package: str) -> str:
        """Generate command to check if package is installed"""
        return f"dpkg -l | grep -q '^ii.*{package}' && echo installed || echo not_installed"
    
    @staticmethod
    def command_exists_check_cmd(command_name: str) -> str:
        """Generate command to check if command exists in PATH"""
        return f"command -v {command_name} >/dev/null 2>&1 && echo exists || echo not_found"
    
    @staticmethod
    def parse_is_installed(output: Optional[str]) -> bool:
        """Parse output to check if package is installed"""
        if not output:
            return False
        return "installed" in output.lower()
    
    @staticmethod
    def parse_command_exists(output: Optional[str]) -> bool:
        """Parse output to check if command exists"""
        if not output:
            return False
        return "exists" in output.lower()
