"""
Docker command wrapper
"""
import logging
from typing import Optional, List
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class Docker(CommandWrapper):
    """Wrapper for Docker commands - generates command strings"""
    
    @staticmethod
    def find_docker_cmd() -> str:
        """Generate command to find docker command path"""
        return (
            "dpkg -L docker.io 2>/dev/null | grep -E '/bin/docker$' | head -1 || "
            "dpkg -L docker-ce 2>/dev/null | grep -E '/bin/docker$' | head -1 || "
            "command -v docker 2>/dev/null || "
            "which docker 2>/dev/null || "
            "find /usr /usr/local -name docker -type f 2>/dev/null | head -1 || "
            "test -x /usr/bin/docker && echo /usr/bin/docker || "
            "test -x /usr/local/bin/docker && echo /usr/local/bin/docker || "
            "echo 'docker'"
        )
    
    @staticmethod
    def version_cmd(docker_cmd: str = "docker") -> str:
        """Generate command to get Docker version"""
        return f"{docker_cmd} --version 2>&1"
    
    @staticmethod
    def ps_cmd(docker_cmd: str = "docker", all: bool = False) -> str:
        """Generate command to list containers"""
        all_flag = "-a" if all else ""
        return f"{docker_cmd} ps {all_flag} 2>&1"
    
    @staticmethod
    def swarm_init_cmd(docker_cmd: str, advertise_addr: str) -> str:
        """Generate command to initialize Docker Swarm"""
        return f"{docker_cmd} swarm init --advertise-addr {advertise_addr} 2>&1"
    
    @staticmethod
    def swarm_join_token_cmd(docker_cmd: str, role: str = "worker") -> str:
        """Generate command to get Swarm join token"""
        return f"{docker_cmd} swarm join-token {role} -q 2>&1"
    
    @staticmethod
    def swarm_join_cmd(docker_cmd: str, token: str, manager_addr: str) -> str:
        """Generate command to join Docker Swarm"""
        return f"{docker_cmd} swarm join --token {token} {manager_addr} 2>&1"
    
    @staticmethod
    def node_ls_cmd(docker_cmd: str) -> str:
        """Generate command to list Swarm nodes"""
        return f"{docker_cmd} node ls 2>&1"
    
    @staticmethod
    def node_update_cmd(docker_cmd: str, node_name: str, availability: str) -> str:
        """Generate command to update node availability"""
        return f"{docker_cmd} node update --availability {availability} {node_name} 2>&1"
    
    @staticmethod
    def volume_create_cmd(docker_cmd: str, volume_name: str) -> str:
        """Generate command to create Docker volume"""
        return f"{docker_cmd} volume create {volume_name} 2>/dev/null || true"
    
    @staticmethod
    def run_cmd(docker_cmd: str, image: str, name: str, **kwargs) -> str:
        """Generate command to run Docker container"""
        # Basic run command - can be extended with more options
        cmd = f"{docker_cmd} run -d --name {name}"
        if 'restart' in kwargs:
            cmd += f" --restart={kwargs['restart']}"
        if 'network' in kwargs:
            cmd += f" --network {kwargs['network']}"
        if 'volumes' in kwargs:
            for vol in kwargs['volumes']:
                cmd += f" -v {vol}"
        if 'ports' in kwargs:
            for port in kwargs['ports']:
                cmd += f" -p {port}"
        cmd += f" {image} 2>&1"
        return cmd
    
    @staticmethod
    def stop_cmd(docker_cmd: str, container_name: str) -> str:
        """Generate command to stop Docker container"""
        return f"{docker_cmd} stop {container_name} 2>/dev/null || true"
    
    @staticmethod
    def rm_cmd(docker_cmd: str, container_name: str) -> str:
        """Generate command to remove Docker container"""
        return f"{docker_cmd} rm {container_name} 2>/dev/null || true"
    
    @staticmethod
    def logs_cmd(docker_cmd: str, container_name: str, tail: int = 20) -> str:
        """Generate command to get Docker container logs"""
        return f"{docker_cmd} logs {container_name} 2>&1 | tail -{tail}"
    
    @staticmethod
    def system_prune_cmd(docker_cmd: str, all: bool = False, force: bool = False) -> str:
        """Generate command to prune Docker system"""
        flags = ""
        if all:
            flags += " -a"
        if force:
            flags += " -f"
        return f"{docker_cmd} system prune{flags} 2>/dev/null || true"
    
    @staticmethod
    def is_installed_check_cmd(docker_cmd: str = "docker") -> str:
        """Generate command to check if Docker is installed"""
        return f"command -v {docker_cmd} >/dev/null 2>&1 && echo installed || echo not_installed"
    
    @staticmethod
    def parse_is_installed(output: Optional[str]) -> bool:
        """Parse output to check if Docker is installed"""
        if not output:
            return False
        return "installed" in output.lower()
