"""
Docker command wrapper with fluent API
"""
import logging
from typing import Optional
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class Docker(CommandWrapper):
    """Wrapper for Docker commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._docker_cmd: str = "docker"
        self._show_all: bool = False
        self._include_all: bool = False
        self._force: bool = False
        self._tail: int = 20

    def docker_cmd(self, cmd: str) -> "Docker":
        """Set docker command path (returns self for chaining)."""
        self._docker_cmd = cmd
        return self

    def show_all(self, value: bool = True) -> "Docker":
        """Show all containers (returns self for chaining)."""
        self._show_all = value
        return self

    def include_all(self, value: bool = True) -> "Docker":
        """Include all in prune (returns self for chaining)."""
        self._include_all = value
        return self

    def force(self, value: bool = True) -> "Docker":
        """Set force flag (returns self for chaining)."""
        self._force = value
        return self

    def tail(self, lines: int) -> "Docker":
        """Set tail lines (returns self for chaining)."""
        self._tail = lines
        return self

    def find_docker(self) -> str:
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

    def version(self) -> str:
        """Generate command to get Docker version"""
        return f"{self._docker_cmd} --version 2>&1"

    def ps(self) -> str:
        """Generate command to list containers"""
        all_flag = "-a" if self._show_all else ""
        return f"{self._docker_cmd} ps {all_flag} 2>&1"

    def swarm_init(self, advertise_addr: str) -> str:
        """Generate command to initialize Docker Swarm"""
        return f"{self._docker_cmd} swarm init --advertise-addr {advertise_addr} 2>&1"

    def swarm_join_token(self, role: str = "worker") -> str:
        """Generate command to get Swarm join token"""
        return f"{self._docker_cmd} swarm join-token {role} -q 2>&1"

    def swarm_join(self, token: str, manager_addr: str) -> str:
        """Generate command to join Docker Swarm"""
        return f"{self._docker_cmd} swarm join --token {token} {manager_addr} 2>&1"

    def node_ls(self) -> str:
        """Generate command to list Swarm nodes"""
        return f"{self._docker_cmd} node ls 2>&1"

    def node_update(self, node_name: str, availability: str) -> str:
        """Generate command to update node availability"""
        return f"{self._docker_cmd} node update --availability {availability} {node_name} 2>&1"

    def volume_create(self, volume_name: str) -> str:
        """Generate command to create Docker volume"""
        return f"{self._docker_cmd} volume create {volume_name} 2>/dev/null || true"

    def volume_rm(self, volume_name: str) -> str:
        """Generate command to remove Docker volume"""
        force_flag = "-f " if self._force else ""
        return f"{self._docker_cmd} volume rm {force_flag}{volume_name} 2>/dev/null || true"

    def run(self, image: str, name: str, **kwargs) -> str:
        """Generate command to run Docker container"""
        import shlex
        cmd = f"{self._docker_cmd} run -d --name {name}"
        if "restart" in kwargs:
            cmd += f" --restart={kwargs['restart']}"
        if "network" in kwargs:
            cmd += f" --network {kwargs['network']}"
        if "volumes" in kwargs:
            for vol in kwargs["volumes"]:
                cmd += f" -v {vol}"
        if "ports" in kwargs:
            for port in kwargs["ports"]:
                cmd += f" -p {port}"
        if "security_opts" in kwargs:
            for opt in kwargs["security_opts"]:
                cmd += f" --security-opt {opt}"
        cmd += f" {image}"
        if "command_args" in kwargs:
            for arg in kwargs["command_args"]:
                cmd += f" {shlex.quote(str(arg))}"
        cmd += " 2>&1"
        return cmd

    def stop(self, container_name: str) -> str:
        """Generate command to stop Docker container"""
        return f"{self._docker_cmd} stop {container_name} 2>/dev/null || true"

    def rm(self, container_name: str) -> str:
        """Generate command to remove Docker container"""
        return f"{self._docker_cmd} rm {container_name} 2>/dev/null || true"

    def logs(self, container_name: str) -> str:
        """Generate command to get Docker container logs"""
        return f"{self._docker_cmd} logs {container_name} 2>&1 | tail -{self._tail}"

    def system_prune(self) -> str:
        """Generate command to prune Docker system"""
        flags = ""
        if self._include_all:
            flags += " -a"
        if self._force:
            flags += " -f"
        return f"{self._docker_cmd} system prune{flags} 2>/dev/null || true"

    def is_installed_check(self) -> str:
        """Generate command to check if Docker is installed"""
        return f"command -v {self._docker_cmd} >/dev/null 2>&1 " "&& echo installed || echo not_installed"
    @staticmethod

    def parse_is_installed(output: Optional[str]) -> bool:
        """Parse output to check if Docker is installed"""
        if not output:
            return False
        return "installed" in output.lower()