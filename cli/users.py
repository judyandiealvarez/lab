"""
User management command wrappers with fluent API
"""
import shlex
from typing import Iterable, Optional
from .base import CommandWrapper

class User(CommandWrapper):
    """Wrapper for user-related commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._username: Optional[str] = None
        self._shell: str = "/bin/bash"
        self._groups: Optional[Iterable[str]] = None
        self._create_home: bool = True

    def username(self, name: str) -> "User":
        """Set username (returns self for chaining)."""
        self._username = name
        return self

    def shell(self, path: str) -> "User":
        """Set shell path (returns self for chaining)."""
        self._shell = path
        return self

    def groups(self, group_list: Iterable[str]) -> "User":
        """Set groups (returns self for chaining)."""
        self._groups = group_list
        return self

    def create_home(self, value: bool = True) -> "User":
        """Set create home directory (returns self for chaining)."""
        self._create_home = value
        return self

    def check_exists(self) -> str:
        """Generate command to verify if a user exists."""
        if not self._username:
            raise ValueError("Username must be set")
        return f"id -u {shlex.quote(self._username)} >/dev/null"

    def add(self) -> str:
        """Generate command to add a user."""
        if not self._username:
            raise ValueError("Username must be set")
        parts = ["useradd"]
        if self._create_home:
            parts.append("-m")
        parts.extend(["-s", shlex.quote(self._shell)])
        if self._groups:
            group_spec = ",".join(self._groups)
            parts.extend(["-G", shlex.quote(group_spec)])
        parts.append(shlex.quote(self._username))
        return " ".join(parts)
