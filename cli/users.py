"""
User management command wrappers
"""

import shlex
from typing import Iterable, Optional

from .base import CommandWrapper


class User(CommandWrapper):
    """Wrapper for user-related commands."""

    @staticmethod
    def check_exists_cmd(username: str) -> str:
        """Generate command to verify if a user exists."""
        return f"id -u {shlex.quote(username)} >/dev/null 2>&1"

    @staticmethod
    def add_cmd(
        username: str,
        *,
        shell: str = "/bin/bash",
        groups: Optional[Iterable[str]] = None,
        create_home: bool = True,
    ) -> str:
        """Generate command to add a user."""
        parts = ["useradd"]
        if create_home:
            parts.append("-m")
        parts.extend(["-s", shlex.quote(shell)])
        if groups:
            group_spec = ",".join(groups)
            parts.extend(["-G", shlex.quote(group_spec)])
        parts.append(shlex.quote(username))
        return " ".join(parts) + " 2>&1"
