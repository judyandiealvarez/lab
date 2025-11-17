"""
Dpkg-related command wrappers
"""

import shlex

from .base import CommandWrapper


class Dpkg(CommandWrapper):
    """Wrapper for dpkg utility commands."""

    @staticmethod
    def divert_cmd(
        path: str,
        *,
        quiet: bool = True,
        local: bool = True,
        rename: bool = True,
        action: str = "--add",
    ) -> str:
        """Generate dpkg-divert command."""
        parts = ["dpkg-divert"]
        if quiet:
            parts.append("--quiet")
        if local:
            parts.append("--local")
        if rename:
            parts.append("--rename")
        parts.append(action)
        parts.append(shlex.quote(path))
        return " ".join(parts) + " 2>&1"
