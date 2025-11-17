"""
File and filesystem command wrappers
"""

import shlex
from typing import Optional

from .base import CommandWrapper


def _quote_path(path: str, *, allow_glob: bool = False) -> str:
    """Quote a path unless glob expansion is required."""
    if allow_glob:
        return path
    return shlex.quote(path)


def _escape_single_quotes(value: str) -> str:
    return value.replace("'", "'\"'\"'")


class FileOps(CommandWrapper):
    """Wrapper for common file operations."""

    @staticmethod
    def write_cmd(path: str, content: str, append: bool = False) -> str:
        """Generate command that writes literal content to a file via printf."""
        sanitized = content.replace("\\", "\\\\")
        sanitized = _escape_single_quotes(sanitized)
        redir = ">>" if append else ">"
        return f"printf '{sanitized}' {redir} {shlex.quote(path)} 2>&1"

    @staticmethod
    def chmod_cmd(path: str, mode: str) -> str:
        """Generate command to change permissions on path."""
        return f"chmod {mode} {shlex.quote(path)} 2>&1"

    @staticmethod
    def mkdir_cmd(path: str, parents: bool = True) -> str:
        """Generate command to create directory."""
        flag = "-p " if parents else ""
        return f"mkdir {flag}{shlex.quote(path)} 2>&1"

    @staticmethod
    def chown_cmd(path: str, owner: str, group: Optional[str] = None) -> str:
        """Generate command to change ownership."""
        owner_spec = owner if group is None else f"{owner}:{group}"
        return f"chown {owner_spec} {shlex.quote(path)} 2>&1"

    @staticmethod
    def remove_cmd(
        path: str,
        *,
        recursive: bool = False,
        force: bool = True,
        allow_glob: bool = False,
    ) -> str:
        """Generate rm command."""
        flags = ""
        if recursive:
            flags += "r"
        if force:
            flags += "f"
        flag_part = f"-{flags} " if flags else ""
        return f"rm {flag_part}{_quote_path(path, allow_glob=allow_glob)} 2>&1"

    @staticmethod
    def truncate_cmd(path: str, *, suppress_errors: bool = False) -> str:
        """Generate command to truncate a file."""
        cmd = f"truncate -s 0 {shlex.quote(path)}"
        cmd += " 2>/dev/null" if suppress_errors else " 2>&1"
        return cmd

    @staticmethod
    def symlink_cmd(target: str, link_path: str) -> str:
        """Generate command to create a symbolic link."""
        return f"ln -s {shlex.quote(target)} {shlex.quote(link_path)} 2>&1"

    @staticmethod
    def find_delete_cmd(
        directory: str,
        pattern: str,
        *,
        file_type: str = "f",
        suppress_errors: bool = True,
    ) -> str:
        """Generate command to delete files matching pattern under directory."""
        pattern_escaped = _escape_single_quotes(pattern)
        cmd = f"find {shlex.quote(directory)} -type {file_type} -name '{pattern_escaped}' -delete"
        cmd += " 2>/dev/null" if suppress_errors else " 2>&1"
        return cmd
