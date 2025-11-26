"""
File and filesystem command wrappers with fluent API
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
    """Wrapper for common file operations with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._recursive: bool = False
        self._force: bool = True
        self._allow_glob: bool = False
        self._suppress_errors: bool = False
        self._append: bool = False

    def recursive(self, value: bool = True) -> "FileOps":
        """Set recursive mode (returns self for chaining)."""
        self._recursive = value
        return self

    def force(self, value: bool = True) -> "FileOps":
        """Set force mode (returns self for chaining)."""
        self._force = value
        return self

    def allow_glob(self, value: bool = True) -> "FileOps":
        """Allow glob expansion (returns self for chaining)."""
        self._allow_glob = value
        return self

    def suppress_errors(self, value: bool = True) -> "FileOps":
        """Suppress errors (returns self for chaining)."""
        self._suppress_errors = value
        return self

    def append(self, value: bool = True) -> "FileOps":
        """Set append mode (returns self for chaining)."""
        self._append = value
        return self

    def write(self, path: str, content: str) -> str:
        """Generate command that writes literal content to a file via printf."""
        sanitized = content.replace("\\", "\\\\")
        sanitized = _escape_single_quotes(sanitized)
        redir = ">>" if self._append else ">"
        return f"printf '{sanitized}' {redir} {shlex.quote(path)} 2>&1"

    def chmod(self, path: str, mode: str) -> str:
        """Generate command to change permissions on path."""
        return f"chmod {mode} {shlex.quote(path)} 2>&1"

    def mkdir(self, path: str, parents: bool = True) -> str:
        """Generate command to create directory."""
        flag = "-p " if parents else ""
        return f"mkdir {flag}{shlex.quote(path)} 2>&1"

    def chown(self, path: str, owner: str, group: Optional[str] = None) -> str:
        """Generate command to change ownership."""
        owner_spec = owner if group is None else f"{owner}:{group}"
        return f"chown {owner_spec} {shlex.quote(path)} 2>&1"

    def remove(self, path: str) -> str:
        """Generate rm command."""
        flags = ""
        if self._recursive:
            flags += "r"
        if self._force:
            flags += "f"
        flag_part = f"-{flags} " if flags else ""
        return f"rm {flag_part}{_quote_path(path, allow_glob=self._allow_glob)} 2>&1"

    def truncate(self, path: str) -> str:
        """Generate command to truncate a file."""
        cmd = f"truncate -s 0 {shlex.quote(path)}"
        cmd += " 2>/dev/null" if self._suppress_errors else " 2>&1"
        return cmd

    def symlink(self, target: str, link_path: str) -> str:
        """Generate command to create a symbolic link."""
        return f"ln -s {shlex.quote(target)} {shlex.quote(link_path)} 2>&1"

    def find_delete(self, directory: str, pattern: str, file_type: str = "f") -> str:
        """Generate command to delete files matching pattern under directory."""
        pattern_escaped = _escape_single_quotes(pattern)
        cmd = f"find {shlex.quote(directory)} -type {file_type} -name '{pattern_escaped}' -delete"
        cmd += " 2>/dev/null" if self._suppress_errors else " 2>&1"
        return cmd

    def exists(self, path: str) -> str:
        """Generate command to check if file exists."""
        cmd = f"test -f {shlex.quote(path)} && echo exists || echo not_found"
        cmd += " 2>/dev/null" if self._suppress_errors else " 2>&1"
        return cmd