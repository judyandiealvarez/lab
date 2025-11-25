"""
Dpkg-related command wrappers with fluent API
"""
import shlex
from typing import Optional
from .base import CommandWrapper

class Dpkg(CommandWrapper):
    """Wrapper for dpkg utility commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._all: bool = False
        self._log_file: Optional[str] = None
        self._suppress_errors: bool = True

    def all(self, value: bool = True) -> "Dpkg":
        """Configure all packages (returns self for chaining)."""
        self._all = value
        return self

    def log_file(self, path: str) -> "Dpkg":
        """Set log file path (returns self for chaining)."""
        self._log_file = path
        return self

    def suppress_errors(self, value: bool = True) -> "Dpkg":
        """Suppress errors (returns self for chaining)."""
        self._suppress_errors = value
        return self

    def configure(self) -> str:
        """Generate dpkg --configure command."""
        parts = ["dpkg"]
        if self._all:
            parts.append("--configure")
            parts.append("-a")
        else:
            parts.append("--configure")
        if self._log_file:
            parts.append(f">{shlex.quote(self._log_file)}")
        if self._suppress_errors:
            parts.append("2>&1 || true")
        else:
            parts.append("2>&1")
        return " ".join(parts)

    def divert(self, path: str, *, quiet: bool = True, local: bool = True, rename: bool = True, action: str = "--add"):
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