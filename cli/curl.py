"""
Curl command wrapper with fluent API
"""
import shlex
from typing import Optional
from .base import CommandWrapper

class Curl(CommandWrapper):
    """Wrapper for curl commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._fail_silently: bool = True
        self._silent: bool = True
        self._show_errors: bool = True
        self._location: bool = False
        self._output: Optional[str] = None
        self._url: Optional[str] = None

    def fail_silently(self, value: bool = True) -> "Curl":
        """Set fail silently mode -f (returns self for chaining)."""
        self._fail_silently = value
        return self

    def silent(self, value: bool = True) -> "Curl":
        """Set silent mode -s (returns self for chaining)."""
        self._silent = value
        return self

    def show_errors(self, value: bool = True) -> "Curl":
        """Show errors even in silent mode -S (returns self for chaining)."""
        self._show_errors = value
        return self

    def location(self, value: bool = True) -> "Curl":
        """Follow redirects -L (returns self for chaining)."""
        self._location = value
        return self

    def output(self, path: str) -> "Curl":
        """Set output file path -o (returns self for chaining)."""
        self._output = path
        return self

    def url(self, url: str) -> "Curl":
        """Set URL to fetch (returns self for chaining)."""
        self._url = url
        return self

    def download(self) -> str:
        """Generate curl download command"""
        if not self._url:
            raise ValueError("URL must be set for curl download")
        flags = []
        if self._fail_silently:
            flags.append("-f")
        if self._silent:
            flags.append("-s")
        if self._show_errors:
            flags.append("-S")
        if self._location:
            flags.append("-L")
        if self._output:
            flags.append(f"-o {shlex.quote(self._output)}")
        flag_str = " ".join(flags) if flags else ""
        return f"curl {flag_str} {shlex.quote(self._url)} 2>&1"

