"""
Process management command wrapper with fluent API
"""
import shlex
from .base import CommandWrapper

class Process(CommandWrapper):
    """Wrapper for process management commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._signal: int = 9
        self._full_match: bool = False
        self._suppress_errors: bool = True

    def signal(self, value: int) -> "Process":
        """Set signal number (returns self for chaining)."""
        self._signal = value
        return self

    def full_match(self, value: bool = True) -> "Process":
        """Use full match pattern (returns self for chaining)."""
        self._full_match = value
        return self

    def suppress_errors(self, value: bool = True) -> "Process":
        """Suppress errors (returns self for chaining)."""
        self._suppress_errors = value
        return self

    def pkill(self, pattern: str) -> str:
        """Generate pkill command."""
        flags = f"-{self._signal}"
        if self._full_match:
            flags += " -f"
        cmd = f"pkill {flags} {shlex.quote(pattern)}"
        if self._suppress_errors:
            cmd += " 2>/dev/null || true"
        else:
            cmd += " 2>&1"
        return cmd
