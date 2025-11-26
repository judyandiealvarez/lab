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

    def lsof_file(self, file_path: str) -> str:
        """Generate lsof command to find process using a file."""
        cmd = f"lsof -t {shlex.quote(file_path)}"
        if self._suppress_errors:
            cmd += " 2>/dev/null | head -1"
        else:
            cmd += " 2>&1 | head -1"
        return cmd

    def fuser_file(self, file_path: str) -> str:
        """Generate fuser command to find process using a file."""
        cmd = f"fuser {shlex.quote(file_path)}"
        if self._suppress_errors:
            cmd += " 2>/dev/null | grep -oE '[0-9]+' | head -1"
        else:
            cmd += " 2>&1 | grep -oE '[0-9]+' | head -1"
        return cmd

    def check_pid(self, pid: int) -> str:
        """Generate command to check if process with PID exists."""
        cmd = f"kill -0 {pid}"
        if self._suppress_errors:
            cmd += " 2>/dev/null && echo exists || echo not_found"
        else:
            cmd += " 2>&1 && echo exists || echo not_found"
        return cmd

    def get_process_name(self, pid: int) -> str:
        """Generate command to get process name by PID."""
        cmd = f"ps -p {pid} -o comm="
        if self._suppress_errors:
            cmd += " 2>/dev/null || echo unknown"
        else:
            cmd += " 2>&1 || echo unknown"
        return cmd

    def kill(self, pid: int) -> str:
        """Generate kill command for a specific PID."""
        cmd = f"kill -{self._signal} {pid}"
        if self._suppress_errors:
            cmd += " 2>/dev/null || true"
        else:
            cmd += " 2>&1"
        return cmd
