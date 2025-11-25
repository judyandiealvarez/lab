"""
Shell command wrapper with fluent API
"""
import shlex
from typing import Optional
from .base import CommandWrapper

class Shell(CommandWrapper):
    """Wrapper for shell script execution with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._shell: str = "sh"
        self._script_path: Optional[str] = None
        self._args: list[str] = []

    def shell(self, shell_path: str) -> "Shell":
        """Set shell path (returns self for chaining)."""
        self._shell = shell_path
        return self

    def script(self, path: str) -> "Shell":
        """Set script path (returns self for chaining)."""
        self._script_path = path
        return self

    def args(self, arguments: list[str]) -> "Shell":
        """Set script arguments (returns self for chaining)."""
        self._args = arguments
        return self

    def execute(self) -> str:
        """Generate shell script execution command"""
        if not self._script_path:
            raise ValueError("Script path must be set for shell execution")
        script_quoted = shlex.quote(self._script_path)
        args_quoted = " ".join([shlex.quote(arg) for arg in self._args]) if self._args else ""
        cmd = f"{self._shell} {script_quoted}"
        if args_quoted:
            cmd += f" {args_quoted}"
        return f"{cmd} 2>&1"

