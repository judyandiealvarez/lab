"""
Command utility wrapper with fluent API
"""
import logging
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class Command(CommandWrapper):
    """Wrapper for command utility with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._command_name: str = ""

    def command(self, name: str) -> "Command":
        """Set command name to check (returns self for chaining)."""
        self._command_name = name
        return self

    def exists(self) -> str:
        """Generate command to check if a command exists"""
        if not self._command_name:
            raise ValueError("Command name must be set")
        return f"command -v {self._command_name}"

