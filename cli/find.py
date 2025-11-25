"""
Find command wrapper with fluent API
"""
import shlex
from typing import Optional
from .base import CommandWrapper

def _escape_single_quotes(value: str) -> str:
    return value.replace("'", "'\"'\"'")

class Find(CommandWrapper):
    """Wrapper for find commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._directory: Optional[str] = None
        self._maxdepth: Optional[int] = None
        self._type: Optional[str] = None
        self._name: Optional[str] = None
        self._action: Optional[str] = None

    def directory(self, path: str) -> "Find":
        """Set directory to search (returns self for chaining)."""
        self._directory = path
        return self

    def maxdepth(self, depth: int) -> "Find":
        """Set maxdepth (returns self for chaining)."""
        self._maxdepth = depth
        return self

    def type(self, file_type: str) -> "Find":
        """Set file type (f for file, d for directory, etc.) (returns self for chaining)."""
        self._type = file_type
        return self

    def name(self, pattern: str) -> "Find":
        """Set name pattern (returns self for chaining)."""
        self._name = pattern
        return self

    def delete(self) -> str:
        """Generate find command with -delete action."""
        if not self._directory:
            raise ValueError("Directory must be set")
        cmd = f"find {shlex.quote(self._directory)}"
        if self._maxdepth is not None:
            cmd += f" -maxdepth {self._maxdepth}"
        if self._type:
            cmd += f" -type {self._type}"
        if self._name:
            pattern_escaped = _escape_single_quotes(self._name)
            cmd += f" -name '{pattern_escaped}'"
        cmd += " -delete"
        cmd += " || true"
        return cmd

    def count(self) -> str:
        """Generate find command that counts matching files."""
        if not self._directory:
            raise ValueError("Directory must be set")
        cmd = f"find {shlex.quote(self._directory)}"
        if self._maxdepth is not None:
            cmd += f" -maxdepth {self._maxdepth}"
        if self._type:
            cmd += f" -type {self._type}"
        if self._name:
            pattern_escaped = _escape_single_quotes(self._name)
            cmd += f" -name '{pattern_escaped}'"
        cmd += " -print | wc -l"
        return cmd

