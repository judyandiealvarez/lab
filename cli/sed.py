"""
Sed command wrappers with fluent API
"""
import shlex
from .base import CommandWrapper

def _escape_single_quotes(value: str) -> str:
    return value.replace("'", "'\"'\"'")

def _escape_delimiter(value: str, delimiter: str) -> str:
    return value.replace(delimiter, f"\\{delimiter}")

class Sed(CommandWrapper):
    """Wrapper for sed commands with fluent API"""
    def __init__(self):
        """Initialize with default settings"""
        self._delimiter: str = "/"
        self._flags: str = "g"

    def delimiter(self, value: str) -> "Sed":
        """Set delimiter (returns self for chaining)."""
        self._delimiter = value
        return self

    def flags(self, value: str) -> "Sed":
        """Set flags (returns self for chaining)."""
        self._flags = value
        return self

    def replace(self, path: str, search: str, replacement: str) -> str:
        """Generate sed command to replace text in a file."""
        escaped_search = _escape_delimiter(_escape_single_quotes(search), self._delimiter)
        escaped_replacement = _escape_delimiter(_escape_single_quotes(replacement), self._delimiter)
        expression = (
            f"s{self._delimiter}"
            f"{escaped_search}{self._delimiter}"
            f"{escaped_replacement}{self._delimiter}{self._flags}"
        )
        return f"sed -i '{expression}' {shlex.quote(path)} 2>&1"
