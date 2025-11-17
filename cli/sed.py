"""
Sed command wrappers
"""

import shlex

from .base import CommandWrapper


def _escape_single_quotes(value: str) -> str:
    return value.replace("'", "'\"'\"'")


def _escape_delimiter(value: str, delimiter: str) -> str:
    return value.replace(delimiter, f"\\{delimiter}")


class Sed(CommandWrapper):
    """Wrapper for sed commands."""

    @staticmethod
    def replace_cmd(
        path: str,
        search: str,
        replacement: str,
        *,
        delimiter: str = "/",
        flags: str = "g",
    ) -> str:
        """Generate sed command to replace text in a file."""
        escaped_search = _escape_delimiter(_escape_single_quotes(search), delimiter)
        escaped_replacement = _escape_delimiter(
            _escape_single_quotes(replacement), delimiter
        )
        expression = (
            f"s{delimiter}"
            f"{escaped_search}{delimiter}"
            f"{escaped_replacement}{delimiter}{flags}"
        )
        return f"sed -i '{expression}' {shlex.quote(path)} 2>&1"
