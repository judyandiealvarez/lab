"""
Base command wrapper with error parsing and command generation
"""
import re
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Optional
logger = logging.getLogger(__name__)

class ErrorType(Enum):
    """Error types that can be detected in command output"""
    NONE = "none"
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    PERMISSION_DENIED = "permission_denied"
    NOT_FOUND = "not_found"
    ALREADY_EXISTS = "already_exists"
    INVALID_ARGUMENT = "invalid_argument"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    COMMAND_FAILED = "command_failed"
    SERVICE_ERROR = "service_error"
    PACKAGE_ERROR = "package_error"
    NETWORK_ERROR = "network_error"
    UNKNOWN = "unknown"
@dataclass

class CommandResult:
    """Structured result from command execution"""
    success: bool
    output: Optional[str]
    error_type: ErrorType
    error_message: Optional[str]
    exit_code: Optional[int]

    def __bool__(self):
        """Allow truthiness check: True when command succeeded."""
        return self.success
    @property

    def failed(self) -> bool:
        """Convenience property: True when command failed."""
        return not self.success
    @property

    def has_error(self) -> bool:
        """Whether command parsing detected an error."""
        return self.error_type != ErrorType.NONE

class CommandWrapper:  # pylint: disable=too-few-public-methods
    """Base wrapper for CLI commands - generates command strings and parses results"""
    def __init__(self) -> None:
        """Prevent direct instantiation; subclasses should be static collections."""
        raise RuntimeError("CommandWrapper should not be instantiated")
    # Error patterns: (pattern, error_type, description)
    ERROR_PATTERNS = [
        # Timeout errors
        (r"timeout|timed out|time out", ErrorType.TIMEOUT, "Command timed out"),
        # Connection errors (exclude logger/syslog warnings)
        (
            r"(?<!logger: socket )(?<!syslog: )(?<!journal: )"
            r"connection (?:refused|reset|closed|failed)|"
            r"unable to connect|cannot connect|connection error",
            ErrorType.CONNECTION_ERROR,
            "Connection error",
        ),
        (r"ssh.*connection.*refused|ssh.*connection.*closed", ErrorType.CONNECTION_ERROR, "SSH connection error"),
        # Permission errors
        (
            r"permission denied|access denied|operation not permitted|eacces",
            ErrorType.PERMISSION_DENIED,
            "Permission denied",
        ),
        (r"cannot open.*permission denied", ErrorType.PERMISSION_DENIED, "File permission denied"),
        # Not found errors
        (
            r"not found|no such file|no such directory|command not found|file not found",
            ErrorType.NOT_FOUND,
            "Resource not found",
        ),
        (r"container.*not found|container.*does not exist", ErrorType.NOT_FOUND, "Container not found"),
        # Already exists
        (
            r"already exists|already in use|already running|already part",
            ErrorType.ALREADY_EXISTS,
            "Resource already exists",
        ),
        # Invalid argument
        (
            r"invalid (?:argument|option|parameter)|bad argument|unknown option",
            ErrorType.INVALID_ARGUMENT,
            "Invalid argument",
        ),
        # Resource exhausted
        (
            r"no space left|disk full|out of memory|resource.*unavailable",
            ErrorType.RESOURCE_EXHAUSTED,
            "Resource exhausted",
        ),
        # Service errors
        (
            r"service.*failed|service.*error|systemctl.*failed|failed to start.*service",
            ErrorType.SERVICE_ERROR,
            "Service error",
        ),
        (r"failed to start|failed to stop|failed to restart", ErrorType.SERVICE_ERROR, "Service operation failed"),
        # Package errors
        (r"package.*not found|unable to locate package|package.*unavailable", ErrorType.PACKAGE_ERROR, "Package error"),
        (r"e:\s*unable to|e:\s*package|e:\s*error", ErrorType.PACKAGE_ERROR, "APT package error"),
        (r"failed to fetch|unable to fetch|404 not found.*package", ErrorType.PACKAGE_ERROR, "Package fetch error"),
        # Network errors
        (r"network.*error|network.*unreachable|no route to host", ErrorType.NETWORK_ERROR, "Network error"),
        (r"failed to fetch.*http|unable to connect.*http", ErrorType.NETWORK_ERROR, "HTTP connection error"),
        # Generic command failures (but not package names containing "error")
        (r"(?<![-a-z0-9])(error|failed|failure|fatal)(?![-a-z0-9])", ErrorType.COMMAND_FAILED, "Command failed"),
    ]
    @classmethod

    def parse_result(cls, output: Optional[str], exit_code: Optional[int] = None) -> CommandResult:
        """
        Parse command output and return structured result
        Args:
            output: Command output (stdout/stderr combined)
            exit_code: Exit code if available
        Returns:
            CommandResult object
        """
        sanitized_output = cls._sanitize_output_for_error_detection(output) if output else output
        error_type, error_msg = cls._parse_error(output, exit_code)
        # Determine success: no error type or already exists (sometimes OK)
        success = error_type in (ErrorType.NONE, ErrorType.ALREADY_EXISTS)
        # But check for explicit failures in output
        if sanitized_output:
            failure_indicator = re.search(
                r"(?<![A-Za-z0-9-])(error|failed|failure)(?![A-Za-z0-9-])",
                sanitized_output,
                re.IGNORECASE,
            )
            if failure_indicator:
                if error_type == ErrorType.NONE:
                    error_type = ErrorType.COMMAND_FAILED
                    error_msg = "Command output contains error indicators"
                    success = False
        return CommandResult(
            success=success,
            output=output,
            error_type=error_type,
            error_message=error_msg,
            exit_code=exit_code if exit_code is not None else (0 if success else 1),
        )
    @staticmethod

    def contains_token(output: Optional[str], token: str) -> bool:
        """Helper to check whether output contains a keyword."""
        if not output:
            return False
        return token.lower() in output.lower()
    @classmethod

    def _parse_error(cls, output: Optional[str], exit_code: Optional[int] = None) -> tuple[ErrorType, Optional[str]]:
        """
        Parse command output to identify error type and message
        Args:
            output: Command output (stdout/stderr combined)
            exit_code: Exit code if available
        Returns:
            Tuple of (ErrorType, error_message)
        """
        # None output means actual error/timeout - only treat as error if
        # exit_code indicates failure. Empty string output means success.
        result_type = ErrorType.NONE
        message = None
        if output is None:
            # Only treat as error if exit_code indicates failure or is unknown (timeout)
            if exit_code is None:
                result_type = ErrorType.TIMEOUT
                message = "Command produced no output (possible timeout)"
            elif exit_code != 0:
                result_type = ErrorType.COMMAND_FAILED
                message = "Command failed with no output"
            # If exit_code is 0 but output is None, something went wrong (shouldn't happen)
            else:
                result_type = ErrorType.UNKNOWN
                message = "Command produced no output"
            return result_type, message
        # Empty string is valid - command succeeded with no output
        if output == "":
            return ErrorType.NONE, None
        analysis_output = CommandWrapper._sanitize_output_for_error_detection(output)
        output_lower = analysis_output.lower()
        # Check exit code first
        if exit_code is not None and exit_code != 0:
            # Try to identify specific error from output
            for pattern, error_type, description in cls.ERROR_PATTERNS:
                if re.search(pattern, output_lower, re.IGNORECASE):
                    error_msg = cls._extract_error_message(output, pattern)
                    return error_type, error_msg or description
            return (ErrorType.COMMAND_FAILED, f"Command failed with exit code {exit_code}")
        # Even with exit code 0, check for error indicators in output
        for pattern, error_type, description in cls.ERROR_PATTERNS:
            if re.search(pattern, output_lower, re.IGNORECASE):
                error_msg = cls._extract_error_message(output, pattern)
                return error_type, error_msg or description
        return ErrorType.NONE, None
    @staticmethod

    def _sanitize_output_for_error_detection(output: Optional[str]) -> Optional[str]:
        """Remove known benign lines that frequently trigger false positives."""
        if not output:
            return output
        ansi_pattern = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
        noise_prefixes = (
            "logger: socket /dev/log",
            "logging to syslog failed",
            "locale:",
            "perl: warning:",
            "apparmor_parser:",
            "libgpg-error-l10n",
            "ssl-cert",
        )
        sanitized_lines = []
        for raw_line in output.splitlines():
            line_no_ansi = ansi_pattern.sub("", raw_line)
            stripped = line_no_ansi.strip()
            lower = stripped.lower()
            if any(lower.startswith(prefix) for prefix in noise_prefixes):
                continue
            if "error: at least one profile failed to load" in lower:
                continue
            if "setting locale failed" in lower or "perl: warning:" in lower:
                continue
            if "pg_lsclusters: not found" in lower:
                continue
            # Skip lines that are just package names (e.g., "libgpg-error-l10n        rsyslog")
            if stripped and not any(c in stripped for c in ":()[]{}"):
                words = stripped.split()
                # If line contains known package names and looks like a package list
                if any(word in ("libgpg-error-l10n", "ssl-cert", "rsyslog") for word in words) and len(words) <= 5:
                    continue
            sanitized_lines.append(line_no_ansi)
        return "\n".join(sanitized_lines)
    @staticmethod

    def _extract_error_message(output: str, pattern: str) -> Optional[str]:
        """Extract relevant error message from sanitized output"""
        # Sanitize output first to avoid false positives
        sanitized = CommandWrapper._sanitize_output_for_error_detection(output)
        lines = sanitized.split("\n")
        for line in lines:
            if re.search(pattern, line, re.IGNORECASE):
                # Return the line, truncated if too long
                msg = line.strip()
                if len(msg) > 200:
                    msg = msg[:197] + "..."
                return msg
        # If no specific line found, return last part of sanitized output
        if len(sanitized) > 200:
            return sanitized[-197:] + "..."
        return sanitized.strip() if sanitized.strip() else None