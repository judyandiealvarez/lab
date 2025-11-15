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
        return self.success
    
    @property
    def failed(self) -> bool:
        return not self.success
    
    @property
    def has_error(self) -> bool:
        return self.error_type != ErrorType.NONE


class CommandWrapper:
    """Base wrapper for CLI commands - generates command strings and parses results"""
    
    # Error patterns: (pattern, error_type, description)
    ERROR_PATTERNS = [
        # Timeout errors
        (r'timeout|timed out|time out', ErrorType.TIMEOUT, 'Command timed out'),
        
        # Connection errors (exclude logger/syslog warnings)
        (r'(?<!logger: socket )(?<!syslog: )(?<!journal: )connection (?:refused|reset|closed|failed)|unable to connect|cannot connect|connection error',
         ErrorType.CONNECTION_ERROR, 'Connection error'),
        (r'ssh.*connection.*refused|ssh.*connection.*closed',
         ErrorType.CONNECTION_ERROR, 'SSH connection error'),
        
        # Permission errors
        (r'permission denied|access denied|operation not permitted|eacces',
         ErrorType.PERMISSION_DENIED, 'Permission denied'),
        (r'cannot open.*permission denied',
         ErrorType.PERMISSION_DENIED, 'File permission denied'),
        
        # Not found errors
        (r'not found|no such file|no such directory|command not found|file not found',
         ErrorType.NOT_FOUND, 'Resource not found'),
        (r'container.*not found|container.*does not exist',
         ErrorType.NOT_FOUND, 'Container not found'),
        
        # Already exists
        (r'already exists|already in use|already running|already part',
         ErrorType.ALREADY_EXISTS, 'Resource already exists'),
        
        # Invalid argument
        (r'invalid (?:argument|option|parameter)|bad argument|unknown option',
         ErrorType.INVALID_ARGUMENT, 'Invalid argument'),
        
        # Resource exhausted
        (r'no space left|disk full|out of memory|resource.*unavailable',
         ErrorType.RESOURCE_EXHAUSTED, 'Resource exhausted'),
        
        # Service errors
        (r'service.*failed|service.*error|systemctl.*failed|failed to start.*service',
         ErrorType.SERVICE_ERROR, 'Service error'),
        (r'failed to start|failed to stop|failed to restart',
         ErrorType.SERVICE_ERROR, 'Service operation failed'),
        
        # Package errors
        (r'package.*not found|unable to locate package|package.*unavailable',
         ErrorType.PACKAGE_ERROR, 'Package error'),
        (r'e:\s*unable to|e:\s*package|e:\s*error',
         ErrorType.PACKAGE_ERROR, 'APT package error'),
        (r'failed to fetch|unable to fetch|404 not found.*package',
         ErrorType.PACKAGE_ERROR, 'Package fetch error'),
        
        # Network errors
        (r'network.*error|network.*unreachable|no route to host',
         ErrorType.NETWORK_ERROR, 'Network error'),
        (r'failed to fetch.*http|unable to connect.*http',
         ErrorType.NETWORK_ERROR, 'HTTP connection error'),
        
        # Generic command failures
        (r'error|failed|failure|fatal',
         ErrorType.COMMAND_FAILED, 'Command failed'),
    ]
    
    @staticmethod
    def parse_result(output: Optional[str], exit_code: Optional[int] = None) -> CommandResult:
        """
        Parse command output and return structured result
        
        Args:
            output: Command output (stdout/stderr combined)
            exit_code: Exit code if available
            
        Returns:
            CommandResult object
        """
        sanitized_output = CommandWrapper._sanitize_output_for_error_detection(output) if output else output
        error_type, error_msg = CommandWrapper._parse_error(output, exit_code)
        
        # Determine success: no error type or already exists (sometimes OK)
        success = (error_type == ErrorType.NONE or 
                  error_type == ErrorType.ALREADY_EXISTS)
        
        # But check for explicit failures in output
        if sanitized_output and any(word in sanitized_output.lower() for word in ['error', 'failed', 'failure']):
            # Double-check: might be false positive
            if error_type == ErrorType.NONE:
                error_type = ErrorType.COMMAND_FAILED
                error_msg = "Command output contains error indicators"
                success = False
        
        return CommandResult(
            success=success,
            output=output,
            error_type=error_type,
            error_message=error_msg,
            exit_code=exit_code if exit_code is not None else (0 if success else 1)
        )
    
    @staticmethod
    def _parse_error(output: Optional[str], exit_code: Optional[int] = None) -> tuple[ErrorType, Optional[str]]:
        """
        Parse command output to identify error type and message
        
        Args:
            output: Command output (stdout/stderr combined)
            exit_code: Exit code if available
            
        Returns:
            Tuple of (ErrorType, error_message)
        """
        # None output means actual error/timeout - only treat as error if exit_code indicates failure
        # Empty string output means successful command with no output - not an error
        if output is None:
            # Only treat as error if exit_code indicates failure or is unknown (timeout)
            if exit_code is None:
                return ErrorType.TIMEOUT, "Command produced no output (possible timeout)"
            elif exit_code != 0:
                return ErrorType.COMMAND_FAILED, "Command failed with no output"
            # If exit_code is 0 but output is None, something went wrong (shouldn't happen)
            return ErrorType.UNKNOWN, "Command produced no output"
        
        # Empty string is valid - command succeeded with no output
        if output == "":
            return ErrorType.NONE, None
        
        analysis_output = CommandWrapper._sanitize_output_for_error_detection(output)
        output_lower = analysis_output.lower()
        
        # Check exit code first
        if exit_code is not None and exit_code != 0:
            # Try to identify specific error from output
            for pattern, error_type, description in CommandWrapper.ERROR_PATTERNS:
                if re.search(pattern, output_lower, re.IGNORECASE):
                    # Extract relevant error message (last 200 chars or specific line)
                    error_msg = CommandWrapper._extract_error_message(output, pattern)
                    return error_type, error_msg or description
            return ErrorType.COMMAND_FAILED, f"Command failed with exit code {exit_code}"
        
        # Even with exit code 0, check for error indicators in output
        for pattern, error_type, description in CommandWrapper.ERROR_PATTERNS:
            if re.search(pattern, output_lower, re.IGNORECASE):
                error_msg = CommandWrapper._extract_error_message(output, pattern)
                return error_type, error_msg or description
        
        return ErrorType.NONE, None

    @staticmethod
    def _sanitize_output_for_error_detection(output: Optional[str]) -> Optional[str]:
        """Remove known benign lines that frequently trigger false positives."""
        if not output:
            return output
        ansi_pattern = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')
        noise_prefixes = (
            'logger: socket /dev/log',
            'logging to syslog failed',
            'locale:',
            'perl: warning:',
            'apparmor_parser:',
        )
        sanitized_lines = []
        for raw_line in output.splitlines():
            line_no_ansi = ansi_pattern.sub('', raw_line)
            stripped = line_no_ansi.strip()
            lower = stripped.lower()
            if any(lower.startswith(prefix) for prefix in noise_prefixes):
                continue
            if 'error: at least one profile failed to load' in lower:
                continue
            sanitized_lines.append(line_no_ansi)
        return "\n".join(sanitized_lines)
    
    @staticmethod
    def _extract_error_message(output: str, pattern: str) -> Optional[str]:
        """Extract relevant error message from output"""
        lines = output.split('\n')
        for line in lines:
            if re.search(pattern, line, re.IGNORECASE):
                # Return the line, truncated if too long
                msg = line.strip()
                if len(msg) > 200:
                    msg = msg[:197] + "..."
                return msg
        
        # If no specific line found, return last part of output
        if len(output) > 200:
            return output[-197:] + "..."
        return output.strip() if output.strip() else None
