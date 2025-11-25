"""
APT Service - manages apt/dpkg operations via SSH, wraps Apt CLI
"""
import logging
import time
from typing import Optional, List, Dict, Tuple
from .ssh import SSHService
from cli.apt import Apt
from cli import FileOps, Dpkg, Process, Sed
logger = logging.getLogger(__name__)
APT_LONG_TIMEOUT = 600
APT_LOCK_WAIT = 600
APT_LOCK_PATTERNS = [
    "could not get lock",
    "unable to lock",
    "resource temporarily unavailable",
    "is another process using it",
]
APT_REPOSITORY_ERROR_PATTERNS = [
    "no longer has a Release file",
    "404  Not Found",
    "Release' no longer has",
    "oracular",
]

class APTService:
    """Service for managing apt/dpkg operations via SSH - wraps Apt CLI with execution"""
    def __init__(
        self,
        ssh_service: SSHService,
        lock_wait: int = APT_LOCK_WAIT,
        long_timeout: int = APT_LONG_TIMEOUT,
        cleanup_processes: Optional[List[str]] = None,
        cleanup_patterns: Optional[List[str]] = None,
        lock_files: Optional[List[str]] = None,
    ):
        self.ssh = ssh_service
        self.lock_wait = lock_wait
        self.long_timeout = long_timeout
        # Configurable cleanup settings
        self.cleanup_processes = cleanup_processes or [
            "apt",
            "apt-get",
            "apt-cache",
            "dpkg",
            "unattended-upgrade",
        ]
        self.cleanup_patterns = cleanup_patterns or ["unattended-upgrade", "apt.systemd.daily"]
        self.lock_files = lock_files or [
            "/var/lib/dpkg/lock-frontend",
            "/var/lib/dpkg/lock",
            "/var/lib/apt/lists/lock",
        ]

    def _build_cleanup_command(self) -> str:
        """Build cleanup command using CLI wrappers."""
        pkill_commands = [Process().signal(9).suppress_errors().pkill(name) for name in self.cleanup_processes]
        pkill_patterns = [
            Process().signal(9).full_match().suppress_errors().pkill(pattern) for pattern in self.cleanup_patterns
        ]
        rm_commands = [FileOps().force().remove(f) for f in self.lock_files]
        dpkg_configure = Dpkg().all().log_file("/tmp/dpkg-configure.log").suppress_errors().configure()
        parts = pkill_commands + pkill_patterns + rm_commands + [dpkg_configure, "echo apt_cleanup_done"]
        return " && ".join(parts)

    def _fix_apt_sources(self) -> bool:
        """Fix apt sources.list by replacing invalid codenames."""
        logger.info("Fixing apt sources.list...")
        sed_cmds = [
            Sed().replace("/etc/apt/sources.list", "oracular", "plucky"),
            Sed().delimiter("|").replace("/etc/apt/sources.list", "old-releases.ubuntu.com", "archive.ubuntu.com"),
        ]
        for idx, sed_cmd in enumerate(sed_cmds, start=1):
            output, exit_code = self.ssh.execute(f"sudo -n {sed_cmd}", timeout=30, sudo=False)
            if exit_code is not None and exit_code != 0:
                logger.warning("Fix apt sources step %s failed: %s", idx, output)
        return True

    def _detect_error_type(self, output: Optional[str], exit_code: Optional[int]) -> str:
        """Detect the type of error from apt command output."""
        if output is None:
            return "unknown"
        output_lower = output.lower()
        # Check for lock errors
        for pattern in APT_LOCK_PATTERNS:
            if pattern.lower() in output_lower:
                return "lock"
        # Check for repository errors
        for pattern in APT_REPOSITORY_ERROR_PATTERNS:
            if pattern.lower() in output_lower:
                return "repository"
        # Check exit code for common apt errors
        if exit_code == 100:
            return "repository"
        return "unknown"

    def _wait_for_package_manager(self, wait_time: Optional[int] = None) -> tuple[bool, Optional[str]]:
        """Use apt update to detect dpkg/apt locks."""
        wait_time = wait_time if wait_time is not None else self.lock_wait
        max_attempts = max(3, wait_time // 30)
        delay = 5
        # Use Apt CLI to generate check command
        check_apt = Apt()
        check_cmd = check_apt.use_apt_get().update()
        # Build cleanup command using CLI wrappers
        cleanup_cmd = self._build_cleanup_command()
        repository_fixed = False
        for attempt in range(1, max_attempts + 1):
            update_output, exit_code = self.ssh.execute(f"sudo -n {check_cmd} < /dev/null", timeout=self.long_timeout, sudo=False)
            if exit_code == 0:
                logger.info("apt update succeeded on attempt %s/%s", attempt, max_attempts)
                return True, update_output or ""
            if exit_code is not None and exit_code != 0:
                error_type = self._detect_error_type(update_output, exit_code)
                if error_type == "lock":
                    logger.warning("apt update failed with lock error (attempt %s/%s). Retrying after cleanup.", attempt, max_attempts)
                    self.ssh.execute(f"sudo -n {cleanup_cmd}", timeout=60, sudo=False)
                    time.sleep(delay)
                    continue
                elif error_type == "repository":
                    if not repository_fixed:
                        logger.warning("apt update failed with repository error (attempt %s/%s). Fixing sources.list...", attempt, max_attempts)
                        self._fix_apt_sources()
                        repository_fixed = True
                        time.sleep(2)
                        continue
                    else:
                        logger.error("apt update failed with repository error after fix (attempt %s/%s): %s", attempt, max_attempts, update_output[-500:] if update_output else "No output")
                        return False, update_output
                else:
                    logger.error("apt update failed with unknown error (exit_code: %s, attempt %s/%s): %s", exit_code, attempt, max_attempts, update_output[-500:] if update_output else "No output")
                    return False, update_output
            logger.error("apt update failed with unexpected error (exit_code: %s)", exit_code)
            return False, update_output or ""
        logger.error("apt update never succeeded after %s attempts", max_attempts)
        return False, None

    def _run_with_lock_retry(self, command: str, timeout: Optional[int] = None, retries: int = 6, delay: int = 10
    ) -> Optional[str]:
        """Execute apt command with retries when lock contention occurs."""
        timeout = timeout if timeout is not None else self.long_timeout
        repository_fixed = False
        for attempt in range(1, retries + 1):
            output, exit_code = self.ssh.execute(f"sudo -n {command} < /dev/null", timeout=timeout, sudo=False)
            if exit_code == 0:
                return output or ""
            if exit_code is not None and exit_code != 0:
                error_type = self._detect_error_type(output, exit_code)
                if error_type == "lock":
                    logger.warning(
                        "Command failed with lock error while running %s (attempt %s/%s); waiting %ss",
                        command.split()[0],
                        attempt,
                        retries,
                        delay,
                    )
                    cleanup_cmd = self._build_cleanup_command()
                    self.ssh.execute(f"sudo -n {cleanup_cmd}", timeout=60, sudo=False)
                    time.sleep(delay)
                    continue
                elif error_type == "repository":
                    if not repository_fixed:
                        logger.warning("Command failed with repository error while running %s (attempt %s/%s). Fixing sources.list...", command.split()[0], attempt, retries)
                        self._fix_apt_sources()
                        repository_fixed = True
                        time.sleep(2)
                        continue
                    else:
                        logger.error("Command failed with repository error after fix (attempt %s/%s): %s", attempt, retries, output[-500:] if output else "No output")
                        return None
                else:
                    logger.error("Command failed with unknown error (exit_code: %s, attempt %s/%s): %s", exit_code, attempt, retries, output[-500:] if output else "No output")
                    return None
        return None

    def _execute_command(self, command: str, timeout: Optional[int] = None) -> Optional[str]:
        """
        Execute an apt/dpkg command with lock waiting and retries.
        Args:
            command: The apt/dpkg command to execute
            timeout: Optional timeout override
        Returns:
            Command output or None if failed
        """
        timeout = timeout if timeout is not None else self.long_timeout
        success, update_output = self._wait_for_package_manager()
        if not success:
            return None
        command_stripped = command.strip()
        if command_stripped.startswith("apt ") or command_stripped.startswith("apt-get "):
            parts = command_stripped.split()
            if len(parts) >= 2 and parts[1] == "update":
                return update_output
        return self._run_with_lock_retry(command, timeout=timeout)

    def execute(self, command: str, timeout: Optional[int] = None) -> Optional[str]:
        """
        Execute an apt/dpkg command with lock waiting and retries.
        Args:
            command: The apt/dpkg command to execute (generated by Apt CLI)
            timeout: Optional timeout override
        Returns:
            Command output or None if failed
        """
        return self._execute_command(command, timeout=timeout)