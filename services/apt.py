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

    def _check_lock_status(self) -> tuple[bool, Optional[str], List[int]]:
        """
        Check if apt/dpkg locks are held before running commands.
        Returns:
            Tuple of (is_locked, lock_info, pids)
            is_locked: True if locks are held, False otherwise
            lock_info: Information about what's holding the lock, or None
            pids: List of PIDs holding locks
        """
        lock_info = []
        pids = []
        # Check each lock file individually using Python logic and CLI wrappers
        for lock_file in self.lock_files:
            # Check if file exists using FileOps wrapper
            file_ops = FileOps().suppress_errors()
            exists_cmd = file_ops.exists(lock_file)
            exists_output, exists_exit = self.ssh.execute(f"sudo -n {exists_cmd}", timeout=5, sudo=False)
            if exists_exit != 0 or not exists_output or "not_found" in exists_output:
                continue
            
            # File exists, check if process is holding it using Process wrapper
            # Try lsof first
            process = Process().suppress_errors()
            lsof_cmd = process.lsof_file(lock_file)
            lsof_output, lsof_exit = self.ssh.execute(f"sudo -n {lsof_cmd}", timeout=5, sudo=False)
            pid = None
            if lsof_exit == 0 and lsof_output and lsof_output.strip():
                pid_str = lsof_output.strip().split('\n')[0]
                try:
                    pid = int(pid_str)
                except ValueError:
                    pid = None
            
            # If lsof didn't work, try fuser
            if not pid:
                fuser_cmd = process.fuser_file(lock_file)
                fuser_output, fuser_exit = self.ssh.execute(f"sudo -n {fuser_cmd}", timeout=5, sudo=False)
                if fuser_exit == 0 and fuser_output and fuser_output.strip():
                    pid_str = fuser_output.strip().split('\n')[0]
                    try:
                        pid = int(pid_str)
                    except ValueError:
                        pid = None
            
            # If we found a PID, verify it's still running and get process name
            if pid:
                # Check if process exists using Process wrapper
                check_pid_cmd = process.check_pid(pid)
                check_output, check_exit = self.ssh.execute(f"sudo -n {check_pid_cmd}", timeout=5, sudo=False)
                if check_exit == 0 and check_output and "exists" in check_output:
                    # Get process name using Process wrapper
                    proc_name_cmd = process.get_process_name(pid)
                    proc_output, proc_exit = self.ssh.execute(f"sudo -n {proc_name_cmd}", timeout=5, sudo=False)
                    proc_name = "unknown"
                    if proc_exit == 0 and proc_output:
                        proc_name = proc_output.strip()
                    lock_info.append(f"{lock_file}: PID {pid} ({proc_name})")
                    if pid not in pids:
                        pids.append(pid)
        
        if lock_info:
            return True, "; ".join(lock_info), pids
        return False, None, []

    def _wait_for_lock_release(self, max_wait: Optional[int] = None, check_interval: int = 5) -> bool:
        """
        Wait for apt/dpkg locks to be released. Kills processes after timeout.
        Args:
            max_wait: Maximum time to wait in seconds (default: self.lock_wait)
            check_interval: How often to check in seconds
        Returns:
            True if locks were released, False if timeout and kill failed
        """
        max_wait = max_wait if max_wait is not None else self.lock_wait
        start_time = time.time()
        while time.time() - start_time < max_wait:
            is_locked, lock_info, pids = self._check_lock_status()
            if not is_locked:
                return True
            if lock_info:
                logger.info("Waiting for apt locks to be released: %s", lock_info)
            time.sleep(check_interval)
        
        # Timeout reached, kill processes holding locks
        is_locked, lock_info, pids = self._check_lock_status()
        if is_locked and pids:
            logger.warning("Timeout waiting for apt locks, killing processes: %s", lock_info)
            process = Process().signal(9).suppress_errors()
            for pid in pids:
                kill_cmd = process.kill(pid)
                self.ssh.execute(f"sudo -n {kill_cmd}", timeout=10, sudo=False)
                logger.info("Killed process PID %s", pid)
            # Wait a moment for locks to be released after killing processes
            time.sleep(2)
            is_locked, _, _ = self._check_lock_status()
            if not is_locked:
                logger.info("Locks released after killing processes")
                return True
            logger.error("Locks still held after killing processes")
            return False
        
        logger.error("Timeout waiting for apt locks to be released after %ss", max_wait)
        return False

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
        # First check if locks are held before attempting command
        is_locked, lock_info, _ = self._check_lock_status()
        if is_locked:
            logger.info("Apt locks detected before running command: %s", lock_info)
            if not self._wait_for_lock_release(wait_time):
                logger.error("Failed to wait for locks to be released, attempting cleanup")
                cleanup_cmd = self._build_cleanup_command()
                self.ssh.execute(f"sudo -n {cleanup_cmd}", timeout=60, sudo=False)
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
            # Check for locks before each attempt
            is_locked, lock_info, _ = self._check_lock_status()
            if is_locked:
                logger.info("Apt locks detected before attempt %s/%s: %s", attempt, retries, lock_info)
                if not self._wait_for_lock_release(max_wait=delay * 2):
                    logger.warning("Locks still held after wait, attempting cleanup")
                    cleanup_cmd = self._build_cleanup_command()
                    self.ssh.execute(f"sudo -n {cleanup_cmd}", timeout=60, sudo=False)
                    time.sleep(delay)
            output, exit_code = self.ssh.execute(f"sudo -n {command}", timeout=timeout, sudo=False)
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

    def execute(self, command: str, timeout: Optional[int] = None) -> Optional[str]:
        """
        Execute an apt/dpkg command with lock waiting and retries.
        Args:
            command: The apt/dpkg command to execute (generated by Apt CLI)
            timeout: Optional timeout override
        Returns:
            Command output or None if failed
        """
        timeout = timeout if timeout is not None else self.long_timeout
        
        # Check if locks are held before executing
        is_locked, lock_info, _ = self._check_lock_status()
        if is_locked:
            logger.info("Apt locks detected before execution: %s", lock_info)
            if not self._wait_for_lock_release():
                logger.error("Failed to wait for locks to be released")
                return None
        
        # Now execute the command
        success, update_output = self._wait_for_package_manager()
        if not success:
            return None
        command_stripped = command.strip()
        if command_stripped.startswith("apt ") or command_stripped.startswith("apt-get "):
            parts = command_stripped.split()
            if len(parts) >= 2 and parts[1] == "update":
                return update_output
        return self._run_with_lock_retry(command, timeout=timeout)