"""
Common functions used by both container and template modules
"""

import base64
import logging
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import paramiko

    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

# Get logger for this module
logger = logging.getLogger(__name__)


def _read_paramiko_output(stdout, stderr, channel, capture_output, exec_timeout):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """Read output from paramiko channels with timeout handling."""
    output_lines = []
    error_lines = []
    last_output_time = time.time()

    # Set channels to non-blocking
    channel.setblocking(0)

    while True:
        current_time = time.time()
        received_output = False

        # Check if channel is closed and exit status is ready
        if channel.exit_status_ready():
            break

        # Read available data from stdout
        if channel.recv_ready():
            try:
                data = stdout.read(4096).decode("utf-8", errors="replace")
                if data:
                    received_output = True
                    last_output_time = current_time
                    if capture_output:
                        output_lines.append(data)
                    else:
                        # Print interactively
                        sys.stdout.write(data)
                        sys.stdout.flush()
            except (IOError, OSError, UnicodeDecodeError):
                pass

        # Read available data from stderr
        if channel.recv_stderr_ready():
            try:
                data = stderr.read(4096).decode("utf-8", errors="replace")
                if data:
                    received_output = True
                    last_output_time = current_time
                    if capture_output:
                        error_lines.append(data)
                    else:
                        # Print interactively
                        sys.stderr.write(data)
                        sys.stderr.flush()
            except (IOError, OSError, UnicodeDecodeError):
                pass

        # Check for timeout only if no output received
        # Timeout starts counting from last output received
        if not received_output:
            time_since_last_output = current_time - last_output_time
            if time_since_last_output > exec_timeout:
                logger.error(
                    "SSH command timeout after %ss of no output - COMMAND FAILED",
                    exec_timeout,
                )
                channel.close()
                return None, None, True  # timeout occurred

        # Small sleep to avoid busy waiting
        time.sleep(0.05)

    # Get remaining output
    if capture_output:
        try:
            remaining = stdout.read().decode("utf-8", errors="replace")
            if remaining:
                output_lines.append(remaining)
            remaining_err = stderr.read().decode("utf-8", errors="replace")
            if remaining_err:
                error_lines.append(remaining_err)
        except (IOError, OSError, UnicodeDecodeError):
            pass
        output = "".join(output_lines).strip()
        error_output = "".join(error_lines).strip()
    else:
        output = None
        error_output = None

    return output, error_output, False  # no timeout


def ssh_exec(  # pylint: disable=too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches
    host,
    command,
    check=True,
    capture_output=False,
    timeout=None,
    cfg=None,
    force_tty=False,
):
    """Execute command via SSH using paramiko if available, fallback to subprocess"""
    if HAS_PARAMIKO and cfg:
        try:
            # Parse host (format: user@host or just host)
            if "@" in host:
                username, hostname = host.split("@", 1)
            else:
                username = "root"
                hostname = host

            connect_timeout = (
                cfg.ssh.connect_timeout if cfg and hasattr(cfg, "ssh") else 10
            )
            exec_timeout = timeout if timeout else 300

            # Create SSH client
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect
            client.connect(
                hostname=hostname,
                username=username,
                timeout=connect_timeout,
                look_for_keys=True,
                allow_agent=True,
            )

            # Execute command
            _, stdout, stderr = client.exec_command(command, timeout=exec_timeout)
            channel = stdout.channel

            # Read output interactively to show progress and prevent blocking
            output, error_output, timed_out = _read_paramiko_output(
                stdout, stderr, channel, capture_output, exec_timeout
            )
            if timed_out:
                client.close()
                if capture_output:
                    return None
                return False

            # Get exit status
            exit_status = channel.recv_exit_status()
            client.close()

            if capture_output:
                if exit_status != 0 and check:
                    raise subprocess.CalledProcessError(
                        exit_status, command, output, error_output
                    )
                return output
            if exit_status != 0 and check:
                raise subprocess.CalledProcessError(exit_status, command)
            return exit_status == 0

        except paramiko.SSHException:
            if capture_output:
                return None
            if check:
                raise
            return False
        except (paramiko.SSHException, paramiko.AuthenticationException, OSError):
            # Fallback to subprocess if paramiko fails
            if capture_output:
                pass  # Will fall through to subprocess
            elif check:
                raise
            else:
                return False

    # Fallback to subprocess if paramiko not available or failed
    connect_timeout = cfg.ssh.connect_timeout if cfg and hasattr(cfg, "ssh") else 10
    batch_mode = (
        "yes" if (cfg and hasattr(cfg, "ssh") and cfg.ssh.batch_mode) else "no"
    )
    tty_flag = "-tt" if force_tty else ""
    cmd = (
        f'ssh -o ConnectTimeout={connect_timeout} -o BatchMode={batch_mode} '
        f'{tty_flag} {host} "{command}"'
    ).strip()
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True,
            timeout=timeout,
        )
        if capture_output:
            return result.stdout.strip()
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error("SSH command timed out - COMMAND FAILED")
        if capture_output:
            return None
        return False
    except subprocess.CalledProcessError:
        if capture_output:
            return None
        return False


def _validate_pct_command(command, capture_output):
    """Validate command before execution."""
    if not command or not isinstance(command, str):
        logger.error("Invalid command passed to pct_exec: %s", command)
        if capture_output:
            return None
        return False

    # Check for potentially dangerous shell characters
    if "\x00" in command:
        logger.error("Command contains null byte, which is not allowed")
        if capture_output:
            return None
        return False
    return True


def _encode_pct_command(command, capture_output):
    """Encode command using base64."""
    try:
        return base64.b64encode(command.encode("utf-8")).decode("ascii")
    except (UnicodeEncodeError, ValueError) as exc:
        logger.error("Failed to encode command: %s", exc)
        if capture_output:
            return None
        return False


def _build_pct_ssh_cmd(proxmox_host, container_id, encoded_cmd, cfg):
    """Build SSH command for pct exec."""
    connect_timeout = cfg.ssh.connect_timeout if cfg and hasattr(cfg, "ssh") else 10
    batch_mode = (
        "yes" if (cfg and hasattr(cfg, "ssh") and cfg.ssh.batch_mode) else "no"
    )
    return (
        f"ssh -o ConnectTimeout={connect_timeout} -o BatchMode={batch_mode} "
        f"{proxmox_host} 'pct exec {container_id} -- bash -c "
        f'"echo {encoded_cmd} | base64 -d | bash"\''
    )


def _validate_ssh_cmd(cmd, capture_output):
    """Validate SSH command for quote issues."""
    single_quotes = cmd.count("'")
    if single_quotes % 2 != 0:
        logger.error(
            "Malformed SSH command detected (unmatched quotes): %s...", cmd[:100]
        )
        if capture_output:
            return None
        return False
    return True


def pct_exec(  # pylint: disable=too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches,too-many-statements
    proxmox_host,
    container_id,
    command,
    check=True,
    capture_output=False,
    timeout=30,
    cfg=None,
):
    """Execute command in container via pct exec"""
    # Validate command before sending
    validation_result = _validate_pct_command(command, capture_output)
    if validation_result is not True:
        return validation_result

    # Use base64 encoding to avoid quote escaping issues
    encoded_cmd = _encode_pct_command(command, capture_output)
    if encoded_cmd is False or encoded_cmd is None:
        return encoded_cmd

    # Build and validate SSH command
    cmd = _build_pct_ssh_cmd(proxmox_host, container_id, encoded_cmd, cfg)
    validation_result = _validate_ssh_cmd(cmd, capture_output)
    if validation_result is not True:
        return validation_result

    process = None
    try:
        # Always stream output to console, but also capture if requested
        # pylint: disable=consider-using-with
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        output_lines = []

        # Flag to track if we got output
        got_output = threading.Event()
        output_lock = threading.Lock()

        def read_output():
            """Read output in a separate thread"""
            nonlocal output_lines
            try:
                for line in iter(process.stdout.readline, ""):
                    if line:
                        with output_lock:
                            # Stream to console
                            sys.stdout.write(line)
                            sys.stdout.flush()

                            # Capture if requested
                            if capture_output:
                                output_lines.append(line)

                            got_output.set()
            except (IOError, OSError):
                pass

        # Start reading output in background thread
        reader_thread = threading.Thread(target=read_output, daemon=True)
        reader_thread.start()

        start_time = time.time()
        last_output_time = start_time

        # Monitor process and timeout
        while True:
            current_time = time.time()

            # Check if process finished first (before checking timeout)
            if process.poll() is not None:
                break

            # Check if we got output recently
            if got_output.is_set():
                last_output_time = current_time
                got_output.clear()

            # Check for timeout (only if no output received for timeout duration
            # AND process still running)
            if timeout and (current_time - last_output_time) > timeout:
                # Double-check process is still running before killing
                if process.poll() is None:
                    process.kill()
                    logger.error(
                        "pct_exec timed out after %ss of no output for "
                        "container %s - COMMAND FAILED",
                        timeout,
                        container_id,
                    )
                    if capture_output:
                        return None
                    return False
                # Process finished, break out of loop
                break

            time.sleep(0.1)

        # Wait for reader thread to finish reading all output
        reader_thread.join(timeout=2)

        # Get return code
        returncode = process.wait()

        # Get any remaining output that might not have been read
        try:
            remaining = process.stdout.read()
            if remaining:
                sys.stdout.write(remaining)
                sys.stdout.flush()
                if capture_output:
                    with output_lock:
                        output_lines.append(remaining)
        except (IOError, OSError):
            pass

        if capture_output:
            with output_lock:
                output = "".join(output_lines).strip()

            # If command failed (non-zero exit) and we have no output, log error
            if returncode != 0 and not output:
                logger.error(
                    "pct_exec command failed with exit code %s and no output "
                    "for container %s",
                    returncode,
                    container_id,
                )
                logger.error("Command was: %s...", command[:200])
                return None

            # If command failed but we have output, log warning but return output
            if returncode != 0 and output:
                logger.warning(
                    "pct_exec command failed with exit code %s for container "
                    "%s, but has output",
                    returncode,
                    container_id,
                )

            # Return empty string for successful commands with no output, not None
            # None should only be returned for actual errors/timeouts
            return output

        if check and returncode != 0:
            output_for_error = None
            if capture_output:
                with output_lock:
                    output_for_error = "".join(output_lines)
            raise subprocess.CalledProcessError(
                returncode, cmd, output=output_for_error
            )

        return returncode == 0
    except KeyboardInterrupt:
        if process:
            process.kill()
        logger.warning(
            "pct_exec interrupted by user for container %s", container_id
        )
        if capture_output:
            return None
        return False
    except (OSError, subprocess.SubprocessError) as exc:
        if process:
            process.kill()
        logger.error(
            "pct_exec command failed with exception for container %s: %s",
            container_id,
            exc,
        )
        if capture_output:
            return None
        return False
    finally:
        # Ensure process is cleaned up
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()


def container_exists(proxmox_host, container_id, cfg=None):
    """Check if container exists"""
    container_id_str = str(container_id)
    result = ssh_exec(
        proxmox_host,
        f"pct list | grep '^{container_id_str} '",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    return result is not None and container_id_str in result


def destroy_container(proxmox_host, container_id, cfg=None):
    """Destroy container if it exists"""
    # Check if container exists
    container_id_str = str(container_id)
    check_result = ssh_exec(
        proxmox_host,
        f"pct list | grep '^{container_id_str} '",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if not check_result or container_id_str not in check_result:
        logger.info("Container %s does not exist, skipping", container_id)
        return

    logger.info("Stopping container %s...", container_id)
    ssh_exec(
        proxmox_host,
        f"pct stop {container_id} 2>/dev/null || true",
        check=False,
        cfg=cfg,
    )
    time.sleep(2)  # Give it time to stop

    logger.info("Destroying container %s...", container_id)
    ssh_exec(
        proxmox_host,
        f"pct destroy {container_id} 2>&1",
        check=False,
        capture_output=True,
        cfg=cfg,
    )

    # Verify it's actually destroyed
    verify_result = ssh_exec(
        proxmox_host,
        f"pct list | grep '^{container_id_str} '",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if verify_result and container_id_str in verify_result:
        logger.warning(
            "Container %s still exists, forcing destruction...", container_id_str
        )
        ssh_exec(
            proxmox_host,
            f"pct destroy {container_id_str} --force 2>&1 || true",
            check=False,
            cfg=cfg,
        )
        time.sleep(1)

    # Final verification
    final_check = ssh_exec(
        proxmox_host,
        f"pct list | grep '^{container_id_str} '",
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if not final_check or container_id_str not in final_check:
        logger.info("Container %s destroyed", container_id_str)
    else:
        logger.error(
            "Container %s still exists after destruction attempt", container_id_str
        )


def wait_for_container(  # pylint: disable=too-many-arguments
    proxmox_host,
    container_id,
    ip_address,
    max_attempts=None,
    sleep_interval=None,
    cfg=None,
):
    """Wait for container to be ready"""
    if max_attempts is None:
        max_attempts = (
            cfg.waits.container_ready_max_attempts
            if cfg and hasattr(cfg, "waits")
            else 30
        )
    if sleep_interval is None:
        sleep_interval = (
            cfg.waits.container_ready_sleep if cfg and hasattr(cfg, "waits") else 3
        )
    for i in range(1, max_attempts + 1):
        status = ssh_exec(
            proxmox_host,
            f"pct status {container_id} 2>&1",
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if "running" in status:
            # Try ping
            try:
                ping_result = subprocess.run(
                    f"ping -c 1 -W 2 {ip_address}",
                    shell=True,
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
                if ping_result.returncode == 0:
                    logger.info("Container is up!")
                    return True
            except (subprocess.TimeoutExpired, OSError):
                pass

            # Try SSH via pct exec (more reliable than direct SSH)
            try:
                test_result = pct_exec(
                    proxmox_host,
                    container_id,
                    "echo test",
                    check=False,
                    capture_output=True,
                    timeout=5,
                    cfg=cfg,
                )
                if test_result == "test":
                    logger.info("Container is up (pct exec working)!")
                    return True
            except (OSError, subprocess.SubprocessError):
                pass

            # Try SSH directly (fallback)
            try:
                connect_timeout = (
                    cfg.ssh.connect_timeout if cfg and hasattr(cfg, "ssh") else 3
                )
                ssh_cmd = (
                    f'ssh -o ConnectTimeout={connect_timeout} -o BatchMode=yes '
                    f'-o StrictHostKeyChecking=no root@{ip_address} "echo test"'
                )
                ssh_result = subprocess.run(
                    ssh_cmd,
                    shell=True,
                    capture_output=True,
                    timeout=connect_timeout,
                    check=False,
                )
                if ssh_result.returncode == 0:
                    logger.info("Container is up (SSH working)!")
                    return True
            except (subprocess.TimeoutExpired, OSError):
                pass

        logger.debug("Waiting... (%s/%s)", i, max_attempts)
        time.sleep(sleep_interval)

    logger.warning("Container may not be fully ready, but continuing...")
    return True  # Continue anyway


def get_ssh_key():
    """Get SSH public key"""
    key_paths = [
        Path.home() / ".ssh" / "id_rsa.pub",
        Path.home() / ".ssh" / "id_ed25519.pub",
    ]
    for key_path in key_paths:
        if key_path.exists():
            return key_path.read_text().strip()
    return ""


def setup_ssh_key(proxmox_host, container_id, ip_address, cfg=None):
    """Setup SSH key in container"""
    ssh_key = get_ssh_key()
    if not ssh_key:
        return False

    default_user = cfg.users.default_user if cfg and hasattr(cfg, "users") else "jaal"

    # Remove old host key
    subprocess.run(
        f"ssh-keygen -R {ip_address} 2>/dev/null", shell=True, check=False
    )

    # Use printf to safely write SSH key without quote issues
    # Base64 encode the key to avoid any shell escaping problems
    key_b64 = base64.b64encode(ssh_key.encode("utf-8")).decode("ascii")

    # Add to default user - use base64 decode to avoid quote issues
    user_cmd = (
        f"mkdir -p /home/{default_user}/.ssh && echo {key_b64} | base64 -d > "
        f"/home/{default_user}/.ssh/authorized_keys && "
        f"chmod 600 /home/{default_user}/.ssh/authorized_keys && "
        f"chown {default_user}:{default_user} /home/{default_user}/.ssh/authorized_keys"
    )
    pct_exec(proxmox_host, container_id, user_cmd, check=False, cfg=cfg)

    # Add to root user - use base64 decode to avoid quote issues
    root_cmd = (
        f"mkdir -p /root/.ssh && echo {key_b64} | base64 -d > "
        f"/root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
    )
    pct_exec(proxmox_host, container_id, root_cmd, check=False, cfg=cfg)

    # Verify the key file exists
    verify_cmd = (
        f"test -f /home/{default_user}/.ssh/authorized_keys && "
        f"test -f /root/.ssh/authorized_keys && echo 'keys_exist' || echo 'keys_missing'"
    )
    verify_result = pct_exec(
        proxmox_host,
        container_id,
        verify_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if verify_result and "keys_exist" in verify_result:
        logger.info("SSH key setup verified successfully")
        return True
    logger.error("SSH key verification failed: %s", verify_result)
    return False
