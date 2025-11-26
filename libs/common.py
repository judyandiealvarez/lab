"""
Common functions used by both container and template modules
"""
import base64
import logging
import os
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

def _read_paramiko_output(stdout, stderr, channel, exec_timeout
):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """Read output from paramiko channels with timeout handling - ALWAYS shows interactively AND captures."""
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
                    # ALWAYS show interactively AND capture
                    sys.stdout.write(data)
                    sys.stdout.flush()
                    output_lines.append(data)
            except (IOError, OSError, UnicodeDecodeError):
                pass
        # Read available data from stderr
        if channel.recv_stderr_ready():
            try:
                data = stderr.read(4096).decode("utf-8", errors="replace")
                if data:
                    received_output = True
                    last_output_time = current_time
                    # ALWAYS show interactively AND capture
                    sys.stderr.write(data)
                    sys.stderr.flush()
                    error_lines.append(data)
            except (IOError, OSError, UnicodeDecodeError):
                pass
        # Check for timeout only if no output received
        # Timeout starts counting from last output received
        if not received_output:
            time_since_last_output = current_time - last_output_time
            if time_since_last_output > exec_timeout:
                logger.error("SSH command timeout after %ss of no output - COMMAND FAILED", exec_timeout)
                channel.close()
                return None, None, True  # timeout occurred
        # Small sleep to avoid busy waiting
        time.sleep(0.05)
    # Get remaining output
    try:
        remaining = stdout.read().decode("utf-8", errors="replace")
        if remaining:
            sys.stdout.write(remaining)
            sys.stdout.flush()
            output_lines.append(remaining)
        remaining_err = stderr.read().decode("utf-8", errors="replace")
        if remaining_err:
            sys.stderr.write(remaining_err)
            sys.stderr.flush()
            error_lines.append(remaining_err)
    except (IOError, OSError, UnicodeDecodeError):
        pass
    output = "".join(output_lines).strip()
    error_output = "".join(error_lines).strip()
    return output, error_output, False  # no timeout

def ssh_exec(  # pylint: disable=too-many-arguments,too-many-locals,too-many-return-statements,too-many-branches
    host,
    command,
    check=True,
    timeout=None,
    cfg=None,
    force_tty=False,
):
    """Execute command via SSH - ALWAYS shows output interactively AND captures it"""
    # Use SSHService when cfg is provided and paramiko is available
    if HAS_PARAMIKO and cfg:
        try:
            from services.ssh import SSHService
            from libs.config import SSHConfig
            
            # Create SSH config from cfg
            ssh_config = SSHConfig(
                connect_timeout=cfg.ssh.connect_timeout if hasattr(cfg, "ssh") else 10,
                batch_mode=cfg.ssh.batch_mode if hasattr(cfg, "ssh") else False,
                default_exec_timeout=timeout if timeout else (cfg.ssh.default_exec_timeout if hasattr(cfg, "ssh") else 300),
                read_buffer_size=cfg.ssh.read_buffer_size if hasattr(cfg, "ssh") else 4096,
                poll_interval=cfg.ssh.poll_interval if hasattr(cfg, "ssh") else 0.05,
                default_username=cfg.ssh.default_username if hasattr(cfg, "ssh") else "root",
                look_for_keys=cfg.ssh.look_for_keys if hasattr(cfg, "ssh") else True,
                allow_agent=cfg.ssh.allow_agent if hasattr(cfg, "ssh") else True,
                verbose=cfg.ssh.verbose if hasattr(cfg, "ssh") and hasattr(cfg.ssh, "verbose") else False,
            )
            
            # Create SSH service and execute command
            ssh_service = SSHService(host, ssh_config)
            output, exit_code = ssh_service.execute(command, timeout=timeout)
            ssh_service.disconnect()
            
            # Handle results
            if output is None or exit_code is None:
                # Timeout or connection failure
                if check:
                    raise subprocess.CalledProcessError(1, command, "SSH command failed or timed out")
                return None
            
            if exit_code != 0 and check:
                raise subprocess.CalledProcessError(exit_code, command, output)
            
            return output
        except subprocess.CalledProcessError:
            # Re-raise if check=True
            if check:
                raise
            return None
        except Exception as exc:
            # Fallback to subprocess if SSHService fails
            logger.warning("SSHService failed, falling back to subprocess: %s", exc)
            if check and isinstance(exc, (paramiko.SSHException, paramiko.AuthenticationException)):
                raise
            # Continue to subprocess fallback below
    
    # Fallback to subprocess if paramiko not available, cfg not provided, or SSHService failed
    if not cfg:
        # If no cfg provided, use defaults
        connect_timeout = 10
        batch_mode = "no"
    else:
        connect_timeout = cfg.ssh.connect_timeout if hasattr(cfg, "ssh") else 10
        batch_mode = "yes" if (hasattr(cfg, "ssh") and cfg.ssh.batch_mode) else "no"
    tty_flag = "-tt" if force_tty else ""
    cmd = (f"ssh -o ConnectTimeout={connect_timeout} -o BatchMode={batch_mode} " f'{tty_flag} {host} "{command}"'
    ).strip()
    try:
        # Use Popen to stream output AND capture it
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        output_lines = []
        error_lines = []
        # Read and stream both stdout and stderr
        def read_stream(stream, is_stderr):
            lines = []
            try:
                for line in stream:
                    if is_stderr:
                        sys.stderr.write(line)
                        sys.stderr.flush()
                        error_lines.append(line)
                    else:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                        output_lines.append(line)
            except (IOError, OSError):
                pass
        # Use threads to read both streams
        stdout_thread = threading.Thread(target=read_stream, args=(process.stdout, False))
        stderr_thread = threading.Thread(target=read_stream, args=(process.stderr, True))
        stdout_thread.start()
        stderr_thread.start()
        # Wait for process to complete
        process.wait(timeout=timeout)
        stdout_thread.join()
        stderr_thread.join()
        output = "".join(output_lines).strip()
        error_output = "".join(error_lines).strip()
        combined = output
        if error_output:
            combined = f"{output}\n{error_output}" if output else error_output
        if process.returncode != 0 and check:
            raise subprocess.CalledProcessError(process.returncode, command, combined)
        return combined
    except subprocess.TimeoutExpired:
        logger.error("SSH command timed out - COMMAND FAILED")
        process.kill()
        return None
    except subprocess.CalledProcessError:
        if check:
            raise
        return None

def container_exists(proxmox_host, container_id, cfg=None):
    """Check if container exists"""
    container_id_str = str(container_id)
    result = ssh_exec(
        proxmox_host,
        f"pct list | grep '^{container_id_str} '",
        check=False,
        cfg=cfg,
    )
    return result is not None and container_id_str in result

def destroy_container(proxmox_host, container_id, cfg=None, lxc_service=None):
    """
    Destroy container if it exists
    Args:
        proxmox_host: Proxmox host
        container_id: Container ID to destroy
        cfg: Lab configuration (optional, needed if lxc_service not provided)
        lxc_service: LXCService instance to reuse connection (optional)
    """
    container_id_str = str(container_id)
    # Use provided lxc_service or create temporary one
    use_temporary_service = lxc_service is None
    if use_temporary_service:
        if not cfg:
            logger.error("Either cfg or lxc_service must be provided")
            return
        from services.lxc import LXCService
        lxc_service = LXCService(proxmox_host, cfg.ssh)
        if not lxc_service.connect():
            logger.error("Failed to connect to Proxmox host")
            return
    try:
        # Check if container exists
        check_cmd = f"pct list | grep '^{container_id_str} ' || echo 'not_found'"
        check_output, _ = lxc_service.execute(check_cmd)
        if not check_output or container_id_str not in check_output or "not_found" in check_output:
            logger.info("Container %s does not exist, skipping", container_id)
            return
        # Stop and destroy in one batch (stop may fail if already stopped, that's ok)
        logger.info("Stopping and destroying container %s...", container_id)
        destroy_cmd = f"pct stop {container_id} 2>/dev/null || true; sleep 2; pct destroy {container_id} 2>&1"
        destroy_output, destroy_exit = lxc_service.execute(destroy_cmd)
        if destroy_exit is not None and destroy_exit != 0:
            logger.warning("Destroy failed, trying force destroy...")
            force_cmd = f"pct destroy {container_id_str} --force 2>&1 || true"
            lxc_service.execute(force_cmd)
            time.sleep(1)
        # Verify destruction
        verify_cmd = f"pct list | grep '^{container_id_str} ' || echo 'not_found'"
        verify_output, _ = lxc_service.execute(verify_cmd)
        if not verify_output or container_id_str not in verify_output or "not_found" in verify_output:
            logger.info("Container %s destroyed", container_id_str)
        else:
            logger.error("Container %s still exists after destruction attempt", container_id_str)
    finally:
        if use_temporary_service:
            lxc_service.disconnect()

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
        max_attempts = cfg.waits.container_ready_max_attempts if cfg and hasattr(cfg, "waits") else 30
    if sleep_interval is None:
        sleep_interval = cfg.waits.container_ready_sleep if cfg and hasattr(cfg, "waits") else 3
    for i in range(1, max_attempts + 1):
        status = ssh_exec(proxmox_host, f"pct status {container_id} 2>&1", check=False, cfg=cfg)
        if "running" in status:
            # Try ping
            try:
                ping_result = subprocess.run(
                    f"ping -c 1 -W 2 {ip_address}",
                    shell=True,
                    timeout=5,
                    check=False,
                )
                if ping_result.returncode == 0:
                    logger.info("Container is up!")
                    return True
            except (subprocess.TimeoutExpired, OSError):
                pass
            # Try SSH directly (fallback) using SSHService
            if cfg and HAS_PARAMIKO:
                try:
                    from services.ssh import SSHService
                    from libs.config import SSHConfig
                    
                    # Create SSH config with short timeout for quick test
                    connect_timeout = cfg.ssh.connect_timeout if hasattr(cfg, "ssh") else 3
                    ssh_config = SSHConfig(
                        connect_timeout=connect_timeout,
                        batch_mode=cfg.ssh.batch_mode if hasattr(cfg, "ssh") else True,
                        default_exec_timeout=5,
                        read_buffer_size=cfg.ssh.read_buffer_size if hasattr(cfg, "ssh") else 4096,
                        poll_interval=cfg.ssh.poll_interval if hasattr(cfg, "ssh") else 0.05,
                        default_username="root",
                        look_for_keys=cfg.ssh.look_for_keys if hasattr(cfg, "ssh") else True,
                        allow_agent=cfg.ssh.allow_agent if hasattr(cfg, "ssh") else True,
                        verbose=cfg.ssh.verbose if hasattr(cfg, "ssh") and hasattr(cfg.ssh, "verbose") else False,
                    )
                    
                    test_host = f"root@{ip_address}"
                    test_ssh = SSHService(test_host, ssh_config)
                    if test_ssh.connect():
                        output, exit_code = test_ssh.execute("echo test", timeout=5)
                        test_ssh.disconnect()
                        if exit_code == 0 and output and "test" in output:
                            logger.info("Container is up (SSH working)!")
                            return True
                except Exception:
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
    if not cfg:
        logger.error("Configuration required for SSH key setup")
        return False
    default_user = cfg.users.default_user if hasattr(cfg, "users") else "jaal"
    # Remove old host key
    subprocess.run(f"ssh-keygen -R {ip_address} 2>/dev/null", shell=True, check=False)
    # Use PCTService to execute commands in container
    try:
        from services.lxc import LXCService
        from services.pct import PCTService
        
        lxc_service = LXCService(proxmox_host, cfg.ssh)
        if not lxc_service.connect():
            logger.error("Failed to connect to Proxmox host for SSH key setup")
            return False
        
        pct_service = PCTService(lxc_service)
        
        # Base64 encode the key to avoid any shell escaping problems
        key_b64 = base64.b64encode(ssh_key.encode("utf-8")).decode("ascii")
        
        # Add to default user - use base64 decode to avoid quote issues
        user_cmd = (
            f"mkdir -p /home/{default_user}/.ssh && echo {key_b64} | base64 -d > "
            f"/home/{default_user}/.ssh/authorized_keys && "
            f"chmod 600 /home/{default_user}/.ssh/authorized_keys && "
            f"chown {default_user}:{default_user} /home/{default_user}/.ssh/authorized_keys"
        )
        pct_service.execute(container_id, user_cmd, check=False)
        
        # Add to root user - use base64 decode to avoid quote issues
        root_cmd = (
            f"mkdir -p /root/.ssh && echo {key_b64} | base64 -d > "
            f"/root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
        )
        pct_service.execute(container_id, root_cmd, check=False)
        
        # Verify the key file exists
        verify_cmd = (
            f"test -f /home/{default_user}/.ssh/authorized_keys && "
            f"test -f /root/.ssh/authorized_keys && echo 'keys_exist' || echo 'keys_missing'"
        )
        verify_output, _ = pct_service.execute(container_id, verify_cmd, check=False)
        
        lxc_service.disconnect()
        
        if verify_output and "keys_exist" in verify_output:
            logger.info("SSH key setup verified successfully")
            return True
        logger.error("SSH key verification failed: %s", verify_output)
        return False
    except Exception as exc:
        logger.error("Failed to setup SSH key: %s", exc)
        return False