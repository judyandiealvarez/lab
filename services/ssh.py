"""
SSH Service - manages SSH connections and command execution
"""
import logging
import sys
import time
from pathlib import Path
from typing import Optional
try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False
from libs.config import SSHConfig
logger = logging.getLogger(__name__)

class SSHService:
    """Service that manages SSH connections and command execution"""
    def __init__(self, host: str, ssh_config: SSHConfig):
        """
        Initialize SSH service
        Args:
            host: SSH host (format: user@host or just host)
            ssh_config: SSH configuration
        """
        self.host = host
        self.ssh_config = ssh_config
        self._client: Optional[paramiko.SSHClient] = None
        self._connected = False
        # Parse host (format: user@host or just host)
        if "@" in host:
            self.username, self.hostname = host.split("@", 1)
        else:
            self.username = ssh_config.default_username
            self.hostname = host

    def connect(self) -> bool:
        """
        Establish SSH connection
        Returns:
            True if connection successful, False otherwise
        """
        if self._connected and self._client:
            # Check if connection is still alive
            try:
                transport = self._client.get_transport()
                if transport and transport.is_active():
                    return True
            except Exception:
                # Connection is dead, need to reconnect
                self._client = None
                self._connected = False
        if not HAS_PARAMIKO:
            logger.error("paramiko not available, cannot establish SSH connection")
            return False
        try:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # Find and load private key that matches the public key we set up
            # We set up id_rsa.pub or id_ed25519.pub, so try to load the corresponding private key
            key_file = None
            key_paths = [
                Path.home() / ".ssh" / "id_rsa",
                Path.home() / ".ssh" / "id_ed25519",
            ]
            for key_path in key_paths:
                if key_path.exists():
                    try:
                        key_file = str(key_path)
                        logger.debug("Found private key: %s", key_file)
                        break
                    except Exception:
                        continue
            connect_kwargs = {
                "hostname": self.hostname,
                "username": self.username,
                "timeout": self.ssh_config.connect_timeout,
                "look_for_keys": self.ssh_config.look_for_keys,
                "allow_agent": self.ssh_config.allow_agent,
            }
            # Explicitly load private key if found
            if key_file:
                try:
                    # Try to load the key
                    pkey = None
                    for key_class in [paramiko.RSAKey, paramiko.Ed25519Key]:
                        try:
                            pkey = key_class.from_private_key_file(key_file)
                            logger.debug("Loaded private key from %s", key_file)
                            break
                        except Exception:
                            continue
                    if pkey:
                        connect_kwargs["pkey"] = pkey
                except Exception as key_exc:
                    logger.warning("Failed to load private key %s: %s", key_file, key_exc)
            self._client.connect(**connect_kwargs)
            self._connected = True
            logger.info("SSH connection established to %s@%s", self.username, self.hostname)
            return True
        except paramiko.AuthenticationException as exc:
            logger.error("SSH authentication failed to %s: %s", self.host, exc)
            logger.error("Tried key file: %s", key_file if "key_file" in locals() else "None")
            self._client = None
            self._connected = False
            return False
        except paramiko.SSHException as exc:
            logger.error("SSH connection error to %s: %s", self.host, exc)
            self._client = None
            self._connected = False
            return False
        except Exception as exc:
            logger.error("Failed to establish SSH connection to %s: %s", self.host, exc)
            logger.error("Exception type: %s", type(exc).__name__)
            self._client = None
            self._connected = False
            return False

    def disconnect(self):
        """Close SSH connection"""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            finally:
                self._client = None
                self._connected = False
            logger.debug("SSH connection closed to %s", self.host)

    def is_connected(self) -> bool:
        """Check if SSH connection is active"""
        if not self._connected or not self._client:
            return False
        try:
            transport = self._client.get_transport()
            return transport is not None and transport.is_active()
        except Exception:
            return False

    def execute(self, command: str, timeout: Optional[int] = None, sudo: bool = False):
        """
        Execute command via SSH connection (always shows output interactively and captures it)
        Args:
            command: Command to execute
            timeout: Command timeout in seconds
            sudo: Whether to run command with sudo
        Returns:
            Tuple of (output, exit_code). output is always captured
        """
        if not self.is_connected():
            if not self.connect():
                logger.error("Cannot execute command: SSH connection not available")
                return None, None
        if sudo:
            # For multi-line scripts, use base64 encoding to avoid quoting issues
            if "\n" in command:
                import base64
                encoded = base64.b64encode(command.encode("utf-8")).decode("utf-8")
                command = f"sudo -n bash -c 'echo {encoded} | base64 -d | bash'"
            else:
                # For single-line commands, wrap in bash -c with proper quoting
                import shlex
                escaped_command = shlex.quote(command)
                command = f"sudo -n bash -c {escaped_command}"
        logger.debug("Running: %s", command)
        exec_timeout = timeout if timeout else self.ssh_config.default_exec_timeout
        try:
            _, stdout, stderr = self._client.exec_command(command, timeout=exec_timeout, get_pty=True)
            channel = stdout.channel
            # Read output
            output_lines = []
            error_lines = []
            last_output_time = time.time()
            # Set channel to non-blocking
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
                        data = stdout.read(self.ssh_config.read_buffer_size).decode("utf-8", errors="replace")
                        if data:
                            received_output = True
                            last_output_time = current_time
                            output_lines.append(data)
                            if self.ssh_config.verbose:
                                sys.stdout.write(data)
                                sys.stdout.flush()
                                logger.info(data.rstrip())
                    except (IOError, OSError, UnicodeDecodeError):
                        pass
                # Read available data from stderr
                if channel.recv_stderr_ready():
                    try:
                        data = stderr.read(self.ssh_config.read_buffer_size).decode("utf-8", errors="replace")
                        if data:
                            received_output = True
                            last_output_time = current_time
                            error_lines.append(data)
                            if self.ssh_config.verbose:
                                sys.stderr.write(data)
                                sys.stderr.flush()
                                logger.error(data.rstrip())
                    except (IOError, OSError, UnicodeDecodeError):
                        pass
                # Check for timeout
                if not received_output:
                    time_since_last_output = current_time - last_output_time
                    if time_since_last_output > exec_timeout:
                        logger.error("SSH command timeout after %ss of no output - COMMAND FAILED", exec_timeout)
                        channel.close()
                        return None, None
                time.sleep(self.ssh_config.poll_interval)
            # Get remaining output
            try:
                remaining = stdout.read().decode("utf-8", errors="replace")
                if remaining:
                    output_lines.append(remaining)
                    if self.ssh_config.verbose:
                        sys.stdout.write(remaining)
                        sys.stdout.flush()
                        logger.info(remaining.rstrip())
                remaining_err = stderr.read().decode("utf-8", errors="replace")
                if remaining_err:
                    error_lines.append(remaining_err)
                    if self.ssh_config.verbose:
                        sys.stderr.write(remaining_err)
                        sys.stderr.flush()
                        logger.error(remaining_err.rstrip())
            except (IOError, OSError, UnicodeDecodeError):
                pass
            # Get exit status
            exit_code = channel.recv_exit_status()
            output = "".join(output_lines).strip()
            error_output = "".join(error_lines).strip()
            # Combine stdout and stderr
            combined = output
            if error_output:
                combined = f"{output}\n{error_output}" if output else error_output
            return combined, exit_code
        except paramiko.SSHException as exc:
            logger.error("SSH command execution failed: %s", exc)
            return None, None
        except Exception as exc:
            logger.error("Unexpected error during SSH command execution: %s", exc)
            return None, None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()