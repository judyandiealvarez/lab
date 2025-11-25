"""
Unit tests for SSHService using pytest
"""
import services.ssh
from libs.config import SSHConfig
from services.ssh import SSHService
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock
import pytest
# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
# Mock paramiko before importing the service
class MockSSHClient:
    def __init__(self, *args, **kwargs):
        pass

    def set_missing_host_key_policy(self, policy):
        pass

    def connect(self, *args, **kwargs):
        pass

    def exec_command(self, *args, **kwargs):
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_channel = MagicMock()
        mock_channel.exit_status_ready.return_value = True
        mock_channel.recv_ready.return_value = False
        mock_channel.recv_stderr_ready.return_value = False
        mock_channel.recv_exit_status.return_value = 0
        mock_channel.setblocking = MagicMock()
        mock_stdout.channel = mock_channel
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b""
        return mock_stdin, mock_stdout, mock_stderr

    def get_transport(self):
        mock_transport = MagicMock()
        mock_transport.is_active.return_value = True
        return mock_transport

    def close(self):
        pass
mock_paramiko = MagicMock()
mock_paramiko.SSHClient = MockSSHClient
mock_paramiko.AutoAddPolicy = MagicMock()
mock_paramiko.SSHException = Exception
# Patch paramiko in sys.modules before importing
sys.modules["paramiko"] = mock_paramiko
# Force HAS_PARAMIKO to be True for testing
services.ssh.HAS_PARAMIKO = True
services.ssh.paramiko = mock_paramiko
@pytest.fixture

def ssh_config():
    """Fixture for SSH config"""
    return SSHConfig(connect_timeout=10, batch_mode=False)
@pytest.fixture

def ssh_host():
    """Fixture for SSH host"""
    return "root@10.11.3.4"
@pytest.fixture

def ssh_service(ssh_config, ssh_host):
    """Fixture for SSHService instance"""
    return SSHService(ssh_host, ssh_config)

def test_init_with_user_at_host(ssh_config):
    """Test initialization with user@host format"""
    service = SSHService("user@host.example.com", ssh_config)
    assert service.host == "user@host.example.com"
    assert service.username == "user"
    assert service.hostname == "host.example.com"
    assert service._connected is False
    assert service._client is None

def test_init_without_user(ssh_config):
    """Test initialization without user (defaults to root)"""
    service = SSHService("host.example.com", ssh_config)
    assert service.host == "host.example.com"
    assert service.username == ssh_config.default_username
    assert service.hostname == "host.example.com"

def test_connect_success(ssh_service, mocker):
    """Test successful connection"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mocker.patch("services.ssh.paramiko.SSHClient", return_value=mock_client)
    result = ssh_service.connect()
    assert result is True
    assert ssh_service._connected is True
    assert ssh_service._client == mock_client
    mock_client.set_missing_host_key_policy.assert_called_once()
    mock_client.connect.assert_called_once_with(
        hostname="10.11.3.4",
        username="root",
        timeout=10,
        look_for_keys=True,
        allow_agent=True,
    )

def test_connect_already_connected(ssh_service):
    """Test connect when already connected"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    ssh_service._client = mock_client
    ssh_service._connected = True
    result = ssh_service.connect()
    assert result is True
    # Should not call connect again
    mock_client.connect.assert_not_called()

def test_connect_dead_connection_reconnects(ssh_service, mocker):
    """Test connect reconnects when connection is dead"""
    # First, set up a dead connection
    mock_old_client = MagicMock()
    mock_old_transport = MagicMock()
    mock_old_transport.is_active.return_value = False
    mock_old_client.get_transport.return_value = mock_old_transport
    ssh_service._client = mock_old_client
    ssh_service._connected = True
    # Now set up a new connection
    mock_new_client = MagicMock()
    mock_new_transport = MagicMock()
    mock_new_transport.is_active.return_value = True
    mock_new_client.get_transport.return_value = mock_new_transport
    mocker.patch("services.ssh.paramiko.SSHClient", return_value=mock_new_client)
    result = ssh_service.connect()
    assert result is True
    assert ssh_service._client == mock_new_client
    mock_new_client.connect.assert_called_once()

def test_connect_exception_handling(ssh_service, mocker):
    """Test connect handles exceptions"""
    mock_client = MagicMock()
    mock_client.connect.side_effect = Exception("Connection failed")
    mocker.patch("services.ssh.paramiko.SSHClient", return_value=mock_client)
    result = ssh_service.connect()
    assert result is False
    assert ssh_service._connected is False
    assert ssh_service._client is None

def test_connect_no_paramiko(ssh_service, mocker):
    """Test connect when paramiko is not available"""
    mocker.patch("services.ssh.HAS_PARAMIKO", False)
    result = ssh_service.connect()
    assert result is False

def test_disconnect_with_client(ssh_service):
    """Test disconnect when client exists"""
    mock_client = MagicMock()
    ssh_service._client = mock_client
    ssh_service._connected = True
    ssh_service.disconnect()
    mock_client.close.assert_called_once()
    assert ssh_service._client is None
    assert ssh_service._connected is False

def test_disconnect_without_client(ssh_service):
    """Test disconnect when no client exists"""
    ssh_service._client = None
    ssh_service._connected = False
    # Should not raise exception
    ssh_service.disconnect()
    assert ssh_service._client is None
    assert ssh_service._connected is False

def test_disconnect_exception_handling(ssh_service):
    """Test disconnect handles exceptions"""
    mock_client = MagicMock()
    mock_client.close.side_effect = Exception("Close failed")
    ssh_service._client = mock_client
    ssh_service._connected = True
    # Should not raise exception
    ssh_service.disconnect()
    assert ssh_service._client is None
    assert ssh_service._connected is False

def test_is_connected_true(ssh_service):
    """Test is_connected returns True for active connection"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    ssh_service._client = mock_client
    ssh_service._connected = True
    result = ssh_service.is_connected()
    assert result is True

def test_is_connected_false_no_client(ssh_service):
    """Test is_connected returns False when no client"""
    ssh_service._client = None
    ssh_service._connected = False
    assert ssh_service.is_connected() is False

def test_is_connected_false_not_connected(ssh_service):
    """Test is_connected returns False when not connected"""
    ssh_service._client = MagicMock()
    ssh_service._connected = False
    assert ssh_service.is_connected() is False

def test_is_connected_false_inactive_transport(ssh_service):
    """Test is_connected returns False for inactive transport"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = False
    mock_client.get_transport.return_value = mock_transport
    ssh_service._client = mock_client
    ssh_service._connected = True
    assert ssh_service.is_connected() is False

def test_is_connected_false_no_transport(ssh_service):
    """Test is_connected returns False when no transport"""
    mock_client = MagicMock()
    mock_client.get_transport.return_value = None
    ssh_service._client = mock_client
    ssh_service._connected = True
    assert ssh_service.is_connected() is False

def test_is_connected_exception_handling(ssh_service):
    """Test is_connected handles exceptions"""
    mock_client = MagicMock()
    mock_client.get_transport.side_effect = Exception("Transport error")
    ssh_service._client = mock_client
    ssh_service._connected = True
    assert ssh_service.is_connected() is False

def test_execute_not_connected_connects(ssh_service, mocker):
    """Test execute connects if not connected"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    # Mock exec_command
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = True
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_channel.setblocking = MagicMock()
    mock_stdout.channel = mock_channel
    mock_stdout.read.return_value = b""
    mock_stderr.read.return_value = b""
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    mocker.patch("services.ssh.paramiko.SSHClient", return_value=mock_client)
    output, exit_code = ssh_service.execute("test command")
    assert exit_code == 0
    mock_client.connect.assert_called()

def test_execute_not_connected_connect_fails(ssh_service, mocker):
    """Test execute returns None when connection fails"""
    mock_client = MagicMock()
    mock_client.connect.side_effect = Exception("Connection failed")
    mocker.patch("services.ssh.paramiko.SSHClient", return_value=mock_client)
    output, exit_code = ssh_service.execute("test command")
    assert output is None
    assert exit_code is None

def test_execute_success_capture_output(ssh_service):
    """Test execute with capture_output=True"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = True
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_stdout.read.return_value = b"test output"
    mock_stderr.read.return_value = b""
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command")
    assert output == "test output"
    assert exit_code == 0

def test_execute_success_no_capture_output(ssh_service, mocker):
    """Test execute with capture_output=False"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = True
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    mocker.patch("sys.stdout.write")
    output, exit_code = ssh_service.execute("test command")
    assert output is None
    assert exit_code == 0

def test_execute_with_stdout_data(ssh_service):
    """Test execute with stdout data"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.side_effect = [False, True]
    mock_channel.recv_ready.side_effect = [True, False]
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_stdout.read.side_effect = [b"test data", b""]
    mock_stderr.read.return_value = b""
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command")
    assert "test data" in output
    assert exit_code == 0

def test_execute_with_stderr_data(ssh_service):
    """Test execute with stderr data"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.side_effect = [False, True]
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.side_effect = [True, False]
    mock_channel.recv_exit_status.return_value = 1
    mock_stdout.channel = mock_channel
    mock_stdout.read.return_value = b""
    mock_stderr.read.side_effect = [b"error message", b""]
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command")
    assert "error message" in output
    assert exit_code == 1

def test_execute_timeout(ssh_service, mocker):
    """Test execute handles timeout"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = False
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_stdout.channel = mock_channel
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    # Mock time to simulate timeout
    mocker.patch("time.time", side_effect=[0, 0, 301])  # 301 seconds elapsed
    mocker.patch("time.sleep")
    output, exit_code = ssh_service.execute("test command", timeout=300)
    assert output is None
    assert exit_code is None
    mock_channel.close.assert_called()

def test_execute_ssh_exception(ssh_service):
    """Test execute handles SSHException"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_client.exec_command.side_effect = Exception("SSH error")
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command")
    assert output is None
    assert exit_code is None

def test_execute_read_exception(ssh_service):
    """Test execute handles read exceptions"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.side_effect = [False, True]
    mock_channel.recv_ready.return_value = True
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_stdout.read.side_effect = IOError("Read error")
    mock_stderr.read.return_value = b""
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    # Should handle exception gracefully
    output, exit_code = ssh_service.execute("test command")
    assert exit_code == 0

def test_execute_with_custom_timeout(ssh_service):
    """Test execute with custom timeout"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = True
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_stdout.read.return_value = b""
    mock_stderr.read.return_value = b""
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command", timeout=60)
    assert exit_code == 0
    # Verify timeout was passed to exec_command
    mock_client.exec_command.assert_called_once()
    call_kwargs = mock_client.exec_command.call_args[1]
    assert call_kwargs["timeout"] == 60

def test_execute_default_timeout(ssh_service):
    """Test execute uses default timeout when not specified"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = True
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_stdout.read.return_value = b""
    mock_stderr.read.return_value = b""
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command")
    assert exit_code == 0
    # Verify default timeout (300) was used
    mock_client.exec_command.assert_called_once()
    call_kwargs = mock_client.exec_command.call_args[1]
    assert call_kwargs["timeout"] == 300

def test_context_manager(ssh_service, mocker):
    """Test context manager usage"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mocker.patch("services.ssh.paramiko.SSHClient", return_value=mock_client)
    with ssh_service:
        assert ssh_service._connected is True
    # After context exit, should be disconnected
    mock_client.close.assert_called_once()

def test_execute_combines_stdout_stderr(ssh_service):
    """Test execute combines stdout and stderr when capture_output=True"""
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_channel = MagicMock()
    mock_channel.exit_status_ready.return_value = True
    mock_channel.recv_ready.return_value = False
    mock_channel.recv_stderr_ready.return_value = False
    mock_channel.recv_exit_status.return_value = 0
    mock_stdout.channel = mock_channel
    mock_stdout.read.return_value = b"stdout message"
    mock_stderr.read.return_value = b"stderr message"
    mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
    ssh_service._client = mock_client
    ssh_service._connected = True
    output, exit_code = ssh_service.execute("test command")
    assert "stdout message" in output
    assert "stderr message" in output
    assert exit_code == 0