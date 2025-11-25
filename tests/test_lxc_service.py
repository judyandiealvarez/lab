"""
Unit tests for LXCService using pytest
"""
from libs.config import SSHConfig
from services.lxc import LXCService
import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest
# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
@pytest.fixture

def ssh_config():
    """Fixture for SSH config"""
    return SSHConfig(connect_timeout=10, batch_mode=False)
@pytest.fixture

def proxmox_host():
    """Fixture for proxmox host"""
    return "root@10.11.3.4"
@pytest.fixture

def lxc_service(ssh_config, proxmox_host):
    """Fixture for LXCService instance"""
    return LXCService(proxmox_host, ssh_config)

def test_init_with_user_at_host(ssh_config):
    """Test initialization with user@host format"""
    service = LXCService("user@host.example.com", ssh_config)
    assert service.proxmox_host == "user@host.example.com"
    assert service._ssh_service.host == "user@host.example.com"
    assert service._ssh_service.username == "user"
    assert service._ssh_service.hostname == "host.example.com"

def test_init_without_user(ssh_config):
    """Test initialization without user (defaults to root)"""
    service = LXCService("host.example.com", ssh_config)
    assert service.proxmox_host == "host.example.com"
    assert service._ssh_service.host == "host.example.com"
    assert service._ssh_service.username == "root"
    assert service._ssh_service.hostname == "host.example.com"

def test_connect_success(lxc_service):
    """Test successful connection"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.connect.return_value = True
    lxc_service._ssh_service = mock_ssh_service
    result = lxc_service.connect()
    assert result is True
    mock_ssh_service.connect.assert_called_once()

def test_connect_failure(lxc_service):
    """Test connection failure"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.connect.return_value = False
    lxc_service._ssh_service = mock_ssh_service
    result = lxc_service.connect()
    assert result is False
    mock_ssh_service.connect.assert_called_once()

def test_disconnect(lxc_service):
    """Test disconnect"""
    mock_ssh_service = MagicMock()
    lxc_service._ssh_service = mock_ssh_service
    lxc_service.disconnect()
    mock_ssh_service.disconnect.assert_called_once()

def test_is_connected_true(lxc_service):
    """Test is_connected returns True for active connection"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.is_connected.return_value = True
    lxc_service._ssh_service = mock_ssh_service
    result = lxc_service.is_connected()
    assert result is True
    mock_ssh_service.is_connected.assert_called_once()

def test_is_connected_false(lxc_service):
    """Test is_connected returns False when not connected"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.is_connected.return_value = False
    lxc_service._ssh_service = mock_ssh_service
    result = lxc_service.is_connected()
    assert result is False
    mock_ssh_service.is_connected.assert_called_once()

def test_execute_success(lxc_service):
    """Test execute with success"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.execute.return_value = ("output", 0)
    lxc_service._ssh_service = mock_ssh_service
    output, exit_code = lxc_service.execute("test command")
    assert output == "output"
    assert exit_code == 0
    mock_ssh_service.execute.assert_called_once_with("test command", timeout=None)

def test_execute_with_timeout(lxc_service):
    """Test execute with timeout"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.execute.return_value = ("output", 0)
    lxc_service._ssh_service = mock_ssh_service
    output, exit_code = lxc_service.execute("test command", timeout=60)
    assert output == "output"
    assert exit_code == 0
    mock_ssh_service.execute.assert_called_once_with("test command", timeout=60)

def test_execute_no_capture_output(lxc_service):
    """Test execute without capturing output"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.execute.return_value = (None, 0)
    lxc_service._ssh_service = mock_ssh_service
    output, exit_code = lxc_service.execute("test command")
    assert output is None
    assert exit_code == 0
    mock_ssh_service.execute.assert_called_once_with("test command", timeout=None)

def test_execute_failure(lxc_service):
    """Test execute with failure"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.execute.return_value = (None, None)
    lxc_service._ssh_service = mock_ssh_service
    output, exit_code = lxc_service.execute("test command")
    assert output is None
    assert exit_code is None
    mock_ssh_service.execute.assert_called_once()

def test_context_manager(lxc_service):
    """Test context manager usage"""
    mock_ssh_service = MagicMock()
    mock_ssh_service.connect.return_value = True
    lxc_service._ssh_service = mock_ssh_service
    with lxc_service:
        mock_ssh_service.connect.assert_called_once()
    # After context exit, should be disconnected
    mock_ssh_service.disconnect.assert_called_once()

def test_from_config(ssh_config):
    """Test from_config class method"""
    mock_config = MagicMock()
    mock_config.proxmox_host = "test@host"
    mock_config.ssh = ssh_config
    service = LXCService.from_config(mock_config)
    assert isinstance(service, LXCService)
    assert service.proxmox_host == "test@host"
    assert service.ssh_config == ssh_config