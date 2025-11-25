"""
Unit tests for PCTService using pytest
"""
from libs.config import SSHConfig
from services.lxc import LXCService
from services.pct import PCTService
import base64
import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest
# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
@pytest.fixture

def lxc_service():
    """Fixture for mocked LXC service"""
    return MagicMock(spec=LXCService)
@pytest.fixture

def pct_service(lxc_service):
    """Fixture for PCTService instance"""
    return PCTService(lxc_service)

def test_init(pct_service, lxc_service):
    """Test initialization"""
    assert pct_service.lxc == lxc_service

def test_encode_command(pct_service):
    """Test _encode_command"""
    command = "test command with special chars: $HOME && echo 'test'"
    encoded = pct_service._encode_command(command)
    decoded = base64.b64decode(encoded).decode("utf-8")
    assert decoded == command

def test_encode_command_empty(pct_service):
    """Test _encode_command with empty string"""
    encoded = pct_service._encode_command("")
    decoded = base64.b64decode(encoded).decode("utf-8")
    assert decoded == ""

def test_build_pct_exec_command(pct_service):
    """Test _build_pct_exec_command"""
    container_id = "3005"
    command = "echo hello"
    result = pct_service._build_pct_exec_command(container_id, command)
    assert "pct exec" in result
    assert container_id in result
    assert "base64 -d" in result
    # Verify command is encoded
    encoded = base64.b64encode(command.encode("utf-8")).decode("ascii")
    assert encoded in result

def test_execute(pct_service, lxc_service):
    """Test execute method"""
    container_id = "3005"
    command = "echo test"
    lxc_service.execute.return_value = ("output", 0)
    output, exit_code = pct_service.execute(container_id, command)
    assert output == "output"
    assert exit_code == 0
    lxc_service.execute.assert_called_once()
    call_args = lxc_service.execute.call_args
    assert "pct exec" in call_args[0][0]
    assert container_id in call_args[0][0]

def test_execute_with_timeout(pct_service, lxc_service):
    """Test execute with timeout"""
    container_id = "3005"
    command = "echo test"
    lxc_service.execute.return_value = ("output", 0)
    output, exit_code = pct_service.execute(container_id, command, timeout=60)
    assert output == "output"
    assert exit_code == 0
    call_kwargs = lxc_service.execute.call_args[1]
    assert call_kwargs["timeout"] == 60

def test_execute_capture_output_false(pct_service, lxc_service):
    """Test execute with capture_output=False"""
    container_id = "3005"
    command = "echo test"
    lxc_service.execute.return_value = (None, 0)
    output, exit_code = pct_service.execute(container_id, command)
    assert output is None
    assert exit_code == 0
    call_kwargs = lxc_service.execute.call_args[1]
    assert call_kwargs["capture_output"] is False

def test_create(pct_service, lxc_service):
    """Test create method"""
    lxc_service.execute.return_value = ("Container created", 0)
    output, exit_code = pct_service.create(
        container_id="3005",
        template_path="/path/to/template",
        hostname="test",
        memory=2048,
        swap=2048,
        cores=4,
        ip_address="10.11.3.5",
        gateway="10.11.3.253",
        bridge="vmbr0",
        storage="sdb",
        rootfs_size=20,
    )
    assert output == "Container created"
    assert exit_code == 0
    lxc_service.execute.assert_called_once()
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct create" in call_args
    assert "3005" in call_args
    assert "2>&1" not in call_args  # Should be removed

def test_create_unprivileged_false(pct_service, lxc_service):
    """Test create with unprivileged=False"""
    lxc_service.execute.return_value = ("Container created", 0)
    pct_service.create(
        container_id="3005",
        template_path="/path/to/template",
        hostname="test",
        memory=2048,
        swap=2048,
        cores=4,
        ip_address="10.11.3.5",
        gateway="10.11.3.253",
        bridge="vmbr0",
        storage="sdb",
        rootfs_size=20,
        unprivileged=False,
    )
    call_args = lxc_service.execute.call_args[0][0]
    assert "--unprivileged 0" in call_args

def test_start(pct_service, lxc_service):
    """Test start method"""
    lxc_service.execute.return_value = ("Container started", 0)
    output, exit_code = pct_service.start("3005")
    assert output == "Container started"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct start" in call_args
    assert "3005" in call_args
    assert "2>&1" not in call_args

def test_start_capture_output_false(pct_service, lxc_service):
    """Test start with capture_output=False"""
    lxc_service.execute.return_value = (None, 0)
    output, exit_code = pct_service.start("3005")
    assert output is None
    assert exit_code == 0
    call_kwargs = lxc_service.execute.call_args[1]
    assert call_kwargs["capture_output"] is False

def test_stop(pct_service, lxc_service):
    """Test stop method"""
    lxc_service.execute.return_value = ("Container stopped", 0)
    output, exit_code = pct_service.stop("3005")
    assert output == "Container stopped"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct stop" in call_args
    assert "3005" in call_args
    assert "--force" not in call_args

def test_stop_force(pct_service, lxc_service):
    """Test stop with force=True"""
    lxc_service.execute.return_value = ("Container stopped", 0)
    pct_service.stop("3005", force=True)
    call_args = lxc_service.execute.call_args[0][0]
    assert "--force" in call_args

def test_status_with_container_id(pct_service, lxc_service):
    """Test status with container_id"""
    lxc_service.execute.return_value = ("running", 0)
    output, exit_code = pct_service.status("3005")
    assert output == "running"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct status" in call_args
    assert "3005" in call_args

def test_status_without_container_id(pct_service, lxc_service):
    """Test status without container_id (list all)"""
    lxc_service.execute.return_value = ("Container list", 0)
    output, exit_code = pct_service.status(None)
    assert output == "Container list"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct list" in call_args

def test_destroy(pct_service, lxc_service):
    """Test destroy method"""
    lxc_service.execute.return_value = ("Container destroyed", 0)
    output, exit_code = pct_service.destroy("3005")
    assert output == "Container destroyed"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct destroy" in call_args
    assert "3005" in call_args
    assert "--force" not in call_args

def test_destroy_force(pct_service, lxc_service):
    """Test destroy with force=True"""
    lxc_service.execute.return_value = ("Container destroyed", 0)
    pct_service.destroy("3005", force=True)
    call_args = lxc_service.execute.call_args[0][0]
    assert "--force" in call_args

def test_set_features_all_enabled(pct_service, lxc_service):
    """Test set_features with all features enabled"""
    lxc_service.execute.return_value = ("Features set", 0)
    output, exit_code = pct_service.set_features("3005")
    assert output == "Features set"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct set" in call_args
    assert "--features" in call_args
    assert "nesting=1" in call_args
    assert "keyctl=1" in call_args
    assert "fuse=1" in call_args

def test_set_features_partial(pct_service, lxc_service):
    """Test set_features with partial features"""
    lxc_service.execute.return_value = ("Features set", 0)
    pct_service.set_features("3005", nesting=True, keyctl=False, fuse=True)
    call_args = lxc_service.execute.call_args[0][0]
    assert "nesting=1" in call_args
    assert "keyctl=1" not in call_args
    assert "fuse=1" in call_args

def test_set_features_none(pct_service, lxc_service):
    """Test set_features with no features enabled"""
    lxc_service.execute.return_value = ("Features set", 0)
    pct_service.set_features("3005", nesting=False, keyctl=False, fuse=False)
    call_args = lxc_service.execute.call_args[0][0]
    assert "--features" in call_args
    # Should have empty features string
    assert "nesting=1" not in call_args
    assert "keyctl=1" not in call_args
    assert "fuse=1" not in call_args

def test_config(pct_service, lxc_service):
    """Test config method"""
    lxc_service.execute.return_value = ("Config output", 0)
    output, exit_code = pct_service.config("3005")
    assert output == "Config output"
    assert exit_code == 0
    call_args = lxc_service.execute.call_args[0][0]
    assert "pct config" in call_args
    assert "3005" in call_args

def test_all_methods_remove_2to1(pct_service, lxc_service):
    """Test that all methods remove '2>&1' from commands"""
    lxc_service.execute.return_value = ("output", 0)
    methods_to_test = [
        (
            "create",
            {
                "container_id": "3005",
                "template_path": "/path",
                "hostname": "test",
                "memory": 2048,
                "swap": 2048,
                "cores": 4,
                "ip_address": "10.11.3.5",
                "gateway": "10.11.3.253",
                "bridge": "vmbr0",
                "storage": "sdb",
                "rootfs_size": 20,
            },
        ),
        ("start", {"container_id": "3005"}),
        ("stop", {"container_id": "3005"}),
        ("status", {"container_id": "3005"}),
        ("destroy", {"container_id": "3005"}),
        ("set_features", {"container_id": "3005"}),
        ("config", {"container_id": "3005"}),
    ]
    for method_name, kwargs in methods_to_test:
        method = getattr(pct_service, method_name)
        method(**kwargs)
        call_args = lxc_service.execute.call_args[0][0]
        assert "2>&1" not in call_args, f"Method {method_name} should remove '2>&1' from command"

def test_execute_error_handling(pct_service, lxc_service):
    """Test execute handles errors from LXC service"""
    lxc_service.execute.return_value = (None, None)
    output, exit_code = pct_service.execute("3005", "test command")
    assert output is None
    assert exit_code is None

def test_create_with_custom_params(pct_service, lxc_service):
    """Test create with custom parameters"""
    lxc_service.execute.return_value = ("Created", 0)
    pct_service.create(
        container_id="3005",
        template_path="/path/to/template",
        hostname="custom-host",
        memory=4096,
        swap=4096,
        cores=8,
        ip_address="10.11.3.10",
        gateway="10.11.3.253",
        bridge="vmbr1",
        storage="sdc",
        rootfs_size=40,
        unprivileged=False,
        ostype="debian",
        arch="arm64",
    )
    call_args = lxc_service.execute.call_args[0][0]
    assert "custom-host" in call_args
    assert "--memory 4096" in call_args
    assert "--cores 8" in call_args
    assert "--ostype debian" in call_args
    assert "--arch arm64" in call_args