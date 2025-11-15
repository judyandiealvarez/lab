#!/usr/bin/env python3
"""
Comprehensive test suite for lab configuration and modules
"""
import sys
import traceback
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test all module imports"""
    print("=" * 60)
    print("TEST 1: Module Imports")
    print("=" * 60)
    
    tests = [
        ("libs.config", "LabConfig"),
        ("libs.logger", "init_logger"),
        ("libs.common", "ssh_exec"),
        ("libs.container", "setup_container_base"),
        ("libs.template", "get_base_template"),
        ("ct", "load_container_handler"),
        ("tmpl", "load_template_handler"),
        ("lab", "get_config"),
    ]
    
    all_passed = True
    for module_name, item_name in tests:
        try:
            module = __import__(module_name, fromlist=[item_name])
            item = getattr(module, item_name)
            print(f"  ✓ {module_name}.{item_name}")
        except Exception as e:
            print(f"  ✗ {module_name}.{item_name}: {e}")
            all_passed = False
    
    return all_passed


def test_config_loading():
    """Test configuration loading"""
    print("\n" + "=" * 60)
    print("TEST 2: Configuration Loading")
    print("=" * 60)
    
    try:
        from lab import load_config, get_config
        
        # Test raw config loading
        config_dict = load_config()
        assert isinstance(config_dict, dict), "Config should be a dictionary"
        assert 'network' in config_dict, "Config should have 'network'"
        assert 'proxmox' in config_dict, "Config should have 'proxmox'"
        assert 'ct' in config_dict, "Config should have 'ct'"
        assert 'templates' in config_dict, "Config should have 'templates'"
        print("  ✓ Raw config loading")
        
        # Test LabConfig creation
        cfg = get_config()
        assert cfg.network == config_dict['network'], "Network should match"
        assert cfg.proxmox_host == config_dict['proxmox']['host'], "Proxmox host should match"
        assert len(cfg.containers) == len(config_dict['ct']), "Container count should match"
        assert len(cfg.templates) == len(config_dict['templates']), "Template count should match"
        print("  ✓ LabConfig creation")
        
        # Test computed fields
        assert cfg.network_base is not None, "network_base should be computed"
        assert cfg.gateway is not None, "gateway should be computed"
        assert cfg.network_base == "10.11.3", "network_base should be correct"
        assert cfg.gateway == "10.11.3.253", "gateway should be correct"
        print("  ✓ Computed fields (network_base, gateway)")
        
        # Test IP address computation
        for container in cfg.containers:
            assert container.ip_address is not None, f"Container {container.name} should have IP"
            assert container.ip_address.startswith(cfg.network_base), f"IP should match network base"
        for template in cfg.templates:
            assert template.ip_address is not None, f"Template {template.name} should have IP"
            assert template.ip_address.startswith(cfg.network_base), f"IP should match network base"
        print("  ✓ IP address computation")
        
        # Test swarm lists
        assert len(cfg.swarm_managers) > 0, "Should have swarm managers"
        assert len(cfg.swarm_workers) > 0, "Should have swarm workers"
        print("  ✓ Swarm manager/worker lists")
        
        return True
    except Exception as e:
        print(f"  ✗ Configuration loading failed: {e}")
        traceback.print_exc()
        return False


def test_config_properties():
    """Test config property access"""
    print("\n" + "=" * 60)
    print("TEST 3: Config Property Access")
    print("=" * 60)
    
    try:
        from lab import get_config
        cfg = get_config()
        
        # Test convenience properties
        props = [
            'proxmox_host', 'proxmox_storage', 'proxmox_bridge',
            'proxmox_template_dir', 'swarm_port', 'portainer_port',
            'portainer_image', 'apt_cache_port'
        ]
        
        for prop in props:
            value = getattr(cfg, prop)
            assert value is not None, f"{prop} should not be None"
            print(f"  ✓ {prop}: {value}")
        
        # Test nested config access
        assert cfg.users.default_user is not None, "users.default_user should exist"
        assert cfg.dns.servers is not None, "dns.servers should exist"
        assert len(cfg.dns.servers) > 0, "dns.servers should not be empty"
        assert cfg.waits.container_startup > 0, "waits.container_startup should be positive"
        print("  ✓ Nested config access")
        
        return True
    except Exception as e:
        print(f"  ✗ Property access failed: {e}")
        traceback.print_exc()
        return False


def test_container_configs():
    """Test container configuration objects"""
    print("\n" + "=" * 60)
    print("TEST 4: Container Configurations")
    print("=" * 60)
    
    try:
        from lab import get_config
        from libs.config import ContainerConfig, ContainerResources
        
        cfg = get_config()
        
        for container in cfg.containers:
            assert isinstance(container, ContainerConfig), f"{container.name} should be ContainerConfig"
            assert container.id > 0, f"{container.name} should have valid ID"
            assert container.name, f"{container.name} should have name"
            assert container.type, f"{container.name} should have type"
            assert container.ip_address, f"{container.name} should have IP address"
            assert container.hostname, f"{container.name} should have hostname"
            
            if container.resources:
                assert isinstance(container.resources, ContainerResources), "Resources should be ContainerResources"
                assert container.resources.memory > 0, "Memory should be positive"
                assert container.resources.cores > 0, "Cores should be positive"
            
            print(f"  ✓ {container.name}: {container.type} (ID: {container.id}, IP: {container.ip_address})")
        
        return True
    except Exception as e:
        print(f"  ✗ Container config test failed: {e}")
        traceback.print_exc()
        return False


def test_template_configs():
    """Test template configuration objects"""
    print("\n" + "=" * 60)
    print("TEST 5: Template Configurations")
    print("=" * 60)
    
    try:
        from lab import get_config
        from libs.config import TemplateConfig
        
        cfg = get_config()
        
        for template in cfg.templates:
            assert isinstance(template, TemplateConfig), f"{template.name} should be TemplateConfig"
            assert template.id > 0, f"{template.name} should have valid ID"
            assert template.name, f"{template.name} should have name"
            assert template.type, f"{template.name} should have type"
            assert template.ip_address, f"{template.name} should have IP address"
            assert template.hostname, f"{template.name} should have hostname"
            
            print(f"  ✓ {template.name}: {template.type} (ID: {template.id}, IP: {template.ip_address})")
        
        return True
    except Exception as e:
        print(f"  ✗ Template config test failed: {e}")
        traceback.print_exc()
        return False


def test_handler_loading():
    """Test dynamic handler loading"""
    print("\n" + "=" * 60)
    print("TEST 6: Dynamic Handler Loading")
    print("=" * 60)
    
    try:
        from lab import get_config
        from ct import load_container_handler
        from tmpl import load_template_handler
        
        cfg = get_config()
        
        # Test container handlers
        container_types = set(ct.type for ct in cfg.containers)
        for ctype in container_types:
            handler = load_container_handler(ctype)
            assert handler is not None, f"Handler for {ctype} should exist"
            assert callable(handler), f"Handler for {ctype} should be callable"
            print(f"  ✓ Container handler: {ctype}")
        
        # Test template handlers
        template_types = set(tmpl.type for tmpl in cfg.templates)
        for ttype in template_types:
            handler = load_template_handler(ttype)
            assert handler is not None, f"Handler for {ttype} should exist"
            assert callable(handler), f"Handler for {ttype} should be callable"
            print(f"  ✓ Template handler: {ttype}")
        
        # Test invalid handlers
        invalid_handler = load_container_handler("invalid-type")
        assert invalid_handler is None, "Invalid container type should return None"
        invalid_handler = load_template_handler("invalid-type")
        assert invalid_handler is None, "Invalid template type should return None"
        print("  ✓ Invalid handler handling")
        
        return True
    except Exception as e:
        print(f"  ✗ Handler loading test failed: {e}")
        traceback.print_exc()
        return False


def test_logging():
    """Test logging functionality"""
    print("\n" + "=" * 60)
    print("TEST 7: Logging System")
    print("=" * 60)
    
    try:
        from libs.logger import init_logger, get_logger
        
        # Initialize logger
        logger = init_logger(level=30)  # WARNING level
        assert logger is not None, "Logger should be initialized"
        print("  ✓ Logger initialization")
        
        # Get module logger
        test_logger = get_logger("test_module")
        assert test_logger is not None, "Module logger should be created"
        assert test_logger.name == "test_module", "Logger name should match"
        print("  ✓ Module logger creation")
        
        # Test logging (should not output at WARNING level)
        test_logger.debug("Debug message (should not appear)")
        test_logger.info("Info message (should not appear)")
        test_logger.warning("Warning message (should appear)")
        print("  ✓ Log level filtering")
        
        return True
    except Exception as e:
        print(f"  ✗ Logging test failed: {e}")
        traceback.print_exc()
        return False


def test_cli_commands():
    """Test CLI command structure"""
    print("\n" + "=" * 60)
    print("TEST 8: CLI Commands")
    print("=" * 60)
    
    try:
        from lab import main
        import argparse
        
        # Test that main function exists and is callable
        assert callable(main), "main should be callable"
        print("  ✓ Main function exists")
        
        # Test command functions exist
        from lab import cmd_deploy, cmd_cleanup, cmd_status
        assert callable(cmd_deploy), "cmd_deploy should be callable"
        assert callable(cmd_cleanup), "cmd_cleanup should be callable"
        assert callable(cmd_status), "cmd_status should be callable"
        print("  ✓ Command functions exist")
        
        return True
    except Exception as e:
        print(f"  ✗ CLI test failed: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("LAB CONFIGURATION TEST SUITE")
    print("=" * 60)
    
    tests = [
        ("Imports", test_imports),
        ("Config Loading", test_config_loading),
        ("Config Properties", test_config_properties),
        ("Container Configs", test_container_configs),
        ("Template Configs", test_template_configs),
        ("Handler Loading", test_handler_loading),
        ("Logging", test_logging),
        ("CLI Commands", test_cli_commands),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ {test_name} crashed: {e}")
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

