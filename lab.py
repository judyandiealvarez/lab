#!/usr/bin/env python3
"""
Lab CLI Tool - Manage Proxmox LXC containers and Docker Swarm
Implements all functionality directly in Python (not just calling bash scripts)
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import Optional
from commands.deploy import Deploy
from commands.cleanup import Cleanup
from commands.status import Status
from commands.backup import Backup
from commands.restore import Restore
from dependency_injector import containers, providers
from services.lxc import LXCService
from services.pct import PCTService
from libs import common, template
from libs.config import LabConfig
from libs.logger import get_logger, init_logger

SCRIPT_DIR = Path(__file__).parent.absolute()
DEFAULT_CONFIG_FILE = SCRIPT_DIR / "lab.yaml"
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
logger = get_logger(__name__)
VERBOSE_FLAG = False
CONFIG_FILE = DEFAULT_CONFIG_FILE
ENVIRONMENT = None

def load_config(config_file: Path = None) -> dict:
    """Load configuration from YAML file as dictionary"""
    if config_file is None:
        config_file = CONFIG_FILE
    if not config_file.exists():
        logger.error("Configuration file %s not found", config_file)
        sys.exit(1)
    if not HAS_YAML:
        logger.error("PyYAML is required. Install it with: pip install pyyaml")
        sys.exit(1)
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config
    except (OSError, yaml.YAMLError) as err:
        logger.error("Error loading configuration: %s", err)
        sys.exit(1)

def get_config(environment: Optional[str] = None) -> LabConfig:
    """Get configuration and return as LabConfig instance"""
    config_dict = load_config()
    config = LabConfig.from_dict(config_dict, verbose=VERBOSE_FLAG, environment=environment)
    config.compute_derived_fields()
    return config
# Re-export for backward compatibility
ssh_exec = common.ssh_exec
container_exists = common.container_exists
destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
get_ssh_key = common.get_ssh_key
setup_ssh_key = common.setup_ssh_key
get_base_template = template.get_base_template

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Lab CLI - Manage Proxmox LXC containers and Docker Swarm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show stdout from SSH service")
    parser.add_argument("--config", "-c", type=str, default=None, help="Path to YAML configuration file (default: lab.yaml)")
    parser.add_argument("--environment", "-e", type=str, default=None, choices=["prod", "test", "dev"], help="Environment to use (prod, test, dev). Defaults to 'test' if environments section exists")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    deploy_parser = subparsers.add_parser("deploy", help="Deploy complete lab: apt-cache, templates, and Docker Swarm")
    deploy_parser.add_argument("--start-step", type=int, default=1, help="Start from this step (default: 1)")
    deploy_parser.add_argument("--end-step", type=int, default=None, help="End at this step (default: last step)")
    deploy_parser.add_argument("--planonly", action="store_true", help="Show deployment plan and exit without executing")

    cleanup_parser = subparsers.add_parser("cleanup", help="Remove all containers and templates")

    redeploy_parser = subparsers.add_parser("redeploy", help="Cleanup and then deploy complete lab")
    redeploy_parser.add_argument("--start-step", type=int, default=1, help="Start from this step (default: 1)")
    redeploy_parser.add_argument("--end-step", type=int, default=None, help="End at this step (default: last step)")

    status_parser = subparsers.add_parser("status", help="Show current lab status")

    backup_parser = subparsers.add_parser("backup", help="Backup cluster according to lab.yaml configuration")
    
    restore_parser = subparsers.add_parser("restore", help="Restore cluster from backup")
    restore_parser.add_argument("--backup-name", required=True, help="Name of the backup to restore (e.g., backup-20251130_120000)")
    
    args = parser.parse_args()
    # Initialize logging (always log to file)
    # Use DEBUG level if verbose flag is set, otherwise INFO
    log_level = logging.DEBUG if args.verbose else logging.INFO
    init_logger(level=log_level)
    # Store verbose flag globally for config access
    global VERBOSE_FLAG, CONFIG_FILE, ENVIRONMENT
    VERBOSE_FLAG = args.verbose
    # Set config file path if specified
    if args.config:
        CONFIG_FILE = Path(args.config).resolve()
    else:
        CONFIG_FILE = DEFAULT_CONFIG_FILE
    # Set environment - default to 'test' if environments section exists and no environment specified
    if args.environment:
        ENVIRONMENT = args.environment
    else:
        # Check if environments section exists, default to 'test' if it does
        config_dict = load_config()
        if "environments" in config_dict:
            ENVIRONMENT = "test"
        else:
            ENVIRONMENT = None

    # DI container created in main and registering existing command classes
    di = containers.DynamicContainer()

    # Lazy-load config: only called when command class is instantiated (not when container is created)
    di.config = providers.Singleton(get_config, environment=ENVIRONMENT)

    # LXC service factory - creates new instance each time (needs connection management)
    def create_lxc_service(cfg):
        return LXCService(cfg.proxmox_host, cfg.ssh)

    di.lxc_service = providers.Factory(create_lxc_service, cfg=di.config)

    # PCT service factory - depends on LXC service
    di.pct_service = providers.Factory(
        PCTService,
        lxc_service=di.lxc_service,
    )

    di.deploy = providers.Factory(
        Deploy,
        cfg=di.config,
        lxc_service=di.lxc_service,
        pct_service=di.pct_service,
    )

    di.cleanup = providers.Factory(
        Cleanup,
        cfg=di.config,
        lxc_service=di.lxc_service,
        pct_service=di.pct_service,
    )
    
    di.status = providers.Factory(
        Status,
        cfg=di.config,
        lxc_service=di.lxc_service,
        pct_service=di.pct_service,
    )

    di.backup = providers.Factory(
        Backup,
        cfg=di.config,
        lxc_service=di.lxc_service,
        pct_service=di.pct_service,
    )

    di.restore = providers.Factory(
        Restore,
        cfg=di.config,
        lxc_service=di.lxc_service,
        pct_service=di.pct_service,
    )

    # Resolve and run command classes from DI
    if args.command == "deploy":
        di.deploy().run(args)

    elif args.command == "cleanup":
        cleanup = di.cleanup()
        cleanup.run(args)

    elif args.command == "redeploy":
        logger.info("=" * 50)
        logger.info("Redeploy: Cleanup and Deploy")
        logger.info("=" * 50)
        logger.info("\n[1/2] Running cleanup...")
        cleanup = di.cleanup()
        cleanup.run(args)

        logger.info("\n[2/2] Running deploy...")
        deploy = di.deploy()
        deploy.run(args)

        logger.info("=" * 50)
        logger.info("Redeploy completed!")
        logger.info("=" * 50)

    elif args.command == "status":
        di.status().run(args)

    elif args.command == "backup":
        di.backup().run(args)

    elif args.command == "restore":
        di.restore().run(args)

    else:
        parser.print_help()
if __name__ == "__main__":
    main()