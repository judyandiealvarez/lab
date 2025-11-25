#!/usr/bin/env python3
"""
Lab CLI Tool - Manage Proxmox LXC containers and Docker Swarm
Implements all functionality directly in Python (not just calling bash scripts)
"""
import argparse
import logging
import sys
import traceback
from pathlib import Path
from cli import Docker, PCT
from commands import CleanupError, DeployError, run_cleanup, run_deploy
from libs.container_manager import create_container
from tmpl import load_template_handler
from libs import common, container, template
from libs.config import LabConfig
from libs.logger import get_logger, init_logger
SCRIPT_DIR = Path(__file__).parent.absolute()
CONFIG_FILE = SCRIPT_DIR / "lab.yaml"
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
logger = get_logger(__name__)

def load_config() -> dict:
    """Load configuration from lab.yaml as dictionary"""
    if not CONFIG_FILE.exists():
        logger.error("Configuration file %s not found", CONFIG_FILE)
        sys.exit(1)
    if not HAS_YAML:
        logger.error("PyYAML is required. Install it with: pip install pyyaml")
        sys.exit(1)
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
        return config
    except (OSError, yaml.YAMLError) as err:
        logger.error("Error loading configuration: %s", err)
        sys.exit(1)

def get_config() -> LabConfig:
    """Get configuration and return as LabConfig instance"""
    config_dict = load_config()
    config = LabConfig.from_dict(config_dict)
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
get_template_path = container.get_template_path
setup_container_base = container.setup_container_base

def create_container(container_cfg, cfg: LabConfig, step_num, total_steps):
    """Generic container creation dispatcher based on type - uses dynamic loading"""
    container_type = container_cfg.type
    container_name = container_cfg.name
    logger.info("[%s/%s] Creating container '%s' (type: %s)...", step_num, total_steps, container_name, container_type)
    # Dynamically load container handler
    # Use common container manager for all container types
    container_handler = create_container
    if not container_handler:
        logger.error("Unknown container type '%s'", container_type)
        return False
    # Call the handler
    return container_handler(container_cfg, cfg)

def create_template(template_cfg, cfg: LabConfig, step_num, total_steps):
    """Generic template creation dispatcher based on type - uses dynamic loading"""
    template_type = template_cfg.type
    template_name = template_cfg.name
    logger.info("[%s/%s] Creating template '%s' (type: %s)...", step_num, total_steps, template_name, template_type)
    # IP address is already computed in template_cfg.ip_address
    # Dynamically load template handler
    template_handler = load_template_handler(template_type)
    if not template_handler:
        logger.error("Unknown template type '%s'", template_type)
        return False
    # Call the handler
    return template_handler(template_cfg, cfg)

def cmd_deploy(args=None):
    """Deploy complete lab: apt-cache, templates, and Docker Swarm"""
    if args is None:
        args = argparse.Namespace(start_step=1, end_step=None)
    cfg = get_config()
    try:
        run_deploy(cfg, start_step=args.start_step, end_step=args.end_step)
    except DeployError as err:
        logger.error("Error during deployment: %s", err)
        logger.error(traceback.format_exc())
        sys.exit(1)

def cmd_cleanup(args=None):
    """Remove all containers and templates"""
    try:
        cfg = get_config()
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error("Error loading configuration: %s", err)
        logger.error(traceback.format_exc())
        sys.exit(1)
    try:
        run_cleanup(cfg)
    except CleanupError as err:
        logger.error("Error during cleanup: %s", err)
        logger.error(traceback.format_exc())
        sys.exit(1)

def cmd_redeploy(args=None):
    """Cleanup and then deploy complete lab"""
    if args is None:
        args = argparse.Namespace(start_step=1, end_step=None)
    logger.info("=" * 50)
    logger.info("Redeploy: Cleanup and Deploy")
    logger.info("=" * 50)
    try:
        cfg = get_config()
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error("Error loading configuration: %s", err)
        logger.error(traceback.format_exc())
        sys.exit(1)
    # Run cleanup first
    logger.info("\n[1/2] Running cleanup...")
    try:
        run_cleanup(cfg)
    except CleanupError as err:
        logger.error("Error during cleanup: %s", err)
        logger.error(traceback.format_exc())
        sys.exit(1)
    # Then run deploy
    logger.info("\n[2/2] Running deploy...")
    try:
        run_deploy(cfg, start_step=args.start_step, end_step=args.end_step)
    except DeployError as err:
        logger.error("Error during deployment: %s", err)
        logger.error(traceback.format_exc())
        sys.exit(1)
    logger.info("\n" + "=" * 50)
    logger.info("Redeploy completed successfully!")
    logger.info("=" * 50)

def cmd_status():
    """Show current lab status"""
    cfg = get_config()
    logger.info("=" * 50)
    logger.info("Lab Status")
    logger.info("=" * 50)
    # Check containers
    logger.info("\nContainers:")
    list_cmd = PCT().status()
    result = ssh_exec(cfg.proxmox_host, list_cmd, check=False, cfg=cfg)
    if result:
        logger.info(result)
    else:
        logger.info("  No containers found")
    # Check templates
    template_dir = cfg.proxmox_template_dir
    logger.info("\nTemplates:")
    result = ssh_exec(
        cfg.proxmox_host,
        f"ls -lh {template_dir}/*.tar.zst 2>/dev/null || echo 'No templates'",
        check=False,
        cfg=cfg,
    )
    if result:
        logger.info(result)
    else:
        logger.info("  No templates found")
    # Check swarm status
    logger.info("\nDocker Swarm:")
    # Get manager from containers
    manager_configs = [c for c in cfg.containers if c.type == "swarm-manager"]
    if not manager_configs:
        logger.info("  No swarm manager found in configuration")
        return
    manager_id = manager_configs[0].id
    # Check if container exists before trying to run commands on it
    if not container_exists(cfg.proxmox_host, manager_id, cfg=cfg):
        logger.info("  Swarm manager container does not exist")
        return
    # Find docker command path
    find_docker_cmd = Docker().find_docker()
    docker_path = pct_exec(cfg.proxmox_host, manager_id, find_docker_cmd, timeout=10, cfg=cfg)
    docker_cmd = docker_path.strip() if docker_path and docker_path.strip() else "docker"
    node_ls_cmd = Docker().docker_cmd(docker_cmd).node_ls()
    result = pct_exec(
        cfg.proxmox_host,
        manager_id,
        f"{node_ls_cmd} 2>/dev/null || echo 'Swarm not initialized or manager not available'",
        check=False,
        cfg=cfg,
    )
    if result:
        logger.info(result)
    else:
        logger.info("  Swarm not available")

def main():
    """Main CLI entry point"""
    # Initialize logging
    init_logger(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Lab CLI - Manage Proxmox LXC containers and Docker Swarm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    deploy_parser = subparsers.add_parser("deploy", help="Deploy complete lab: apt-cache, templates, and Docker Swarm")
    deploy_parser.add_argument("--start-step", type=int, default=1, help="Start from this step (default: 1)")
    deploy_parser.add_argument("--end-step", type=int, default=None, help="End at this step (default: last step)")
    deploy_parser.set_defaults(func=cmd_deploy)
    cleanup_parser = subparsers.add_parser("cleanup", help="Remove all containers and templates")
    cleanup_parser.set_defaults(func=cmd_cleanup)
    redeploy_parser = subparsers.add_parser("redeploy", help="Cleanup and then deploy complete lab")
    redeploy_parser.add_argument("--start-step", type=int, default=1, help="Start from this step (default: 1)")
    redeploy_parser.add_argument("--end-step", type=int, default=None, help="End at this step (default: last step)")
    redeploy_parser.set_defaults(func=cmd_redeploy)
    status_parser = subparsers.add_parser("status", help="Show current lab status")
    status_parser.set_defaults(func=cmd_status)
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
if __name__ == "__main__":
    main()