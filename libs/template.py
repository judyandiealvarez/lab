"""
Template-specific functions - only used by template modules
"""
import sys
import logging
from typing import Optional
from .common import ssh_exec
from .config import LabConfig

# Get logger for this module
logger = logging.getLogger(__name__)


def get_base_template(proxmox_host: str, cfg: LabConfig) -> Optional[str]:
    """Get base Ubuntu template, download if needed"""
    templates = cfg.template_config.base
    template_dir = cfg.proxmox_template_dir
    
    for template in templates:
        check_result = ssh_exec(proxmox_host, f"test -f {template_dir}/{template} && echo exists || echo missing", check=False, capture_output=True, cfg=cfg)
        if check_result and "exists" in check_result:
            return template
    
    # Download last template in list
    template_to_download = templates[-1]
    logger.info(f"Base template not found. Downloading {template_to_download}...")
    
    # Run pveam download with live output (no capture_output so we see progress)
    download_cmd = f"pveam download local {template_to_download}"
    logger.info(f"Running: {download_cmd}")
    # Use timeout of 300 seconds (5 minutes) for download
    ssh_exec(proxmox_host, download_cmd, check=False, capture_output=False, timeout=300, cfg=cfg)
    
    # Verify download completed
    verify_result = ssh_exec(proxmox_host, f"test -f {template_dir}/{template_to_download} && echo exists || echo missing", check=False, capture_output=True, cfg=cfg)
    if not verify_result or "exists" not in verify_result:
        logger.error(f"Template {template_to_download} was not downloaded successfully")
        return None
    
    logger.info(f"Template {template_to_download} downloaded successfully")
    return template_to_download

