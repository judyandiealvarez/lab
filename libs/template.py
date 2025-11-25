"""
Template-specific functions - only used by template modules
"""
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
        check_result = ssh_exec(
            proxmox_host,
            f"test -f {template_dir}/{template} && echo exists || echo missing",
            check=False,
            cfg=cfg,
        )
        if check_result and "exists" in check_result:
            return template
    # Download last template in list
    template_to_download = templates[-1]
    logger.info("Base template not found. Downloading %s...", template_to_download)
    # Run pveam download with live output (no capture_output so we see progress)
    download_cmd = f"pveam download local {template_to_download}"
    logger.info("Running: %s", download_cmd)
    # Use timeout of 300 seconds (5 minutes) for download
    ssh_exec(proxmox_host, download_cmd, check=False, timeout=300, cfg=cfg, force_tty=True)
    # Verify download completed
    verify_result = ssh_exec(
        proxmox_host,
        f"test -f {template_dir}/{template_to_download} && echo exists || echo missing",
        check=False,
        cfg=cfg,
    )
    if not verify_result or "exists" not in verify_result:
        logger.error("Template %s was not downloaded successfully", template_to_download)
        return None
    logger.info("Template %s downloaded successfully", template_to_download)
    return template_to_download