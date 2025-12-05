"""
Template Service - manages template operations on Proxmox host
"""
import logging
from typing import Optional
from libs.config import LabConfig
from .lxc import LXCService
logger = logging.getLogger(__name__)

class TemplateService:
    """Service for managing templates on Proxmox host"""
    def __init__(self, lxc_service: LXCService):
        """
        Initialize Template service
        Args:
            lxc_service: LXC service instance with SSH connection
        """
        self.lxc = lxc_service

    def get_base_template(self, cfg: LabConfig) -> Optional[str]:
        """
        Get base Ubuntu template, download if needed
        Args:
            cfg: Lab configuration
        Returns:
            Template name or None if download failed
        """
        templates = cfg.template_config.base
        template_dir = cfg.proxmox_template_dir
        for template in templates:
            check_cmd = f"test -f {template_dir}/{template} && echo exists || echo missing"
            check_result, _ = self.lxc.execute(check_cmd)
            if check_result and "exists" in check_result:
                return template
        # Download last template in list
        template_to_download = templates[-1]
        logger.info("Base template not found. Downloading %s...", template_to_download)
        # Run pveam download with live output
        download_cmd = f"pveam download local {template_to_download}"
        logger.info("Running: %s", download_cmd)
        output, exit_code = self.lxc.execute(download_cmd, timeout=300)
        # Verify download completed
        verify_cmd = f"test -f {template_dir}/{template_to_download} && echo exists || echo missing"
        verify_result, _ = self.lxc.execute(verify_cmd)
        if not verify_result or "exists" not in verify_result:
            error_msg = f"Template {template_to_download} download failed"
            if exit_code is not None and exit_code != 0:
                error_msg += f" (exit code: {exit_code})"
            if output:
                error_msg += f". Output: {output}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        logger.info("Template %s downloaded successfully", template_to_download)
        return template_to_download

    def get_template_path(self, template_name: Optional[str], cfg: LabConfig) -> str:
        """
        Get path to template file by template name
        Args:
            template_name: Template name or None for base template
            cfg: Lab configuration
        Returns:
            Full path to template file
        Raises:
            RuntimeError: If template cannot be found or downloaded
        """
        template_dir = cfg.proxmox_template_dir
        # If template_name is None, use base template directly
        if template_name is None:
            base_template = self.get_base_template(cfg)
            if not base_template:
                raise RuntimeError("Failed to get base template")
            return f"{template_dir}/{base_template}"
        # Find template config
        template_cfg = None
        for tmpl in cfg.templates:
            if tmpl.name == template_name:
                template_cfg = tmpl
                break
        if not template_cfg:
            # Fallback to base template
            base_template = self.get_base_template(cfg)
            if not base_template:
                raise RuntimeError("Failed to get base template")
            return f"{template_dir}/{base_template}"
        # Find template file by name - search for files matching template name
        template_name_pattern = f"{template_cfg.name}*.tar.zst"
        find_cmd = f"ls -t {template_dir}/{template_name_pattern} 2>/dev/null | head -1 | xargs basename 2>/dev/null"
        template_file, _ = self.lxc.execute(find_cmd)
        if template_file:
            return f"{template_dir}/{template_file.strip()}"
        # Fallback to base template
        base_template = self.get_base_template(cfg)
        if not base_template:
            raise RuntimeError("Failed to get base template")
        return f"{template_dir}/{base_template}"

    def validate_template(self, template_path: str) -> bool:
        """
        Validate that template file exists and is readable
        Args:
            template_path: Full path to template file
        Returns:
            True if template is valid, False otherwise
        """
        validate_cmd = f"test -f {template_path} && test -r {template_path} && echo 'valid' || echo 'invalid'"
        result, _ = self.lxc.execute(validate_cmd)
        return result is not None and "valid" in result