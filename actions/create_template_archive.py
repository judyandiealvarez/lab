"""
Create template archive action
"""
import logging
import time
from datetime import datetime
from .base import Action
from cli import Vzdump, PCT
from libs import common

logger = logging.getLogger(__name__)

def _wait_for_archive_file(proxmox_host, container_id, dumpdir, cfg, max_wait=120):
    """Wait for archive file to be created and stable (not growing)."""
    wait_count = 0
    last_size = 0
    stable_count = 0
    backup_file = None
    while wait_count < max_wait:
        time.sleep(2)
        wait_count += 2
        # Find the archive file in dumpdir (where vzdump creates it)
        find_archive_cmd = Vzdump().find_archive(dumpdir, container_id)
        backup_file = common.ssh_exec(proxmox_host, find_archive_cmd, cfg=cfg)
        if not backup_file:
            continue
        backup_file = backup_file.strip()
        # Check file size
        size_cmd = Vzdump().get_archive_size(backup_file)
        size_check = common.ssh_exec(proxmox_host, size_cmd, cfg=cfg)
        if not size_check:
            continue
        current_size = Vzdump.parse_archive_size(size_check)
        if not current_size or current_size <= 0:
            continue
        if current_size == last_size:
            stable_count += 1
            if stable_count >= 3:  # File size stable for 6 seconds
                break
        else:
            stable_count = 0
            last_size = current_size
    return backup_file

class CreateTemplateArchiveAction(Action):
    """Create template archive from container"""
    description = "template archive creation"

    def execute(self) -> bool:
        """Create template archive"""
        proxmox_host = self.cfg.proxmox_host
        container_id = self.container_id
        template_dir = self.cfg.proxmox_template_dir
        # Stop container
        logger.info("Stopping container...")
        stop_cmd = PCT().container_id(container_id).stop()
        try:
            stop_output = common.ssh_exec(proxmox_host, stop_cmd, cfg=self.cfg)
        except Exception:
            logger.warning("Stop container had issues, trying force stop")
            force_stop_cmd = PCT().container_id(container_id).force(True).stop()
            try:
                common.ssh_exec(proxmox_host, force_stop_cmd, cfg=self.cfg)
            except Exception:
                logger.warning("Force stop also failed, continuing anyway")
        time.sleep(2)
        # Create template archive
        logger.info("Creating template archive for container %s in directory %s", container_id, template_dir)
        vzdump_cmd = Vzdump().compress("zstd").mode("stop").create_template(container_id, template_dir)
        logger.info("Executing vzdump command: %s", vzdump_cmd)
        try:
            vzdump_output = common.ssh_exec(proxmox_host, vzdump_cmd, cfg=self.cfg)
            logger.info("vzdump output (first 500 chars): %s", vzdump_output[:500] if vzdump_output else "None")
        except Exception as exc:
            logger.error("vzdump command failed with exception: %s", exc, exc_info=True)
            return False
        if not vzdump_output:
            logger.error("vzdump produced no output - command may have failed silently")
            return False
        # Wait for archive file to be created and stable (vzdump creates in dumpdir)
        logger.info("Waiting for template archive to be ready (max 120 seconds)...")
        backup_file = _wait_for_archive_file(proxmox_host, container_id, template_dir, self.cfg, max_wait=120)
        if not backup_file:
            logger.error("Template archive file not found after vzdump in directory %s", template_dir)
            logger.error("Checking if any vzdump files exist in %s...", template_dir)
            check_cmd = f"ls -la {template_dir}/*vzdump* 2>&1 | head -10"
            check_output = common.ssh_exec(proxmox_host, check_cmd, cfg=self.cfg)
            logger.error("Files in template directory: %s", check_output if check_output else "None found")
            return False
        logger.info("Template archive file found: %s", backup_file)
        # Verify archive is not empty and has reasonable size (> 10MB)
        size_cmd = Vzdump().get_archive_size(backup_file)
        size_check = common.ssh_exec(proxmox_host, size_cmd, cfg=self.cfg)
        if not size_check:
            logger.error("Failed to get archive file size")
            return False
        file_size = Vzdump.parse_archive_size(size_check)
        if not file_size or file_size < 10485760:  # Less than 10MB is suspicious
            logger.error("Template archive is too small (%s bytes if found), likely corrupted", file_size)
            return False
        logger.info("Template archive size: %.2f MB", file_size / 1048576)
        # Rename template and move to storage location (local:vztmpl/)
        # Use template name directly in filename
        template_name = self.container_cfg.name if self.container_cfg else "template"
        date_str = datetime.now().strftime("%Y%m%d")
        final_template_name = f"{template_name}_{date_str}_amd64.tar.zst"
        logger.info("Final template name: %s", final_template_name)
        # Templates need to be in storage template dir (not cache/) to be visible as local:vztmpl/
        storage_template_dir = self.cfg.proxmox.template_dir
        storage_template_path = f"{storage_template_dir}/{final_template_name}"
        logger.info("Moving template from %s to %s", backup_file, storage_template_path)
        move_cmd = f"mv '{backup_file}' {storage_template_path} 2>&1"
        try:
            move_output = common.ssh_exec(proxmox_host, move_cmd, cfg=self.cfg)
            if move_output:
                logger.info("Move command output: %s", move_output)
        except Exception as exc:
            logger.error("Failed to move template archive to storage: %s", exc, exc_info=True)
            return False
        logger.info("Template moved to storage location: %s", storage_template_path)
        # Update template list
        pveam_cmd = "pveam update 2>&1"
        pveam_output = common.ssh_exec(proxmox_host, pveam_cmd, cfg=self.cfg)
        if not pveam_output:
            logger.warning("pveam update had issues")
        # Cleanup other templates from both cache and storage directories
        logger.info("Cleaning up other template archives...")
        preserve_patterns = " ".join([f"! -name '{p}'" for p in self.cfg.template_config.preserve])
        # Cleanup cache directory
        cleanup_cache_cmd = (
            f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
            f"! -name '{final_template_name}' {preserve_patterns} -delete 2>&1"
        )
        common.ssh_exec(proxmox_host, cleanup_cache_cmd, cfg=self.cfg)
        # Cleanup storage directory (but keep base templates)
        cleanup_storage_cmd = (
            f"find {storage_template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
            f"! -name '{final_template_name}' {preserve_patterns} "
            f"! -name 'ubuntu-24.10-standard_24.10-1_amd64.tar.zst' -delete 2>&1"
        )
        common.ssh_exec(proxmox_host, cleanup_storage_cmd, cfg=self.cfg)
        # Destroy container after archive is created
        from libs.common import destroy_container
        # Use pct_service's lxc service to reuse connection
        lxc_svc = self.pct_service.lxc if self.pct_service else None
        try:
            destroy_container(proxmox_host, container_id, cfg=self.cfg, lxc_service=lxc_svc)
            logger.info("Container %s destroyed after template archive creation", container_id)
        except Exception as exc:
            logger.error("Failed to destroy container %s: %s", container_id, exc)
            return False
        return True

