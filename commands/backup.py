"""Backup command orchestration."""
from __future__ import annotations
import sys
from datetime import datetime
from dataclasses import dataclass
from libs.logger import get_logger
from libs.command import Command
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)


class BackupError(RuntimeError):
    """Raised when backup fails."""


@dataclass
class Backup(Command):
    """Backup command class."""
    lxc_service: LXCService = None
    pct_service: PCTService = None

    def run(self, args):
        """Execute the backup workflow."""
        import traceback
        try:
            logger.info("=" * 50)
            logger.info("Backing Up Cluster")
            logger.info("=" * 50)

            if not self.cfg.backup:
                logger.error("Backup configuration not found in lab.yaml")
                raise BackupError("Backup configuration not found")

            # Connect LXC service
            if not self.lxc_service.connect():
                logger.error("Failed to connect to Proxmox host %s", self.cfg.proxmox_host)
                raise BackupError("Failed to connect to Proxmox host")

            try:
                # Find backup container
                backup_container = None
                for container in self.cfg.containers:
                    if container.id == self.cfg.backup.container_id:
                        backup_container = container
                        break

                if not backup_container:
                    logger.error("Backup container with ID %s not found", self.cfg.backup.container_id)
                    raise BackupError(f"Backup container with ID {self.cfg.backup.container_id} not found")

                # Create backup name with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"{self.cfg.backup.name_prefix}-{timestamp}"

                logger.info("Creating backup: %s", backup_name)
                logger.info("Backup will be stored on container %s at %s", backup_container.id, self.cfg.backup.backup_dir)

                # Ensure backup directory exists on backup container
                mkdir_cmd = f"mkdir -p {self.cfg.backup.backup_dir}"
                mkdir_output, mkdir_exit = self.pct_service.execute(str(backup_container.id), mkdir_cmd)
                if mkdir_exit != 0:
                    logger.error("Failed to create backup directory: %s", mkdir_output)
                    raise BackupError("Failed to create backup directory")

                # Process each backup item
                backup_files = []
                for item in self.cfg.backup.items:
                    logger.info("Backing up item: %s from container %s", item.name, item.source_container_id)

                    # Find source container
                    source_container = None
                    for container in self.cfg.containers:
                        if container.id == item.source_container_id:
                            source_container = container
                            break

                    if not source_container:
                        logger.error("Source container %s not found for backup item %s", item.source_container_id, item.name)
                        raise BackupError(f"Source container {item.source_container_id} not found")

                    # Create temporary backup file name
                    temp_file = f"/tmp/{backup_name}-{item.name}"

                    if item.archive_base and item.archive_path:
                        # Create archive
                        logger.info("  Archiving: %s", item.source_path)
                        archive_file = f"{temp_file}.tar.gz"
                        
                        # For k3s-control-data, ensure SQLite database is backed up consistently
                        # SQLite WAL mode requires checkpointing to ensure all transactions are in the main DB file
                        # Even if services are stopped, checkpoint ensures WAL data is committed to main DB
                        if item.name == "k3s-control-data":
                            logger.info("  Checkpointing SQLite WAL to ensure all data is in main database file...")
                            db_path = f"{item.source_path}/server/db/state.db"
                            # Checkpoint WAL to ensure all transactions are in the main database file
                            # This is critical: WAL file may not be included in tar or may be inconsistent
                            checkpoint_cmd = f"python3 -c \"import sqlite3; conn = sqlite3.connect('{db_path}'); result = conn.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone(); conn.close(); print(f'Checkpoint: {{result}}')\" 2>&1"
                            checkpoint_output, checkpoint_exit = self.pct_service.execute(str(item.source_container_id), checkpoint_cmd, timeout=60)
                            if checkpoint_exit == 0:
                                logger.info("  SQLite WAL checkpoint completed: %s", checkpoint_output.strip() if checkpoint_output else "OK")
                            else:
                                logger.warning("  SQLite WAL checkpoint failed (may not be in WAL mode): %s", checkpoint_output)
                        
                        # Use --warning=no-file-changed to ignore "file changed as we read it" warnings (common for live databases)
                        archive_cmd = f"tar --warning=no-file-changed -czf {archive_file} -C {item.archive_base} {item.archive_path} 2>&1"
                        archive_output, archive_exit = self.pct_service.execute(str(item.source_container_id), archive_cmd, timeout=300)
                        
                        # Restore original database if we replaced it
                        if item.name == "k3s-control-data" and restore_after_archive:
                            restore_after_archive()
                        
                        # Check if archive file was created (exit code 1 is OK if file changed during backup)
                        verify_cmd = f"test -f {archive_file} && echo exists || echo missing"
                        verify_output, verify_exit = self.pct_service.execute(str(item.source_container_id), verify_cmd, timeout=10)
                        if verify_exit != 0 or not verify_output or "missing" in verify_output:
                            logger.error("  Failed to create archive for %s: %s", item.name, archive_output)
                            raise BackupError(f"Failed to create archive for {item.name}")
                        if archive_exit != 0:
                            # Archive was created but tar returned non-zero (likely due to file changes during backup)
                            logger.warning("  Archive created with warnings (file changed during backup - expected for live databases)")
                        backup_files.append((item.source_container_id, archive_file, f"{item.name}.tar.gz"))
                    else:
                        # Copy file directly
                        logger.info("  Copying file: %s", item.source_path)
                        copy_cmd = f"cp {item.source_path} {temp_file} 2>&1"
                        copy_output, copy_exit = self.pct_service.execute(str(item.source_container_id), copy_cmd, timeout=60)
                        if copy_exit != 0:
                            logger.error("  Failed to copy file for %s: %s", item.name, copy_output)
                            raise BackupError(f"Failed to copy file for {item.name}")
                        backup_files.append((item.source_container_id, temp_file, item.name))

                # Copy all backup files to backup container
                logger.info("Copying backup files to backup container...")
                backup_file_names = []
                for source_id, temp_file, dest_name in backup_files:
                    # Copy via Proxmox host
                    proxmox_temp = f"/tmp/{backup_name}-{dest_name}"
                    # Pull from source container
                    pull_cmd = f"pct pull {source_id} {temp_file} {proxmox_temp}"
                    pull_output, pull_exit = self.lxc_service.execute(pull_cmd, timeout=300)
                    if pull_exit != 0:
                        logger.error("Failed to pull file from container %s: %s", source_id, pull_output)
                        raise BackupError(f"Failed to pull file from container {source_id}")
                    # Push to backup container
                    backup_file_path = f"{self.cfg.backup.backup_dir}/{backup_name}-{dest_name}"
                    push_cmd = f"pct push {backup_container.id} {proxmox_temp} {backup_file_path}"
                    push_output, push_exit = self.lxc_service.execute(push_cmd, timeout=300)
                    if push_exit != 0:
                        logger.error("Failed to push file to backup container: %s", push_output)
                        raise BackupError("Failed to push file to backup container")
                    # Clean up proxmox temp
                    self.lxc_service.execute(f"rm -f {proxmox_temp}", timeout=30)
                    # Clean up source temp
                    self.pct_service.execute(str(source_id), f"rm -f {temp_file} {temp_file}.tar.gz 2>&1 || true", timeout=30)
                    backup_file_names.append(f"{backup_name}-{dest_name}")

                # Create final tarball on backup container
                logger.info("Creating final backup tarball...")
                final_tarball = f"{self.cfg.backup.backup_dir}/{backup_name}.tar.gz"
                tar_files_list = " ".join(backup_file_names)
                tar_cmd = f"cd {self.cfg.backup.backup_dir} && tar -czf {backup_name}.tar.gz {tar_files_list} 2>&1"
                tar_output, tar_exit = self.pct_service.execute(str(backup_container.id), tar_cmd, timeout=300)
                if tar_exit != 0:
                    logger.error("Failed to create final tarball: %s", tar_output)
                    raise BackupError("Failed to create final tarball")

                # Clean up individual files
                cleanup_cmd = f"cd {self.cfg.backup.backup_dir} && rm -f {tar_files_list} 2>&1 || true"
                self.pct_service.execute(str(backup_container.id), cleanup_cmd, timeout=30)

                logger.info("=" * 50)
                logger.info("Backup completed successfully!")
                logger.info("Backup name: %s", backup_name)
                logger.info("Backup location: %s on container %s", final_tarball, backup_container.id)
                logger.info("=" * 50)

            except BackupError:
                raise
            except Exception as err:
                logger.error("Unexpected error during backup: %s", err)
                logger.error(traceback.format_exc())
                raise BackupError(f"Backup failed: {err}") from err
        finally:
            if self.lxc_service:
                self.lxc_service.disconnect()

