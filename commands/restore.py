"""Restore command orchestration."""
from __future__ import annotations
import sys
from dataclasses import dataclass
from libs.logger import get_logger
from libs.command import Command
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)


class RestoreError(RuntimeError):
    """Raised when restore fails."""


@dataclass
class Restore(Command):
    """Restore command class."""
    lxc_service: LXCService = None
    pct_service: PCTService = None

    def run(self, args):
        """Execute the restore workflow."""
        import traceback
        try:
            logger.info("=" * 50)
            logger.info("Restoring Cluster from Backup")
            logger.info("=" * 50)

            if not args.backup_name:
                logger.error("Backup name is required. Use --backup-name <name>")
                raise RestoreError("Backup name is required")

            if not self.cfg.backup:
                logger.error("Backup configuration not found in lab.yaml")
                raise RestoreError("Backup configuration not found")

            backup_name = args.backup_name

            # Connect LXC service
            if not self.lxc_service.connect():
                logger.error("Failed to connect to Proxmox host %s", self.cfg.proxmox_host)
                raise RestoreError("Failed to connect to Proxmox host")

            try:
                # Find backup container
                backup_container = None
                for container in self.cfg.containers:
                    if container.id == self.cfg.backup.container_id:
                        backup_container = container
                        break

                if not backup_container:
                    logger.error("Backup container with ID %s not found", self.cfg.backup.container_id)
                    raise RestoreError(f"Backup container with ID {self.cfg.backup.container_id} not found")

                backup_tarball = f"{self.cfg.backup.backup_dir}/{backup_name}.tar.gz"

                logger.info("Restoring from backup: %s", backup_name)
                logger.info("Backup location: %s", backup_tarball)

                # Verify backup exists
                logger.info("Verifying backup exists...")
                check_cmd = f"test -f {backup_tarball} && echo exists || echo missing"
                check_output, check_exit = self.pct_service.execute(str(backup_container.id), check_cmd, timeout=30)
                if check_exit != 0 or not check_output or "missing" in check_output:
                    logger.error("Backup not found at %s", backup_tarball)
                    raise RestoreError(f"Backup not found: {backup_name}")

                # Extract backup tarball on backup container
                logger.info("Extracting backup tarball...")
                extract_dir = f"{self.cfg.backup.backup_dir}/{backup_name}"
                extract_cmd = f"mkdir -p {extract_dir} && cd {self.cfg.backup.backup_dir} && tar -xzf {backup_name}.tar.gz -C {extract_dir} 2>&1"
                extract_output, extract_exit = self.pct_service.execute(str(backup_container.id), extract_cmd, timeout=300)
                if extract_exit != 0:
                    logger.error("Failed to extract backup: %s", extract_output)
                    raise RestoreError("Failed to extract backup")

                # Stop ALL k3s services before restore (control + all workers)
                control_node_id = None
                worker_node_ids = []
                k3s_items = [item for item in self.cfg.backup.items if item.name.startswith("k3s-")]
                
                if k3s_items:
                    # Get control node ID
                    if self.cfg.kubernetes and self.cfg.kubernetes.control:
                        control_node_id = self.cfg.kubernetes.control[0]
                    
                    # Get worker node IDs
                    if self.cfg.kubernetes and self.cfg.kubernetes.workers:
                        worker_node_ids = self.cfg.kubernetes.workers
                    
                    logger.info("Stopping all k3s services before restore...")
                    
                    # Stop control node k3s service
                    if control_node_id:
                        logger.info("Stopping k3s service on control node %s...", control_node_id)
                        stop_cmd = "systemctl stop k3s"
                        stop_output, stop_exit = self.pct_service.execute(str(control_node_id), stop_cmd, timeout=60)
                        if stop_exit != 0:
                            logger.warning("Failed to stop k3s on control node (may not be running): %s", stop_output)
                        else:
                            logger.info("k3s service stopped on control node")
                    
                    # Stop all worker node k3s-agent services
                    for worker_id in worker_node_ids:
                        logger.info("Stopping k3s-agent service on worker node %s...", worker_id)
                        stop_agent_cmd = "systemctl stop k3s-agent 2>&1 || true"
                        stop_agent_output, stop_agent_exit = self.pct_service.execute(str(worker_id), stop_agent_cmd, timeout=60)
                        if stop_agent_exit != 0:
                            logger.warning("Failed to stop k3s-agent on worker %s (may not be running): %s", worker_id, stop_agent_output)
                        else:
                            logger.info("k3s-agent service stopped on worker %s", worker_id)
                    
                    logger.info("All k3s services stopped")
                    
                    # Clear k3s state on control node if restoring control data
                    control_data_item = next((item for item in self.cfg.backup.items if item.name == "k3s-control-data"), None)
                    if control_node_id and control_data_item:
                        logger.info("Clearing existing k3s state on control node to ensure clean restore...")
                        remove_cmd = f"rm -rf {control_data_item.source_path} && mkdir -p {control_data_item.source_path} && chmod 700 {control_data_item.source_path} 2>&1 || true"
                        remove_output, remove_exit = self.pct_service.execute(str(control_node_id), remove_cmd, timeout=30)
                        if remove_exit != 0:
                            logger.warning("Failed to clear k3s state (may not exist): %s", remove_output)
                        else:
                            logger.info("k3s state cleared on control node")

                # Restore items in correct order: control node first, then workers, then others
                items_to_restore = list(self.cfg.backup.items)
                # Sort: k3s-control-data first, then k3s-control-config, then workers, then others
                items_to_restore.sort(key=lambda x: (
                    0 if x.name == "k3s-control-data" else
                    1 if x.name == "k3s-control-config" else
                    2 if "k3s-worker" in x.name and "data" in x.name else
                    3 if "k3s-worker" in x.name and "config" in x.name else
                    4
                ))
                
                # Restore each item
                for item in items_to_restore:
                    logger.info("Restoring item: %s to container %s", item.name, item.source_container_id)

                    # Find source container
                    source_container = None
                    for container in self.cfg.containers:
                        if container.id == item.source_container_id:
                            source_container = container
                            break

                    if not source_container:
                        logger.error("Source container %s not found for restore item %s", item.source_container_id, item.name)
                        raise RestoreError(f"Source container {item.source_container_id} not found")

                    # Determine backup file name
                    is_archive = item.archive_base and item.archive_path
                    backup_file_name = f"{backup_name}-{item.name}" + (".tar.gz" if is_archive else "")
                    backup_file_path = f"{extract_dir}/{backup_file_name}"

                    # Check if backup file exists
                    check_file_cmd = f"test -f {backup_file_path} && echo exists || echo missing"
                    check_file_output, check_file_exit = self.pct_service.execute(str(backup_container.id), check_file_cmd, timeout=30)
                    if check_file_exit != 0 or not check_file_output or "missing" in check_file_output:
                        logger.warning("Backup file not found for item %s, skipping...", item.name)
                        continue

                    # Copy backup file from backup container to source container via Proxmox host
                    proxmox_temp = f"/tmp/{backup_file_name}"
                    # Pull from backup container
                    pull_cmd = f"pct pull {backup_container.id} {backup_file_path} {proxmox_temp}"
                    pull_output, pull_exit = self.lxc_service.execute(pull_cmd, timeout=300)
                    if pull_exit != 0:
                        logger.error("Failed to pull backup file: %s", pull_output)
                        raise RestoreError("Failed to pull backup file")

                    # Push to source container
                    source_temp = f"/tmp/{backup_file_name}"
                    push_cmd = f"pct push {item.source_container_id} {proxmox_temp} {source_temp}"
                    push_output, push_exit = self.lxc_service.execute(push_cmd, timeout=300)
                    if push_exit != 0:
                        logger.error("Failed to push backup file to source container: %s", push_output)
                        raise RestoreError("Failed to push backup file to source container")

                    # Clean up proxmox temp
                    self.lxc_service.execute(f"rm -f {proxmox_temp}", timeout=30)

                    # Restore based on item type
                    if item.archive_base and item.archive_path:
                        # Extract archive
                        logger.info("  Extracting archive to: %s", item.source_path)
                        # For k3s-etcd, ensure db directory is completely empty before restore
                        # For other items, backup existing data first
                        if item.name == "k3s-etcd":
                            # Remove entire db directory contents to ensure clean restore
                            db_dir = item.source_path
                            remove_db_cmd = f"rm -rf {db_dir}/* 2>&1 || true"
                            self.pct_service.execute(str(item.source_container_id), remove_db_cmd, timeout=30)
                        else:
                            backup_existing_cmd = f"mv {item.source_path} {item.source_path}.backup.$(date +%s) 2>&1 || true"
                            self.pct_service.execute(str(item.source_container_id), backup_existing_cmd, timeout=30)
                        # Extract archive
                        extract_cmd = f"mkdir -p {item.archive_base} && tar -xzf {source_temp} -C {item.archive_base} 2>&1"
                        extract_output, extract_exit = self.pct_service.execute(str(item.source_container_id), extract_cmd, timeout=300)
                        if extract_exit != 0:
                            logger.error("  Failed to extract archive: %s", extract_output)
                            raise RestoreError(f"Failed to extract archive for {item.name}")
                    else:
                        # Copy file directly
                        logger.info("  Copying file to: %s", item.source_path)
                        # Backup existing file first
                        backup_existing_cmd = f"mv {item.source_path} {item.source_path}.backup.$(date +%s) 2>&1 || true"
                        self.pct_service.execute(str(item.source_container_id), backup_existing_cmd, timeout=30)
                        # Copy file
                        copy_cmd = f"cp {source_temp} {item.source_path} 2>&1"
                        copy_output, copy_exit = self.pct_service.execute(str(item.source_container_id), copy_cmd, timeout=60)
                        if copy_exit != 0:
                            logger.error("  Failed to copy file: %s", copy_output)
                            raise RestoreError(f"Failed to copy file for {item.name}")
                        # For k3s-token, also copy to /var/lib/rancher/k3s/server/token (k3s uses this location)
                        if item.name == "k3s-token":
                            token_copy_cmd = f"cp {item.source_path} /var/lib/rancher/k3s/server/token 2>&1"
                            token_copy_output, token_copy_exit = self.pct_service.execute(str(item.source_container_id), token_copy_cmd, timeout=30)
                            if token_copy_exit != 0:
                                logger.warning("  Failed to copy token to server/token location: %s", token_copy_output)
                            else:
                                logger.info("  Token also copied to /var/lib/rancher/k3s/server/token")
                        # For k3s-agent service.env files, ensure directory exists and reload systemd
                        if "service-env" in item.name:
                            # Ensure directory exists
                            dir_path = "/etc/systemd/system"
                            mkdir_cmd = f"mkdir -p {dir_path} 2>&1 || true"
                            self.pct_service.execute(str(item.source_container_id), mkdir_cmd, timeout=30)
                            # Reload systemd daemon to pick up the restored service.env file
                            reload_cmd = "systemctl daemon-reload 2>&1"
                            reload_output, reload_exit = self.pct_service.execute(str(item.source_container_id), reload_cmd, timeout=30)
                            if reload_exit != 0:
                                logger.warning("  Failed to reload systemd daemon: %s", reload_output)
                            else:
                                logger.info("  Systemd daemon reloaded to pick up restored service.env")

                    # Clean up source temp
                    self.pct_service.execute(str(item.source_container_id), f"rm -f {source_temp} 2>&1 || true", timeout=30)


                # Clean up extracted backup directory
                cleanup_cmd = f"rm -rf {extract_dir} 2>&1 || true"
                self.pct_service.execute(str(backup_container.id), cleanup_cmd, timeout=30)

                # Start all k3s services after restore (control first, then workers)
                if k3s_items:
                    import time
                    
                    # Start control node k3s service first
                    if control_node_id:
                        # Remove credential files that might conflict with restored datastore
                        logger.info("Removing credential files that may conflict with restored datastore...")
                        cred_cleanup_cmd = "rm -f /var/lib/rancher/k3s/server/cred/passwd /var/lib/rancher/k3s/server/cred/ipsec.psk 2>&1 || true"
                        self.pct_service.execute(str(control_node_id), cred_cleanup_cmd, timeout=30)
                        
                        # Start k3s service
                        logger.info("Starting k3s service on control node with restored data...")
                        start_cmd = "systemctl start k3s"
                        start_output, start_exit = self.pct_service.execute(str(control_node_id), start_cmd, timeout=60)
                        if start_exit != 0:
                            logger.error("Failed to start k3s on control node: %s", start_output)
                            raise RestoreError("Failed to start k3s after restore")
                        logger.info("k3s service started on control node, waiting for it to be ready...")
                        
                        # Wait for k3s control plane to be ready
                        max_wait = 120
                        wait_time = 0
                        while wait_time < max_wait:
                            check_cmd = "systemctl is-active k3s && kubectl get nodes 2>&1 | grep -q Ready || echo not-ready"
                            check_output, check_exit = self.pct_service.execute(str(control_node_id), check_cmd, timeout=30)
                            if check_exit == 0 and "not-ready" not in check_output:
                                logger.info("k3s control plane is ready")
                                break
                            logger.info("Waiting for k3s control plane to be ready (waited %d/%d seconds)...", wait_time, max_wait)
                            time.sleep(5)
                            wait_time += 5
                        if wait_time >= max_wait:
                            logger.warning("k3s control plane may not be fully ready after restore, but continuing...")
                    
                    # Start all worker node k3s-agent services
                    if worker_node_ids:
                        logger.info("Starting k3s-agent services on worker nodes...")
                        for worker_id in worker_node_ids:
                            logger.info("Starting k3s-agent service on worker node %s...", worker_id)
                            start_agent_cmd = "systemctl start k3s-agent"
                            start_agent_output, start_agent_exit = self.pct_service.execute(str(worker_id), start_agent_cmd, timeout=60)
                            if start_agent_exit != 0:
                                logger.warning("Failed to start k3s-agent on worker %s: %s", worker_id, start_agent_output)
                            else:
                                logger.info("k3s-agent service started on worker %s", worker_id)
                        
                        # Wait a bit for workers to connect
                        logger.info("Waiting for worker nodes to connect to control plane...")
                        time.sleep(10)
                        
                        # Verify worker nodes are connecting
                        if control_node_id:
                            verify_cmd = "kubectl get nodes 2>&1"
                            verify_output, verify_exit = self.pct_service.execute(str(control_node_id), verify_cmd, timeout=30)
                            if verify_exit == 0 and verify_output:
                                logger.info("Current node status:\n%s", verify_output)

                # Fix Rancher first-login setting if it's empty but users exist (inconsistent backup state)
                # This can happen if backup was taken during a write transaction
                if control_node_id:
                    logger.info("Checking Rancher first-login setting consistency...")
                    import time
                    time.sleep(10)  # Wait for Rancher to be ready
                    # Check if first-login is empty
                    check_first_login_cmd = "kubectl get settings.management.cattle.io first-login -o jsonpath='{.value}' 2>&1 || echo 'not-found'"
                    first_login_value, _ = self.pct_service.execute(str(control_node_id), check_first_login_cmd, timeout=30)
                    # Check if admin user exists
                    check_user_cmd = "kubectl get users.management.cattle.io -o jsonpath='{range .items[*]}{.username}{\\\"\\n\\\"}{end}' 2>&1 | grep -q '^admin$' && echo 'exists' || echo 'not-found'"
                    user_exists, _ = self.pct_service.execute(str(control_node_id), check_user_cmd, timeout=30)
                    
                    if first_login_value and first_login_value.strip() == "" and user_exists and "exists" in user_exists:
                        # Inconsistent state: user exists but first-login is empty
                        logger.warning("Detected inconsistent backup state: admin user exists but first-login is empty")
                        logger.info("Fixing first-login setting to 'false'...")
                        patch_cmd = "kubectl patch settings.management.cattle.io first-login --type json -p '[{\"op\": \"replace\", \"path\": \"/value\", \"value\": \"false\"}]' 2>&1"
                        patch_output, patch_exit = self.pct_service.execute(str(control_node_id), patch_cmd, timeout=30)
                        if patch_exit == 0:
                            logger.info("first-login setting fixed to 'false'")
                            # Restart Rancher to apply the change
                            restart_cmd = "kubectl rollout restart deployment rancher -n cattle-system 2>&1"
                            self.pct_service.execute(str(control_node_id), restart_cmd, timeout=30)
                        else:
                            logger.warning("Failed to fix first-login setting: %s", patch_output)

                logger.info("=" * 50)
                logger.info("Restore completed successfully!")
                logger.info("Backup restored: %s", backup_name)
                logger.info("=" * 50)

            except RestoreError:
                raise
            except Exception as err:
                logger.error("Unexpected error during restore: %s", err)
                logger.error(traceback.format_exc())
                raise RestoreError(f"Restore failed: {err}") from err
        finally:
            if self.lxc_service:
                self.lxc_service.disconnect()

