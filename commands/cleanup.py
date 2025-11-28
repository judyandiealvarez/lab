"""Cleanup command orchestration."""
from __future__ import annotations
import sys
from dataclasses import dataclass
from typing import List
from cli import PCT, Find
from libs import common
from libs.logger import get_logger
from libs.command import Command
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)
destroy_container = common.destroy_container

class CleanupError(RuntimeError):
    """Raised when cleanup fails."""
@dataclass

class Cleanup(Command):
    """Holds cleanup context."""
    lxc_service: LXCService = None
    pct_service: PCTService = None

    def run(self, args):
        """Execute the cleanup workflow."""
        import traceback
        try:
            logger.info("=" * 50)
            logger.info("Cleaning Up Lab Environment")
            logger.info("=" * 50)

            logger.info("Destroying ALL containers and templates...")

            # Connect LXC service (injected via DI)
            if not self.lxc_service.connect():
                logger.error("Failed to connect to Proxmox host %s", self.cfg.proxmox_host)
                raise CleanupError("Failed to connect to Proxmox host")

            try:
                self._destroy_containers()
                self._remove_templates()
            finally:
                if self.lxc_service:
                    self.lxc_service.disconnect()
        except CleanupError as err:
            logger.error("Error during cleanup: %s", err)
            logger.error(traceback.format_exc())
            sys.exit(1)

    def _destroy_containers(self):
        logger.info("Stopping and destroying containers...")

        container_ids = self._list_container_ids()
        total = len(container_ids)

        if total == 0:
            logger.info("No containers found")
            return

        logger.info("Found %s containers to destroy: %s", total, ", ".join(container_ids))

        for idx, cid in enumerate(container_ids, 1):
            logger.info("[%s/%s] Processing container %s...", idx, total, cid)
            destroy_container(self.cfg.proxmox_host, cid, cfg=self.cfg, lxc_service=self.lxc_service)

        self._verify_containers_removed()

    def _list_container_ids(self) -> List[str]:
        list_cmd = PCT().status()
        result, exit_code = self.lxc_service.execute(list_cmd)
        container_ids: List[str] = []

        if result:
            lines = result.strip().split("\n")

            for line in lines[1:]:
                parts = line.split()

                if parts and parts[0].isdigit():
                    container_ids.append(parts[0])

        return container_ids

    def _verify_containers_removed(self):
        logger.info("Verifying all containers are destroyed...")

        remaining_result, exit_code = self.lxc_service.execute(PCT().status())
        remaining_ids: List[str] = []

        if remaining_result:
            remaining_lines = remaining_result.strip().split("\n")
            for line in remaining_lines[1:]:
                parts = line.split()
                if parts and parts[0].isdigit():
                    remaining_ids.append(parts[0])

        if remaining_ids:
            raise CleanupError(f"{len(remaining_ids)} containers still exist: {', '.join(remaining_ids)}")

        logger.info("All containers destroyed")

    def _remove_templates(self):
        logger.info("Removing templates...")
        template_dir = self.cfg.proxmox_template_dir

        logger.info("Cleaning template directory %s...", template_dir)
        count_cmd = Find().directory(template_dir).maxdepth(1).type("f").name("*.tar.zst").count()
        count_result, exit_code = self.lxc_service.execute(count_cmd)
        template_count = count_result.strip() if count_result else "0"

        logger.info("Removing %s template files...", template_count)
        delete_cmd = Find().directory(template_dir).maxdepth(1).type("f").name("*.tar.zst").delete()
        self.lxc_service.execute(delete_cmd)

        logger.info("Templates removed")
