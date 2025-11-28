"""Status command orchestration."""
from dataclasses import dataclass
from cli import PCT
from libs.logger import get_logger
from libs.command import Command
from services.lxc import LXCService
from services.pct import PCTService
logger = get_logger(__name__)


@dataclass
class Status(Command):
    """Status command class."""
    lxc_service: LXCService = None
    pct_service: PCTService = None

    def run(self, args):
        """Show current lab status."""
        # Connect LXC service (injected via DI)
        if not self.lxc_service.connect():
            logger.error("Failed to connect to Proxmox host %s", self.cfg.proxmox_host)
            return
        try:
            logger.info("=" * 50)
            logger.info("Lab Status")
            logger.info("=" * 50)
            # Check containers
            logger.info("Containers:")
            list_cmd = PCT().status()
            result, _ = self.lxc_service.execute(list_cmd)
            if result:
                logger.info(result)
            else:
                logger.info("  No containers found")
            # Check templates
            template_dir = self.cfg.proxmox_template_dir
            logger.info("Templates:")
            template_cmd = f"ls -lh {template_dir}/*.tar.zst 2>/dev/null || echo 'No templates'"
            result, _ = self.lxc_service.execute(template_cmd)
            if result:
                logger.info(result)
            else:
                logger.info("  No templates found")
        finally:
            if self.lxc_service:
                self.lxc_service.disconnect()

