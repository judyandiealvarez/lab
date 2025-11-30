"""
Enable SiNS DNS service action
"""
import logging
from cli import SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class EnableSinsServiceAction(Action):
    """Action to enable and start SiNS DNS service"""
    description = "sins dns service enablement"

    def execute(self) -> bool:
        """Enable and start SiNS DNS service"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        # Enable and start service
        logger.info("Enabling and starting SiNS service...")
        start_cmd = SystemCtl().service("sins").enable_and_start()
        output, exit_code = self.ssh_service.execute(start_cmd, sudo=True)
        # Wait a moment for service to start (Type=simple doesn't notify immediately)
        import time
        time.sleep(3)
        if exit_code is not None and exit_code != 0:
            # Get service status to check if it's restarting
            status_cmd = SystemCtl().service("sins").status()
            status_output, _ = self.ssh_service.execute(status_cmd, sudo=True)
            if "activating (auto-restart)" in status_output or "auto-restart" in status_output:
                # Check if the error is PostgreSQL connection failure
                journal_cmd = "journalctl -u sins.service -n 10 --no-pager | grep -i 'postgres\\|connection refused' || true"
                journal_output, _ = self.ssh_service.execute(journal_cmd, sudo=True)
                if "postgres" in journal_output.lower() or "connection refused" in journal_output.lower():
                    logger.warning("SiNS service is restarting due to PostgreSQL connection failure. This is expected if PostgreSQL is not yet available. Service will retry automatically.")
                    return True
                # Check if the error is port 53 already in use
                journal_cmd = "journalctl -u sins.service -n 10 --no-pager | grep -i 'address already in use\\|port.*53' || true"
                journal_output, _ = self.ssh_service.execute(journal_cmd, sudo=True)
                if "address already in use" in journal_output.lower() or "port" in journal_output.lower():
                    logger.error("SiNS service cannot bind to port 53 - port is already in use. Ensure systemd-resolved is disabled.")
                    return False
            logger.error("Failed to start SiNS service: %s", output)
            logger.error("Service status:\n%s", status_output)
            # Get detailed error from journal
            journal_cmd = "journalctl -u sins.service -n 50 --no-pager"
            journal_output, _ = self.ssh_service.execute(journal_cmd, sudo=True)
            logger.error("Service journal logs:\n%s", journal_output)
            return False
        # Verify service is active
        status_cmd = SystemCtl().service("sins").is_active()
        status, exit_code = self.ssh_service.execute(status_cmd, sudo=True)
        if exit_code == 0 and SystemCtl.parse_is_active(status):
            # Verify it's actually listening on port 53
            port_check_cmd = "ss -tulnp | grep -E ':53.*sins|:53.*dotnet' || echo 'not_listening'"
            port_output, _ = self.ssh_service.execute(port_check_cmd, sudo=True)
            if "not_listening" not in port_output and ":53" in port_output:
                logger.info("SiNS DNS server is running and listening on port 53")
                return True
            else:
                logger.warning("SiNS service is active but not listening on port 53")
        # Check if service is in restarting state (acceptable if PostgreSQL isn't available yet)
        status_cmd = SystemCtl().service("sins").status()
        status_output, _ = self.ssh_service.execute(status_cmd, sudo=True)
        if "activating (auto-restart)" in status_output or "auto-restart" in status_output:
            # Check if the error is PostgreSQL connection failure
            journal_cmd = "journalctl -u sins.service -n 10 --no-pager | grep -i 'postgres\|connection refused' || true"
            journal_output, _ = self.ssh_service.execute(journal_cmd, sudo=True)
            if "postgres" in journal_output.lower() or "connection refused" in journal_output.lower():
                logger.warning("SiNS service is restarting due to PostgreSQL connection failure. This is expected if PostgreSQL is not yet available. Service will retry automatically.")
                return True
            # Check if the error is port 53 already in use
            journal_cmd = "journalctl -u sins.service -n 10 --no-pager | grep -i 'address already in use\|port.*53' || true"
            journal_output, _ = self.ssh_service.execute(journal_cmd, sudo=True)
            if "address already in use" in journal_output.lower() or "port" in journal_output.lower():
                logger.error("SiNS service cannot bind to port 53 - port is already in use. Ensure systemd-resolved is disabled.")
                return False
        # Check if process is running even if systemd says inactive
        # Check for both sins.dll (.NET) and sins (native binary)
        process_check_cmd = "pgrep -f 'sins\\.dll|^sins ' >/dev/null && echo running || echo not_running"
        process_output, _ = self.ssh_service.execute(process_check_cmd, sudo=True)
        if "running" in process_output:
            logger.info("SiNS process is running despite inactive systemd status")
            return True
        logger.error("SiNS DNS server is not running")
        return False

