"""
Wait for apt-cache service to be ready action
"""
import logging
import time
from services.lxc import LXCService
from services.pct import PCTService
from .base import Action
logger = logging.getLogger(__name__)

class WaitAptCacheReadyAction(Action):
    """Action to wait for apt-cache service to be ready"""
    description = "wait apt cache ready"

    def execute(self) -> bool:
        """Wait for apt-cache service to be ready"""
        if not self.cfg or not self.container_cfg:
            logger.error("Configuration not initialized")
            return False
        
        logger.info("Verifying apt-cache service is ready...")
        max_attempts = 20
        proxmox_host = self.cfg.proxmox_host
        container_id = str(self.container_cfg.id)
        apt_cache_port = self.cfg.apt_cache_port
        
        lxc_service = LXCService(proxmox_host, self.cfg.ssh)
        if not lxc_service.connect():
            logger.error("Failed to connect to Proxmox host for apt-cache verification")
            return False
        
        try:
            pct_service = PCTService(lxc_service)
            for attempt in range(1, max_attempts + 1):
                service_check, _ = pct_service.execute(
                    container_id,
                    "systemctl is-active apt-cacher-ng 2>/dev/null || echo 'inactive'",
                    timeout=10,
                )
                if service_check and "active" in service_check:
                    port_check_cmd = (
                        f"nc -z localhost {apt_cache_port} 2>/dev/null "
                        "&& echo 'port_open' || echo 'port_closed'"
                    )
                    port_check, _ = pct_service.execute(
                        container_id,
                        port_check_cmd,
                        timeout=10,
                    )
                    if port_check and "port_open" in port_check:
                        # Test if apt-cacher-ng can actually fetch from upstream
                        test_cmd = (
                            f"timeout 10 wget -qO- 'http://127.0.0.1:{apt_cache_port}/acng-report.html' 2>&1 | "
                            "grep -q 'Apt-Cacher NG' && echo 'working' || echo 'not_working'"
                        )
                        functionality_test, _ = pct_service.execute(
                            container_id,
                            test_cmd,
                            timeout=15,
                        )
                        if functionality_test and "working" in functionality_test:
                            logger.info("apt-cache service is ready on %s:%s", self.container_cfg.ip_address, apt_cache_port)
                            return True
                        elif attempt < max_attempts:
                            logger.debug("apt-cache service not fully ready yet (attempt %s/%s), waiting...", attempt, max_attempts)
                            time.sleep(2)
                            continue
                else:
                    # Service is not active - try to start it and check logs
                    if attempt == 1:
                        # On first attempt, try to start the service
                        start_cmd = "systemctl start apt-cacher-ng 2>&1"
                        start_output, _ = pct_service.execute(
                            container_id,
                            start_cmd,
                            timeout=10,
                        )
                        if start_output:
                            logger.info("Service start attempt output: %s", start_output)
                        # Check service status for errors
                        status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1 | head -15"
                        status_output, _ = pct_service.execute(
                            container_id,
                            status_cmd,
                            timeout=10,
                        )
                        if status_output:
                            logger.warning("Service status: %s", status_output)
                if attempt < max_attempts:
                    logger.info("Waiting for apt-cache service... (%s/%s)", attempt, max_attempts)
                    time.sleep(3)
                else:
                    # Get final status and logs before failing
                    status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1"
                    status_output, _ = pct_service.execute(
                        container_id,
                        status_cmd,
                        timeout=10,
                    )
                    journal_cmd = "journalctl -u apt-cacher-ng --no-pager -n 30 2>&1"
                    journal_output, _ = pct_service.execute(
                        container_id,
                        journal_cmd,
                        timeout=10,
                    )
                    error_msg = "apt-cache service did not become ready in time"
                    if status_output:
                        error_msg += f"\nService status: {status_output}"
                    if journal_output:
                        error_msg += f"\nService logs: {journal_output}"
                    logger.error(error_msg)
                    return False
        finally:
            lxc_service.disconnect()
        
        return False

