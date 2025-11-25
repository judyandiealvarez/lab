"""
Configure HAProxy systemd override action
"""
import logging
from cli import FileOps
from .base import Action
logger = logging.getLogger(__name__)

class ConfigureHaproxySystemdAction(Action):
    """Action to configure systemd override for HAProxy"""
    description = "haproxy systemd override"

    def execute(self) -> bool:
        """Configure systemd override for HAProxy"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        mkdir_cmd = FileOps().mkdir("/etc/systemd/system/haproxy.service.d", parents=True)
        output, exit_code = self.ssh_service.execute(mkdir_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("create haproxy systemd override directory failed with exit code %s", exit_code)
            if output:
                logger.error("create haproxy systemd override directory output: %s", output.splitlines()[-1])
            return False
        override_content = (
            "[Service]\n"
            "Type=notify\n"
            "PrivateNetwork=no\n"
            "ProtectSystem=no\n"
            "ProtectHome=no\n"
            "ExecStart=\n"
            "ExecStart=/usr/sbin/haproxy -Ws -f $CONFIG -p $PIDFILE $EXTRAOPTS\n"
        )
        override_cmd = FileOps().write("/etc/systemd/system/haproxy.service.d/override.conf", override_content)
        output, exit_code = self.ssh_service.execute(override_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("write haproxy systemd override failed with exit code %s", exit_code)
            if output:
                logger.error("write haproxy systemd override output: %s", output.splitlines()[-1])
            return False
        reload_cmd = "systemctl daemon-reload"
        output, exit_code = self.ssh_service.execute(reload_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("reload systemd daemon failed with exit code %s", exit_code)
            if output:
                logger.error("reload systemd daemon output: %s", output.splitlines()[-1])
            return False
        return True

