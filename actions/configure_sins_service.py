"""
Configure SiNS DNS service action
"""
import logging
import base64
from cli import FileOps, SystemCtl
from .base import Action
logger = logging.getLogger(__name__)

class ConfigureSinsServiceAction(Action):
    """Action to configure SiNS DNS systemd service"""
    description = "sins dns service configuration"

    def execute(self) -> bool:
        """Configure SiNS DNS systemd service"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        # Create systemd service file
        logger.info("Creating SiNS systemd service...")
        service_content = """[Unit]
Description=SiNS DNS Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/sins/app
ExecStart=/usr/bin/dotnet /opt/sins/app/sins.dll
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
"""
        service_b64 = base64.b64encode(service_content.encode()).decode()
        service_cmd = (
            f"systemctl stop sins 2>/dev/null || true; "
            f"echo {service_b64} | base64 -d > /etc/systemd/system/sins.service && "
            f"systemctl daemon-reload"
        )
        output, exit_code = self.ssh_service.execute(service_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to create SiNS service file: %s", output)
            return False
        return True

