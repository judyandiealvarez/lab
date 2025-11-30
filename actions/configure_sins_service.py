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
        # Get web port from container params
        web_port = 80
        if hasattr(self, "container_cfg") and self.container_cfg:
            params = self.container_cfg.params or {}
            web_port = params.get("web_port", 80)
        
        # Check if service file already exists (provided by Debian package)
        check_service_cmd = "test -f /etc/systemd/system/sins.service && echo exists || echo missing"
        service_exists, _ = self.ssh_service.execute(check_service_cmd, sudo=True)
        if "exists" in service_exists:
            logger.info("SiNS service file already exists (provided by Debian package), updating web port configuration and timeout...")
            # Read existing service file
            read_service_cmd = "cat /etc/systemd/system/sins.service"
            existing_service, _ = self.ssh_service.execute(read_service_cmd, sudo=True)
            # Update ASPNETCORE_URLS to use the configured web port
            # Replace existing ASPNETCORE_URLS line or add it if missing
            needs_update = False
            if f"ASPNETCORE_URLS=http://+:{web_port}" not in existing_service and f"ASPNETCORE_URLS=http://0.0.0.0:{web_port}" not in existing_service:
                needs_update = True
            # Change Type=notify to Type=simple (app doesn't send systemd notifications)
            if "Type=notify" in existing_service:
                needs_update = True
            # Add or update TimeoutStartSec to prevent systemd timeout
            if "TimeoutStartSec" not in existing_service:
                needs_update = True
            if not needs_update:
                logger.info("SiNS service file already configured with correct web port and timeout")
                reload_cmd = "systemctl daemon-reload"
                self.ssh_service.execute(reload_cmd, sudo=True)
                return True
            # Update the service file: change Type=notify to Type=simple (app doesn't send systemd notifications)
            # Also update ASPNETCORE_URLS and TimeoutStartSec
            update_cmd = (
                f"sed -i 's|^Type=notify|Type=simple|' /etc/systemd/system/sins.service && "
                f"sed -i 's|Environment=ASPNETCORE_URLS=.*|Environment=ASPNETCORE_URLS=http://0.0.0.0:{web_port}|' /etc/systemd/system/sins.service && "
                f"grep -q '^TimeoutStartSec=' /etc/systemd/system/sins.service || sed -i '/^\\[Service\\]/a TimeoutStartSec=300' /etc/systemd/system/sins.service && "
                f"sed -i 's|^TimeoutStartSec=.*|TimeoutStartSec=300|' /etc/systemd/system/sins.service"
            )
            output, exit_code = self.ssh_service.execute(update_cmd, sudo=True)
            if exit_code == 0:
                logger.info("Updated SiNS service file: changed Type=notify to Type=simple, ASPNETCORE_URLS=http://0.0.0.0:%s, TimeoutStartSec=300", web_port)
                reload_cmd = "systemctl daemon-reload"
                self.ssh_service.execute(reload_cmd, sudo=True)
                return True
            else:
                logger.warning("Failed to update service file, will create new one")
        # Create systemd service file if it doesn't exist
        logger.info("Creating SiNS systemd service...")
        # Get web port from container params
        web_port = 80
        if hasattr(self, "container_cfg") and self.container_cfg:
            params = self.container_cfg.params or {}
            web_port = params.get("web_port", 80)
        # Find where sins binary or DLL is installed
        # Check common locations from Debian package
        find_binary_cmd = "test -f /opt/sins/sins && echo /opt/sins/sins || (which sins 2>/dev/null || find /usr /opt -name 'sins.dll' -o -name 'sins' -type f 2>/dev/null | head -1)"
        binary_path, _ = self.ssh_service.execute(find_binary_cmd, sudo=True)
        if not binary_path or not binary_path.strip():
            logger.error("Could not find SiNS binary")
            return False
        binary_path = binary_path.strip()
        # Determine working directory and exec command
        if binary_path.endswith(".dll"):
            # .NET application
            working_dir = "/".join(binary_path.split("/")[:-1])
            exec_start = f"/usr/bin/dotnet {binary_path}"
        elif binary_path == "/opt/sins/sins":
            # Debian package native binary
            working_dir = "/opt/sins"
            exec_start = "/opt/sins/sins"
        else:
            # Other native binary
            working_dir = "/".join(binary_path.split("/")[:-1]) if "/" in binary_path else "/usr/bin"
            exec_start = binary_path
        # Determine appsettings location
        appsettings_locations = [
            "/etc/sins/appsettings.json",
            f"{working_dir}/appsettings.json",
            "/opt/sins/app/appsettings.json",
        ]
        appsettings_path = None
        for loc in appsettings_locations:
            check_cmd = f"test -f {loc} && echo exists || echo missing"
            check_output, _ = self.ssh_service.execute(check_cmd, sudo=True)
            if "exists" in check_output:
                appsettings_path = loc
                break
        if not appsettings_path:
            appsettings_path = "/etc/sins/appsettings.json"
        service_content = f"""[Unit]
Description=SiNS DNS Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={working_dir}
Environment=ASPNETCORE_URLS=http://0.0.0.0:{web_port}
Environment=ASPNETCORE_ENVIRONMENT=Production
ExecStart={exec_start}
Restart=always
RestartSec=10
TimeoutStartSec=300

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

