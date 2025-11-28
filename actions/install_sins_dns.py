"""
Install SiNS DNS server action
"""
import logging
import base64
import json
from cli import FileOps, SystemCtl
from cli.apt import Apt
from .base import Action
logger = logging.getLogger(__name__)

class InstallSinsDnsAction(Action):
    """Action to install SiNS DNS server"""
    description = "sins dns installation"

    def execute(self) -> bool:
        """Install SiNS DNS server"""
        if not self.ssh_service or not self.apt_service:
            logger.error("SSH service or APT service not initialized")
            return False
        # Install git if not available
        logger.info("Ensuring git is installed...")
        git_check_cmd = "command -v git >/dev/null && echo exists || echo missing"
        git_check_output, _ = self.ssh_service.execute(git_check_cmd, sudo=True)
        if "missing" in git_check_output:
            install_git_cmd = Apt().install(["git"])
            output = self.apt_service.execute(install_git_cmd)
            if output is None:
                logger.error("Failed to install git")
                return False
        # Clone SiNS repository
        logger.info("Cloning SiNS repository...")
        clone_cmd = "cd /opt && rm -rf sins && git clone https://github.com/judyandiealvarez/SiNS.git sins"
        output, exit_code = self.ssh_service.execute(clone_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to clone SiNS repository: %s", output)
            return False
        # Verify repository was cloned
        verify_cmd = "test -d /opt/sins && echo exists || echo missing"
        verify_output, verify_exit_code = self.ssh_service.execute(verify_cmd, sudo=True)
        if verify_exit_code != 0 or "exists" not in verify_output:
            logger.error("SiNS repository was not cloned")
            return False
        # Get PostgreSQL connection info from container params
        params = self.container_cfg.params if hasattr(self.container_cfg, "params") else {}
        postgres_host = params.get("postgres_host", "10.11.3.18")
        postgres_port = params.get("postgres_port", 5432)
        postgres_db = params.get("postgres_db", "dns_server")
        postgres_user = params.get("postgres_user", "postgres")
        postgres_password = params.get("postgres_password", "postgres")
        dns_port = params.get("dns_port", 53)
        web_port = params.get("web_port", 80)
        # Create appsettings.json
        logger.info("Configuring SiNS application settings...")
        # Generate a secure 256-bit (32 bytes) JWT secret key
        import secrets
        jwt_secret = secrets.token_urlsafe(32)  # 32 bytes = 256 bits
        appsettings = {
            "ConnectionStrings": {
                "DefaultConnection": f"Host={postgres_host};Port={postgres_port};Database={postgres_db};Username={postgres_user};Password={postgres_password}"
            },
            "DnsSettings": {
                "Port": dns_port
            },
            "WebSettings": {
                "Port": web_port
            },
            "Jwt": {
                "Key": jwt_secret,
                "Issuer": "SiNS-DNS-Server",
                "Audience": "SiNS-DNS-Client",
                "ExpirationMinutes": 1440
            }
        }
        appsettings_json = json.dumps(appsettings, indent=2)
        appsettings_b64 = base64.b64encode(appsettings_json.encode()).decode()
        appsettings_cmd = f"echo {appsettings_b64} | base64 -d > /opt/sins/sins/appsettings.Production.json"
        output, exit_code = self.ssh_service.execute(appsettings_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to create appsettings.Production.json: %s", output)
            return False
        # Build SiNS application
        logger.info("Building SiNS application...")
        build_cmd = "cd /opt/sins/sins && dotnet publish -c Release -o /opt/sins/app"
        output, exit_code = self.ssh_service.execute(build_cmd, sudo=True, timeout=600)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to build SiNS application: %s", output[-500:] if output else "No output")
            return False
        # Verify build
        verify_build_cmd = "test -f /opt/sins/app/sins.dll && echo exists || echo missing"
        verify_build_output, verify_build_exit_code = self.ssh_service.execute(verify_build_cmd, sudo=True)
        if verify_build_exit_code != 0 or "exists" not in verify_build_output:
            logger.error("SiNS application was not built - sins.dll not found")
            return False
        # Copy appsettings to app directory
        appsettings2_cmd = f"echo {appsettings_b64} | base64 -d > /opt/sins/app/appsettings.json"
        output, exit_code = self.ssh_service.execute(appsettings2_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to create appsettings.json: %s", output)
            return False
        return True

