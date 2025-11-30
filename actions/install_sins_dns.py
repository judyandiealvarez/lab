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
        """Install SiNS DNS server from Debian package"""
        if not self.ssh_service or not self.apt_service:
            logger.error("SSH service or APT service not initialized")
            return False
        # Add Gemfury APT repository manually (matching Dockerfile format)
        logger.info("Adding Gemfury APT repository...")
        # Add repository source with [trusted=yes] flag (no GPG key needed)
        repo_source = "deb [trusted=yes] https://judyalvarez@apt.fury.io/judyalvarez /"
        add_source_cmd = f"echo '{repo_source}' | tee /etc/apt/sources.list.d/fury.list"
        output, exit_code = self.ssh_service.execute(add_source_cmd, sudo=True, timeout=30)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to add Gemfury repository source: %s", output[-200:] if output else "No output")
            return False
        # Update apt and install sins from repository
        logger.info("Installing SiNS DNS server from APT repository...")
        install_cmd = "apt-get update && apt-get install -y sins"
        output, exit_code = self.ssh_service.execute(install_cmd, sudo=True, timeout=300)
        if exit_code is not None and exit_code != 0:
            logger.error("Failed to install sins package: %s", output[-200:] if output else "No output")
            return False
        # Verify installation
        verify_cmd = "command -v sins >/dev/null && echo installed || echo not_installed"
        verify_output, verify_exit_code = self.ssh_service.execute(verify_cmd, sudo=True)
        if verify_exit_code != 0 or "installed" not in verify_output:
            logger.error("SiNS package was not installed correctly")
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
        # Create PostgreSQL database if it doesn't exist
        logger.info("Ensuring PostgreSQL database '%s' exists...", postgres_db)
        # Install postgresql-client if not already installed (needed for psql)
        install_pg_client_cmd = "command -v psql >/dev/null || apt-get install -y postgresql-client"
        self.ssh_service.execute(install_pg_client_cmd, sudo=True, timeout=60)
        # Create database (ignore error if it already exists)
        create_db_cmd = f"PGPASSWORD={postgres_password} psql -h {postgres_host} -p {postgres_port} -U {postgres_user} -d postgres -tc \"SELECT 1 FROM pg_database WHERE datname = '{postgres_db}'\" | grep -q 1 || PGPASSWORD={postgres_password} psql -h {postgres_host} -p {postgres_port} -U {postgres_user} -d postgres -c \"CREATE DATABASE {postgres_db};\""
        output, exit_code = self.ssh_service.execute(create_db_cmd, sudo=True, timeout=30)
        if exit_code == 0:
            logger.info("PostgreSQL database '%s' is ready", postgres_db)
        else:
            logger.warning("Could not verify/create PostgreSQL database (may already exist): %s", output[-100:] if output else "No output")
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
        # Determine appsettings location (check common locations for Debian package)
        # Try /etc/sins/ first, then /opt/sins/app/, then /usr/lib/sins/
        logger.info("Configuring SiNS appsettings.json...")
        # Check where the package installed the application
        find_app_cmd = "find /usr /opt /etc -name 'sins.dll' -o -name 'sins' -type f 2>/dev/null | head -1"
        app_location, _ = self.ssh_service.execute(find_app_cmd, sudo=True)
        # Write to all possible config locations to ensure correct config is used
        # .NET apps check /etc first, then working directory, so we write to all to overwrite any wrong configs
        config_locations = [
            "/etc/sins/appsettings.json",  # Primary location - .NET apps check /etc first
            "/opt/sins/appsettings.json",  # Service WorkingDirectory is /opt/sins
            "/opt/sins/app/appsettings.json",
        ]
        logger.info("Writing SiNS appsettings.json to all config locations...")
        config_written = False
        for config_path in config_locations:
            # Create directory if needed
            config_dir = "/".join(config_path.split("/")[:-1])
            mkdir_cmd = f"mkdir -p {config_dir}"
            self.ssh_service.execute(mkdir_cmd, sudo=True)
            # Write appsettings (overwrite any existing file)
            appsettings_cmd = f"echo {appsettings_b64} | base64 -d > {config_path}"
            output, exit_code = self.ssh_service.execute(appsettings_cmd, sudo=True)
            if exit_code == 0:
                logger.info("SiNS appsettings.json written to %s", config_path)
                config_written = True
            else:
                logger.warning("Failed to write appsettings.json to %s: %s", config_path, output[-100:] if output else "No output")
        if not config_written:
            logger.error("Failed to write appsettings.json to any location")
            return False
        return True

