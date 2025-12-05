"""
Configure PostgreSQL files action
"""
import logging
from cli import FileOps, Sed
from .base import Action
logger = logging.getLogger(__name__)

class ConfigurePostgresFilesAction(Action):
    """Action to configure PostgreSQL configuration files"""
    description = "postgresql files configuration"

    def execute(self) -> bool:
        """Configure PostgreSQL configuration files"""
        if not self.ssh_service:
            logger.error("SSH service not initialized")
            return False
        # Get params
        version = "17"
        port = 5432
        # Use network from config (environment-specific) if available, otherwise fallback to container params or default
        allow_cidr = "10.11.3.0/24"
        if self.cfg and hasattr(self.cfg, "network") and self.cfg.network:
            allow_cidr = self.cfg.network
        if self.container_cfg:
            params = self.container_cfg.params or {}
            version = str(params.get("version", "17"))
            port = params.get("port", 5432)
            # Only use container param cidr if config network is not available
            if not (self.cfg and hasattr(self.cfg, "network") and self.cfg.network):
                allow_cidr = params.get("cidr", allow_cidr)
        config_path = f"/etc/postgresql/{version}/main/postgresql.conf"
        # Remove all existing listen_addresses lines
        remove_cmd = f"sed -i '/^#*listen_addresses.*/d' {config_path}"
        output, exit_code = self.ssh_service.execute(remove_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("remove listen_addresses failed with exit code %s", exit_code)
            return False
        # Check if listen_addresses already exists (shouldn't after removal, but check anyway)
        check_cmd = f"grep -q '^listen_addresses =' {config_path} && echo exists || echo not_exists"
        check_output, _ = self.ssh_service.execute(check_cmd, sudo=True)
        if "not_exists" in check_output:
            # Find the line number of CONNECTIONS section
            line_num_cmd = f"grep -n '^# CONNECTIONS AND AUTHENTICATION' {config_path} | cut -d: -f1"
            line_num_output, _ = self.ssh_service.execute(line_num_cmd, sudo=True)
            if line_num_output and line_num_output.strip().isdigit():
                line_num = int(line_num_output.strip())
                # Use awk to insert the line after the specified line number
                insert_script = f"""awk -v n={line_num} -v s="listen_addresses = '*'" 'NR==n {{print; print s; next}}1' {config_path} > {config_path}.tmp && mv {config_path}.tmp {config_path}
"""
            else:
                # Fallback: append at end of file
                insert_script = f"""echo "listen_addresses = '*'" >> {config_path}
"""
            output, exit_code = self.ssh_service.execute(insert_script, sudo=True)
            if exit_code is not None and exit_code != 0:
                logger.error("insert listen_addresses failed with exit code %s", exit_code)
                if output:
                    logger.error("insert listen_addresses output: %s", output)
                return False
        port_cmd = Sed().flags("").replace(config_path, "^#?port.*", f"port = {port}")
        # Add both local and network entries to pg_hba.conf
        pg_hba_content = f"local all all peer\nhost all all {allow_cidr} md5\n"
        pg_hba_cmd = FileOps().write(f"/etc/postgresql/{version}/main/pg_hba.conf", pg_hba_content)
        results = []
        for cmd, desc in [
            (port_cmd, "configure postgres port"),
            (pg_hba_cmd, "write pg_hba rule"),
        ]:
            output, exit_code = self.ssh_service.execute(cmd, sudo=True)
            if exit_code is not None and exit_code != 0:
                logger.error("%s failed with exit code %s", desc, exit_code)
                if output:
                    logger.error("%s output: %s", desc, output.splitlines()[-1])
            results.append(exit_code is None or exit_code == 0)
        # Restart PostgreSQL to apply listen_addresses change
        if all(results):
            logger.info("Restarting PostgreSQL to apply configuration changes...")
            from cli import SystemCtl
            import time
            cluster_service = f"postgresql@{version}-main"
            restart_cmd = SystemCtl().service(cluster_service).restart()
            output, exit_code = self.ssh_service.execute(restart_cmd, sudo=True)
            if exit_code is not None and exit_code != 0:
                logger.error("restart postgresql failed with exit code %s", exit_code)
                return False
            time.sleep(3)
            # Verify it's listening on all interfaces
            port_check_cmd = "ss -tlnp | grep :5432 | grep -v '127.0.0.1' || echo 'not_listening'"
            port_output, _ = self.ssh_service.execute(port_check_cmd, sudo=True)
            if "not_listening" not in port_output and ":5432" in port_output:
                logger.info("PostgreSQL is listening on all interfaces after restart")
            else:
                logger.warning("PostgreSQL may not be listening on external interfaces after restart")
        return all(results)

