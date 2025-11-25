"""
Configure HAProxy action
"""
import logging
import textwrap
from cli import FileOps
from .base import Action
logger = logging.getLogger(__name__)

class ConfigureHaproxyAction(Action):
    """Action to configure HAProxy"""
    description = "haproxy configuration"

    def execute(self) -> bool:
        """Configure HAProxy"""
        if not self.ssh_service or not self.cfg:
            logger.error("SSH service or config not initialized")
            return False
        params = self.cfg.containers_dict.get(self.container_id, {}).get("params", {}) if hasattr(self.cfg, "containers_dict") else {}
        # Try to get from container config if available
        if hasattr(self, "_container_cfg") and self._container_cfg:
            params = self._container_cfg.params or {}
        http_port = params.get("http_port", 80)
        https_port = params.get("https_port", 443)
        stats_port = params.get("stats_port", 8404)
        # Build backend servers from swarm config
        backend_servers = []
        if hasattr(self.cfg, "swarm_managers") and hasattr(self.cfg, "swarm_workers"):
            swarm_nodes = (self.cfg.swarm_managers or []) + (self.cfg.swarm_workers or [])
            for index, node in enumerate(swarm_nodes, start=1):
                backend_servers.append(f"    server node{index} {node.ip_address}:80 check")
        servers_text = "\n".join(backend_servers) if backend_servers else "    server dummy 127.0.0.1:80 check"
        config_text = textwrap.dedent(f"""
        global
            log /dev/log local0
            log /dev/log local1 notice
            maxconn 2048
            daemon
        defaults
            log     global
            mode    http
            option  httplog
            option  dontlognull
            timeout connect 5s
            timeout client  50s
            timeout server  50s
        frontend http-in
            bind *:{http_port}
            default_backend nodes
        frontend https-in
            bind *:{https_port}
            mode http
            default_backend nodes
        backend nodes
{servers_text}
        listen stats
            bind *:{stats_port}
            mode http
            stats enable
            stats uri /
            stats refresh 10s
        """).strip() + "\n"
        write_cmd = FileOps().write("/etc/haproxy/haproxy.cfg", config_text)
        output, exit_code = self.ssh_service.execute(write_cmd, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("write haproxy configuration failed with exit code %s", exit_code)
            if output:
                logger.error("write haproxy configuration output: %s", output.splitlines()[-1])
            return False
        return True

