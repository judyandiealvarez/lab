"""
Configure apt cache proxy action
"""
from .base import Action
from cli import FileOps

class ConfigureAptProxyAction(Action):
    """Configure apt cache proxy"""
    description = "apt cache proxy configuration"

    def execute(self) -> bool:
        """Configure apt cache proxy"""
        # Get apt-cache IP from containers (find by name)
        apt_cache_container = next((c for c in self.cfg.containers if c.name == self.cfg.apt_cache_ct), None)
        if not apt_cache_container:
            return True  # No apt-cache, skip
        apt_cache_ip = apt_cache_container.ip_address
        apt_cache_port = self.cfg.apt_cache_port
        proxy_content = f'Acquire::http::Proxy "http://{apt_cache_ip}:{apt_cache_port}";\n'
        proxy_cmd = FileOps().write("/etc/apt/apt.conf.d/01proxy", proxy_content)
        output, exit_code = self.pct_service.execute(self.container_id, proxy_cmd)
        if exit_code is not None and exit_code != 0:
            self.logger.error("Failed to configure apt cache proxy: %s", output)
            return False
        return True

