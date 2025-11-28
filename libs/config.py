"""
Configuration data model - class-based representation of lab.yaml
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
@dataclass

class ContainerResources:
    """Container resource allocation"""
    memory: int
    swap: int
    cores: int
    rootfs_size: int
@dataclass

class ContainerConfig:  # pylint: disable=too-many-instance-attributes
    """Container configuration"""
    name: str
    id: int
    ip: int  # Last octet only
    hostname: str
    template: Optional[str] = None
    resources: Optional[ContainerResources] = None
    params: Dict[str, Any] = field(default_factory=dict)
    actions: List[str] = field(default_factory=list)
    ip_address: Optional[str] = None  # Full IP, computed later
    privileged: Optional[bool] = None
    nested: Optional[bool] = None
@dataclass

class TemplateConfig:  # pylint: disable=too-many-instance-attributes
    """Template configuration"""
    name: str
    id: int
    ip: int  # Last octet only
    hostname: str
    template: Optional[str] = None  # "base" or name of another template
    resources: Optional[ContainerResources] = None
    ip_address: Optional[str] = None  # Full IP, computed later
    actions: Optional[List[str]] = None
    privileged: Optional[bool] = None
    nested: Optional[bool] = None
@dataclass

class SwarmConfig:
    """Docker Swarm configuration"""
    managers: List[int] = field(default_factory=list)
    workers: List[int] = field(default_factory=list)

@dataclass
class KubernetesConfig:
    """Kubernetes (k3s) configuration"""
    control: List[int] = field(default_factory=list)
    workers: List[int] = field(default_factory=list)
@dataclass

class ProxmoxConfig:
    """Proxmox configuration"""
    host: str
    storage: str
    bridge: str
    template_dir: str
    gateway_octet: int
@dataclass

class ServiceConfig:
    """Service configuration"""
    port: Optional[int] = None
    image: Optional[str] = None
    http_port: Optional[int] = None
    https_port: Optional[int] = None
    stats_port: Optional[int] = None
    password: Optional[str] = None
@dataclass

class ServicesConfig:
    """All services configuration"""
    apt_cache: ServiceConfig
    docker_swarm: ServiceConfig
    portainer: ServiceConfig
    postgresql: Optional[ServiceConfig] = None
    haproxy: Optional[ServiceConfig] = None
    rancher: Optional[ServiceConfig] = None
    longhorn: Optional[ServiceConfig] = None
@dataclass

@dataclass
class UserConfig:
    """Individual user configuration"""
    name: str
    password: Optional[str] = None
    sudo_group: str = "sudo"

@dataclass
class UsersConfig:
    """Users configuration - list of users"""
    users: List[UserConfig]
    
    @property
    def default_user(self) -> str:
        """Get the first user's name (for backward compatibility)"""
        return self.users[0].name if self.users else "root"
    
    @property
    def sudo_group(self) -> str:
        """Get the first user's sudo group (for backward compatibility)"""
        return self.users[0].sudo_group if self.users else "sudo"

@dataclass
class DNSConfig:
    """DNS configuration"""
    servers: List[str]
@dataclass

class DockerConfig:
    """Docker configuration"""
    version: str
    repository: str
    release: str
    ubuntu_release: str
@dataclass

class TemplatePatternsConfig:
    """Template patterns configuration"""
    base: List[str]
    patterns: Dict[str, str]
    preserve: List[str]
@dataclass
@dataclass

class SSHConfig:
    """SSH configuration"""
    connect_timeout: int
    batch_mode: bool
    default_exec_timeout: int = 300
    read_buffer_size: int = 4096
    poll_interval: float = 0.05
    default_username: str = "root"
    look_for_keys: bool = True
    allow_agent: bool = True
    verbose: bool = False
@dataclass

class WaitsConfig:  # pylint: disable=too-many-instance-attributes
    """Wait/retry configuration"""
    container_startup: int
    container_ready_max_attempts: int
    container_ready_sleep: int
    network_config: int
    service_start: int
    swarm_init: int
    portainer_start: int
    glusterfs_setup: int
@dataclass

class GlusterFSConfig:
    """GlusterFS configuration"""
    volume_name: str
    brick_path: str
    mount_point: str
    replica_count: int
@dataclass

class TimeoutsConfig:
    """Timeout configuration"""
    apt_cache: int
    ubuntu_template: int
    docker_template: int
    swarm_deploy: int
@dataclass

class LabConfig:  # pylint: disable=too-many-instance-attributes
    """Main lab configuration class"""
    network: str
    proxmox: ProxmoxConfig
    containers: List[ContainerConfig]
    templates: List[TemplateConfig]
    swarm: SwarmConfig
    services: ServicesConfig
    users: UsersConfig
    dns: DNSConfig
    docker: DockerConfig
    template_config: TemplatePatternsConfig
    ssh: SSHConfig
    waits: WaitsConfig
    timeouts: TimeoutsConfig
    glusterfs: Optional[GlusterFSConfig] = None
    kubernetes: Optional[KubernetesConfig] = None
    apt_cache_ct: str = "apt-cache"
    # Computed fields
    network_base: Optional[str] = None
    gateway: Optional[str] = None
    swarm_managers: List[ContainerConfig] = field(default_factory=list)
    swarm_workers: List[ContainerConfig] = field(default_factory=list)
    kubernetes_control: List[ContainerConfig] = field(default_factory=list)
    kubernetes_workers: List[ContainerConfig] = field(default_factory=list)
    @classmethod

    def from_dict(cls, data: Dict[str, Any], verbose: bool = False) -> "LabConfig":  # pylint: disable=too-many-locals
        """Create LabConfig from dictionary (loaded from YAML)"""
        # Helper to create ContainerResources from dict
        def make_resources(res_dict: Optional[Dict]) -> Optional[ContainerResources]:
            if not res_dict:
                return None
            return ContainerResources(
                memory=res_dict["memory"],
                swap=res_dict["swap"],
                cores=res_dict["cores"],
                rootfs_size=res_dict["rootfs_size"],
            )
        # Parse containers
        containers = []
        for ct in data.get("ct", []):
            containers.append(
                ContainerConfig(
                    name=ct["name"],
                    id=ct["id"],
                    ip=ct["ip"],
                    hostname=ct["hostname"],
                    template=ct.get("template"),
                    resources=make_resources(ct.get("resources")),
                    params=ct.get("params", {}),
                    actions=ct.get("actions", []),
                    privileged=ct.get("privileged"),
                    nested=ct.get("nested"),
                )
            )
        # Parse templates
        templates = []
        for tmpl in data.get("templates", []):
            templates.append(
                TemplateConfig(
                    name=tmpl["name"],
                    id=tmpl["id"],
                    ip=tmpl["ip"],
                    hostname=tmpl["hostname"],
                    template=tmpl.get("template"),
                    resources=make_resources(tmpl.get("resources")),
                    actions=tmpl.get("actions", []),
                    privileged=tmpl.get("privileged"),
                    nested=tmpl.get("nested"),
                )
            )
        # Parse swarm
        swarm_data = data.get("swarm", {})
        swarm = SwarmConfig(
            managers=[m["id"] if isinstance(m, dict) else m for m in swarm_data.get("managers", [])],
            workers=[w["id"] if isinstance(w, dict) else w for w in swarm_data.get("workers", [])],
        )
        # Parse kubernetes (optional)
        kubernetes = None
        if "kubernetes" in data:
            k8s_data = data["kubernetes"]
            kubernetes = KubernetesConfig(
                control=[c["id"] if isinstance(c, dict) else c for c in k8s_data.get("control", [])],
                workers=[w["id"] if isinstance(w, dict) else w for w in k8s_data.get("workers", [])],
            )
        # Parse proxmox
        proxmox_data = data["proxmox"]
        proxmox = ProxmoxConfig(
            host=proxmox_data["host"],
            storage=proxmox_data["storage"],
            bridge=proxmox_data["bridge"],
            template_dir=proxmox_data["template_dir"],
            gateway_octet=proxmox_data["gateway_octet"],
        )
        # Parse services
        services_data = data["services"]
        services = ServicesConfig(
            apt_cache=ServiceConfig(port=services_data["apt_cache"]["port"]),
            docker_swarm=ServiceConfig(port=services_data["docker_swarm"]["port"]),
            portainer=ServiceConfig(
                port=services_data["portainer"]["port"],
                image=services_data["portainer"]["image"],
                password=services_data["portainer"].get("password"),
            ),
            postgresql=(
                ServiceConfig(port=services_data.get("postgresql", {}).get("port"))
                if "postgresql" in services_data
                else None
            ),
            haproxy=(
                ServiceConfig(
                    http_port=services_data.get("haproxy", {}).get("http_port"),
                    https_port=services_data.get("haproxy", {}).get("https_port"),
                    stats_port=services_data.get("haproxy", {}).get("stats_port"),
                )
                if "haproxy" in services_data
                else None
            ),
            rancher=(
                ServiceConfig(
                    port=services_data.get("rancher", {}).get("port"),
                    image=services_data.get("rancher", {}).get("image"),
                )
                if "rancher" in services_data
                else None
            ),
            longhorn=(
                ServiceConfig(
                    port=services_data.get("longhorn", {}).get("port"),
                )
                if "longhorn" in services_data
                else None
            ),
        )
        # Parse users
        users_data = data["users"]
        # Support both old format (dict) and new format (list)
        if isinstance(users_data, list):
            user_list = [UserConfig(
                name=user["name"],
                password=user.get("password"),
                sudo_group=user.get("sudo_group", "sudo")
            ) for user in users_data]
        else:
            # Backward compatibility: convert old format to new format
            user_list = [UserConfig(
                name=users_data["default_user"],
                password=users_data.get("password"),
                sudo_group=users_data.get("sudo_group", "sudo")
            )]
        users = UsersConfig(users=user_list)
        # Parse DNS
        dns_data = data["dns"]
        dns = DNSConfig(servers=dns_data["servers"])
        # Parse Docker
        docker_data = data["docker"]
        docker = DockerConfig(
            version=docker_data["version"],
            repository=docker_data["repository"],
            release=docker_data["release"],
            ubuntu_release=docker_data["ubuntu_release"],
        )
        # Parse template_config
        template_config_data = data.get("template_config", {})
        template_config = TemplatePatternsConfig(
            base=template_config_data.get("base", []),
            patterns=template_config_data.get("patterns", {}),
            preserve=template_config_data.get("preserve", []),
        )
        # Parse SSH
        ssh_data = data["ssh"]
        ssh = SSHConfig(
            connect_timeout=ssh_data["connect_timeout"],
            batch_mode=ssh_data["batch_mode"],
            verbose=verbose,
        )
        # Parse waits
        waits_data = data["waits"]
        waits = WaitsConfig(
            container_startup=waits_data["container_startup"],
            container_ready_max_attempts=waits_data["container_ready_max_attempts"],
            container_ready_sleep=waits_data["container_ready_sleep"],
            network_config=waits_data["network_config"],
            service_start=waits_data["service_start"],
            swarm_init=waits_data["swarm_init"],
            portainer_start=waits_data["portainer_start"],
            glusterfs_setup=waits_data["glusterfs_setup"],
        )
        # Parse timeouts
        timeouts_data = data["timeouts"]
        timeouts = TimeoutsConfig(
            apt_cache=timeouts_data["apt_cache"],
            ubuntu_template=timeouts_data["ubuntu_template"],
            docker_template=timeouts_data["docker_template"],
            swarm_deploy=timeouts_data["swarm_deploy"],
        )
        # Parse GlusterFS (optional)
        glusterfs = None
        if "glusterfs" in data:
            glusterfs_data = data["glusterfs"]
            glusterfs = GlusterFSConfig(
                volume_name=glusterfs_data.get("volume_name", "swarm-storage"),
                brick_path=glusterfs_data.get("brick_path", "/gluster/brick"),
                mount_point=glusterfs_data.get("mount_point", "/mnt/gluster"),
                replica_count=glusterfs_data.get("replica_count", 2),
            )
        return cls(
            network=data["network"],
            proxmox=proxmox,
            containers=containers,
            templates=templates,
            swarm=swarm,
            kubernetes=kubernetes,
            services=services,
            users=users,
            dns=dns,
            docker=docker,
            template_config=template_config,
            ssh=ssh,
            waits=waits,
            timeouts=timeouts,
            glusterfs=glusterfs,
            apt_cache_ct=data.get("apt-cache-ct", "apt-cache"),
        )

    def compute_derived_fields(self):
        """Compute derived fields like network_base, gateway, and IP addresses"""
        # Compute network_base
        network = self.network.split("/")[0]
        parts = network.split(".")
        self.network_base = ".".join(parts[:-1])
        # Compute gateway
        self.gateway = f"{self.network_base}.{self.proxmox.gateway_octet}"
        # Compute IP addresses for containers
        for container in self.containers:
            container.ip_address = f"{self.network_base}.{container.ip}"
        # Compute IP addresses for templates
        for template in self.templates:
            template.ip_address = f"{self.network_base}.{template.ip}"
        # Build swarm managers and workers lists
        self.swarm_managers = [ct for ct in self.containers if ct.id in self.swarm.managers]
        self.swarm_workers = [ct for ct in self.containers if ct.id in self.swarm.workers]
        # Build kubernetes control and workers lists
        if self.kubernetes:
            self.kubernetes_control = [ct for ct in self.containers if ct.id in self.kubernetes.control]
            self.kubernetes_workers = [ct for ct in self.containers if ct.id in self.kubernetes.workers]
    # Convenience properties for backward compatibility
    @property

    def proxmox_host(self) -> str:
        """Return proxmox host."""
        return self.proxmox.host
    @property

    def proxmox_storage(self) -> str:
        """Return proxmox storage."""
        return self.proxmox.storage
    @property

    def proxmox_bridge(self) -> str:
        """Return proxmox bridge."""
        return self.proxmox.bridge
    @property

    def proxmox_template_dir(self) -> str:
        """Return proxmox template directory."""
        return self.proxmox.template_dir
    @property

    def swarm_port(self) -> int:
        """Return Docker Swarm port."""
        return self.services.docker_swarm.port
    @property

    def portainer_port(self) -> int:
        """Return Portainer port."""
        return self.services.portainer.port
    @property

    def portainer_image(self) -> str:
        """Return Portainer image."""
        return self.services.portainer.image
    @property

    def apt_cache_port(self) -> int:
        """Return apt-cache port."""
        return self.services.apt_cache.port
    @property

    def container_resources(self) -> Dict[str, Any]:
        """Backward compatibility: return empty dict."""
        return {}
    @property

    def template_resources(self) -> Dict[str, Any]:
        """Backward compatibility: return empty dict."""
        return {}