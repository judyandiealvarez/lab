"""
Configuration data model - class-based representation of lab.yaml
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path


@dataclass
class ContainerResources:
    """Container resource allocation"""
    memory: int
    swap: int
    cores: int
    rootfs_size: int


@dataclass
class ContainerConfig:
    """Container configuration"""
    name: str
    id: int
    ip: int  # Last octet only
    hostname: str
    type: str
    template: Optional[str] = None
    resources: Optional[ContainerResources] = None
    params: Dict[str, Any] = field(default_factory=dict)
    ip_address: Optional[str] = None  # Full IP, computed later


@dataclass
class TemplateConfig:
    """Template configuration"""
    name: str
    id: int
    ip: int  # Last octet only
    hostname: str
    type: str
    resources: Optional[ContainerResources] = None
    ip_address: Optional[str] = None  # Full IP, computed later


@dataclass
class SwarmConfig:
    """Docker Swarm configuration"""
    managers: List[int] = field(default_factory=list)
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


@dataclass
class ServicesConfig:
    """All services configuration"""
    apt_cache: ServiceConfig
    docker_swarm: ServiceConfig
    portainer: ServiceConfig
    postgresql: Optional[ServiceConfig] = None
    haproxy: Optional[ServiceConfig] = None


@dataclass
class UsersConfig:
    """User configuration"""
    default_user: str
    sudo_group: str


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
class SSHConfig:
    """SSH configuration"""
    connect_timeout: int
    batch_mode: bool


@dataclass
class WaitsConfig:
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
class LabConfig:
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
    apt_cache_ct: str = "apt-cache"
    
    # Computed fields
    network_base: Optional[str] = None
    gateway: Optional[str] = None
    swarm_managers: List[ContainerConfig] = field(default_factory=list)
    swarm_workers: List[ContainerConfig] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LabConfig':
        """Create LabConfig from dictionary (loaded from YAML)"""
        # Helper to create ContainerResources from dict
        def make_resources(res_dict: Optional[Dict]) -> Optional[ContainerResources]:
            if not res_dict:
                return None
            return ContainerResources(
                memory=res_dict['memory'],
                swap=res_dict['swap'],
                cores=res_dict['cores'],
                rootfs_size=res_dict['rootfs_size']
            )
        
        # Parse containers
        containers = []
        for ct in data.get('ct', []):
            containers.append(ContainerConfig(
                name=ct['name'],
                id=ct['id'],
                ip=ct['ip'],
                hostname=ct['hostname'],
                type=ct['type'],
                template=ct.get('template'),
                resources=make_resources(ct.get('resources')),
                params=ct.get('params', {})
            ))
        
        # Parse templates
        templates = []
        for tmpl in data.get('templates', []):
            templates.append(TemplateConfig(
                name=tmpl['name'],
                id=tmpl['id'],
                ip=tmpl['ip'],
                hostname=tmpl['hostname'],
                type=tmpl['type'],
                resources=make_resources(tmpl.get('resources'))
            ))
        
        # Parse swarm
        swarm_data = data.get('swarm', {})
        swarm = SwarmConfig(
            managers=[m['id'] if isinstance(m, dict) else m for m in swarm_data.get('managers', [])],
            workers=[w['id'] if isinstance(w, dict) else w for w in swarm_data.get('workers', [])]
        )
        
        # Parse proxmox
        proxmox_data = data['proxmox']
        proxmox = ProxmoxConfig(
            host=proxmox_data['host'],
            storage=proxmox_data['storage'],
            bridge=proxmox_data['bridge'],
            template_dir=proxmox_data['template_dir'],
            gateway_octet=proxmox_data['gateway_octet']
        )
        
        # Parse services
        services_data = data['services']
        services = ServicesConfig(
            apt_cache=ServiceConfig(port=services_data['apt_cache']['port']),
            docker_swarm=ServiceConfig(port=services_data['docker_swarm']['port']),
            portainer=ServiceConfig(
                port=services_data['portainer']['port'],
                image=services_data['portainer']['image']
            ),
            postgresql=ServiceConfig(port=services_data.get('postgresql', {}).get('port')) if 'postgresql' in services_data else None,
            haproxy=ServiceConfig(
                http_port=services_data.get('haproxy', {}).get('http_port'),
                https_port=services_data.get('haproxy', {}).get('https_port'),
                stats_port=services_data.get('haproxy', {}).get('stats_port')
            ) if 'haproxy' in services_data else None
        )
        
        # Parse users
        users_data = data['users']
        users = UsersConfig(
            default_user=users_data['default_user'],
            sudo_group=users_data['sudo_group']
        )
        
        # Parse DNS
        dns_data = data['dns']
        dns = DNSConfig(servers=dns_data['servers'])
        
        # Parse Docker
        docker_data = data['docker']
        docker = DockerConfig(
            version=docker_data['version'],
            repository=docker_data['repository'],
            release=docker_data['release'],
            ubuntu_release=docker_data['ubuntu_release']
        )
        
        # Parse template_config
        template_config_data = data.get('template_config', {})
        template_config = TemplatePatternsConfig(
            base=template_config_data.get('base', []),
            patterns=template_config_data.get('patterns', {}),
            preserve=template_config_data.get('preserve', [])
        )
        
        # Parse SSH
        ssh_data = data['ssh']
        ssh = SSHConfig(
            connect_timeout=ssh_data['connect_timeout'],
            batch_mode=ssh_data['batch_mode']
        )
        
        # Parse waits
        waits_data = data['waits']
        waits = WaitsConfig(
            container_startup=waits_data['container_startup'],
            container_ready_max_attempts=waits_data['container_ready_max_attempts'],
            container_ready_sleep=waits_data['container_ready_sleep'],
            network_config=waits_data['network_config'],
            service_start=waits_data['service_start'],
            swarm_init=waits_data['swarm_init'],
            portainer_start=waits_data['portainer_start'],
            glusterfs_setup=waits_data['glusterfs_setup']
        )
        
        # Parse timeouts
        timeouts_data = data['timeouts']
        timeouts = TimeoutsConfig(
            apt_cache=timeouts_data['apt_cache'],
            ubuntu_template=timeouts_data['ubuntu_template'],
            docker_template=timeouts_data['docker_template'],
            swarm_deploy=timeouts_data['swarm_deploy']
        )
        
        # Parse GlusterFS (optional)
        glusterfs = None
        if 'glusterfs' in data:
            glusterfs_data = data['glusterfs']
            glusterfs = GlusterFSConfig(
                volume_name=glusterfs_data.get('volume_name', 'swarm-storage'),
                brick_path=glusterfs_data.get('brick_path', '/gluster/brick'),
                mount_point=glusterfs_data.get('mount_point', '/mnt/gluster'),
                replica_count=glusterfs_data.get('replica_count', 2)
            )
        
        return cls(
            network=data['network'],
            proxmox=proxmox,
            containers=containers,
            templates=templates,
            swarm=swarm,
            services=services,
            users=users,
            dns=dns,
            docker=docker,
            template_config=template_config,
            ssh=ssh,
            waits=waits,
            timeouts=timeouts,
            glusterfs=glusterfs,
            apt_cache_ct=data.get('apt-cache-ct', 'apt-cache')
        )
    
    def compute_derived_fields(self):
        """Compute derived fields like network_base, gateway, and IP addresses"""
        # Compute network_base
        network = self.network.split('/')[0]
        parts = network.split('.')
        self.network_base = '.'.join(parts[:-1])
        
        # Compute gateway
        self.gateway = f"{self.network_base}.{self.proxmox.gateway_octet}"
        
        # Compute IP addresses for containers
        for container in self.containers:
            container.ip_address = f"{self.network_base}.{container.ip}"
        
        # Compute IP addresses for templates
        for template in self.templates:
            template.ip_address = f"{self.network_base}.{template.ip}"
        
        # Build swarm managers and workers lists
        self.swarm_managers = [
            ct for ct in self.containers
            if ct.id in self.swarm.managers
        ]
        self.swarm_workers = [
            ct for ct in self.containers
            if ct.id in self.swarm.workers
        ]
    
    # Convenience properties for backward compatibility
    @property
    def proxmox_host(self) -> str:
        return self.proxmox.host
    
    @property
    def proxmox_storage(self) -> str:
        return self.proxmox.storage
    
    @property
    def proxmox_bridge(self) -> str:
        return self.proxmox.bridge
    
    @property
    def proxmox_template_dir(self) -> str:
        return self.proxmox.template_dir
    
    @property
    def swarm_port(self) -> int:
        return self.services.docker_swarm.port
    
    @property
    def portainer_port(self) -> int:
        return self.services.portainer.port
    
    @property
    def portainer_image(self) -> str:
        return self.services.portainer.image
    
    @property
    def apt_cache_port(self) -> int:
        return self.services.apt_cache.port
    
    @property
    def container_resources(self) -> Dict[str, Any]:
        """Backward compatibility: return empty dict"""
        return {}
    
    @property
    def template_resources(self) -> Dict[str, Any]:
        """Backward compatibility: return empty dict"""
        return {}

