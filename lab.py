#!/usr/bin/env python3
"""
Lab CLI Tool - Manage Proxmox LXC containers and Docker Swarm
Implements all functionality directly in Python (not just calling bash scripts)
"""
import subprocess
import sys
import argparse
import os
import time
import re
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent.absolute()
CONFIG_FILE = SCRIPT_DIR / "lab.yaml"

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def load_config():
    """Load configuration from lab.yaml"""
    if not CONFIG_FILE.exists():
        print(f"Error: Configuration file {CONFIG_FILE} not found", file=sys.stderr)
        sys.exit(1)
    
    try:
        if HAS_YAML:
            with open(CONFIG_FILE, 'r') as f:
                config = yaml.safe_load(f)
        else:
            print("Error: PyYAML is required. Install it with: pip install pyyaml", file=sys.stderr)
            sys.exit(1)
        
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)


def get_network_base(network_cidr):
    """Extract network base from CIDR notation (e.g., 10.11.3.0/24 -> 10.11.3)"""
    network = network_cidr.split('/')[0]
    parts = network.split('.')
    return '.'.join(parts[:-1])


def get_gateway(network_cidr, gateway_octet):
    """Get gateway IP from network (e.g., 10.11.3.0/24 -> 10.11.3.253)"""
    base = get_network_base(network_cidr)
    return f"{base}.{gateway_octet}"


def get_config():
    """Get configuration and return as convenient variables"""
    config = load_config()
    
    network_base = get_network_base(config['network'])
    gateway = get_gateway(config['network'], config['proxmox']['gateway_octet'])
    
    def build_ip(last_octet):
        return f"{network_base}.{last_octet}"
    
    # Build containers list with full IPs
    containers = []
    if 'ct' in config:
        for ct in config['ct']:
            ct_copy = ct.copy()
            ct_copy['ip_address'] = build_ip(ct['ip'])
            containers.append(ct_copy)
    
    # Build swarm info from containers
    swarm_managers = []
    swarm_workers = []
    if 'swarm' in config and 'managers' in config['swarm']:
        for mgr_id in config['swarm']['managers']:
            for ct in containers:
                if ct['id'] == mgr_id:
                    swarm_managers.append(ct)
                    break
    if 'swarm' in config and 'workers' in config['swarm']:
        for worker_id in config['swarm']['workers']:
            for ct in containers:
                if ct['id'] == worker_id:
                    swarm_workers.append(ct)
                    break
    
    return {
        'proxmox_host': config['proxmox']['host'],
        'proxmox_storage': config['proxmox']['storage'],
        'proxmox_bridge': config['proxmox']['bridge'],
        'proxmox_template_dir': config['proxmox']['template_dir'],
        'network': config['network'],
        'network_base': network_base,
        'gateway': gateway,
        'containers': containers,
        'swarm_managers': swarm_managers,
        'swarm_workers': swarm_workers,
        'templates': config['templates'],
        'template_config': config.get('template_config', {}),
        'swarm_port': config['services']['docker_swarm']['port'],
        'portainer_port': config['services']['portainer']['port'],
        'portainer_image': config['services']['portainer']['image'],
        'apt_cache_port': config['services']['apt_cache']['port'],
        'timeouts': config['timeouts'],
        'container_resources': config.get('containers', {}),  # For backward compatibility
        'template_resources': config.get('template_resources', {}),
        'users': config['users'],
        'dns': config['dns'],
        'docker': config['docker'],
        'ssh': config['ssh'],
        'waits': config['waits'],
        'glusterfs': config.get('glusterfs', {})
    }


def ssh_exec(host, command, check=True, capture_output=False, timeout=None, cfg=None):
    """Execute command via SSH"""
    connect_timeout = cfg['ssh']['connect_timeout'] if cfg and 'ssh' in cfg else 10
    batch_mode = 'yes' if (cfg and cfg['ssh'].get('batch_mode', True)) else 'no'
    cmd = f'ssh -o ConnectTimeout={connect_timeout} -o BatchMode={batch_mode} {host} "{command}"'
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True,
            timeout=timeout
        )
        if capture_output:
            return result.stdout.strip()
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except subprocess.CalledProcessError:
        return False


def pct_exec(proxmox_host, container_id, command, check=True, capture_output=False, cfg=None):
    """Execute command in container via pct exec"""
    # Use base64 encoding to avoid quote escaping issues
    import base64
    encoded_cmd = base64.b64encode(command.encode()).decode()
    # Decode and execute via bash
    connect_timeout = cfg['ssh']['connect_timeout'] if cfg and 'ssh' in cfg else 10
    batch_mode = 'yes' if (cfg and cfg['ssh'].get('batch_mode', True)) else 'no'
    cmd = f"ssh -o ConnectTimeout={connect_timeout} -o BatchMode={batch_mode} {proxmox_host} 'pct exec {container_id} -- bash -c \"echo {encoded_cmd} | base64 -d | bash\"'"
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True
        )
        if capture_output:
            return result.stdout.strip()
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False


def container_exists(proxmox_host, container_id, cfg=None):
    """Check if container exists"""
    result = ssh_exec(proxmox_host, f"pct list | grep -q '^{container_id} '", check=False, capture_output=False, cfg=cfg)
    return result


def destroy_container(proxmox_host, container_id, cfg=None):
    """Destroy container if it exists"""
    if container_exists(proxmox_host, container_id, cfg=cfg):
        print(f"  Destroying existing container {container_id}...")
        ssh_exec(proxmox_host, f"pct stop {container_id} 2>/dev/null || true", check=False, cfg=cfg)
        ssh_exec(proxmox_host, f"pct destroy {container_id} 2>/dev/null || true", check=False, cfg=cfg)


def wait_for_container(proxmox_host, container_id, ip_address, max_attempts=None, sleep_interval=None, cfg=None):
    """Wait for container to be ready"""
    if max_attempts is None:
        max_attempts = cfg['waits']['container_ready_max_attempts'] if cfg and 'waits' in cfg else 30
    if sleep_interval is None:
        sleep_interval = cfg['waits']['container_ready_sleep'] if cfg and 'waits' in cfg else 3
    for i in range(1, max_attempts + 1):
        status = ssh_exec(proxmox_host, f"pct status {container_id} 2>&1", check=False, capture_output=True, cfg=cfg)
        if 'running' in status:
            # Try ping
            try:
                ping_result = subprocess.run(
                    f"ping -c 1 -W 2 {ip_address}",
                    shell=True,
                    capture_output=True,
                    timeout=5
                )
                if ping_result.returncode == 0:
                    print("Container is up!")
                    return True
            except (subprocess.TimeoutExpired, Exception):
                pass
            
            # Try SSH via pct exec (more reliable than direct SSH)
            try:
                test_result = pct_exec(proxmox_host, container_id, "echo test", check=False, capture_output=True, cfg=cfg)
                if test_result == "test":
                    print("Container is up (pct exec working)!")
                    return True
            except Exception:
                pass
            
            # Try SSH directly (fallback)
            try:
                connect_timeout = cfg['ssh']['connect_timeout'] if cfg and 'ssh' in cfg else 3
                ssh_result = subprocess.run(
                    f'ssh -o ConnectTimeout={connect_timeout} -o BatchMode=yes -o StrictHostKeyChecking=no root@{ip_address} "echo test"',
                    shell=True,
                    capture_output=True,
                    timeout=connect_timeout
                )
                if ssh_result.returncode == 0:
                    print("Container is up (SSH working)!")
                    return True
            except (subprocess.TimeoutExpired, Exception):
                pass
        
        print(f"Waiting... ({i}/{max_attempts})")
        time.sleep(sleep_interval)
    
    print("WARNING: Container may not be fully ready, but continuing...")
    return True  # Continue anyway


def get_ssh_key():
    """Get SSH public key"""
    key_paths = [
        Path.home() / ".ssh" / "id_rsa.pub",
        Path.home() / ".ssh" / "id_ed25519.pub"
    ]
    for key_path in key_paths:
        if key_path.exists():
            return key_path.read_text().strip()
    return ""


def setup_ssh_key(proxmox_host, container_id, ip_address, cfg=None):
    """Setup SSH key in container"""
    ssh_key = get_ssh_key()
    if not ssh_key:
        return
    
    default_user = cfg['users']['default_user'] if cfg and 'users' in cfg else 'jaal'
    
    # Remove old host key
    subprocess.run(f"ssh-keygen -R {ip_address} 2>/dev/null", shell=True)
    
    # Add to default user - ensure directory exists first
    pct_exec(proxmox_host, container_id,
             f"mkdir -p /home/{default_user}/.ssh && echo '{ssh_key}' > /home/{default_user}/.ssh/authorized_keys && chmod 600 /home/{default_user}/.ssh/authorized_keys && chown {default_user}:{default_user} /home/{default_user}/.ssh/authorized_keys",
             check=False, cfg=cfg)
    
    # Add to root user
    pct_exec(proxmox_host, container_id,
             f"mkdir -p /root/.ssh && echo '{ssh_key}' > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys",
             check=False, cfg=cfg)


def get_base_template(proxmox_host, cfg):
    """Get base Ubuntu template, download if needed"""
    templates = cfg['template_config']['base']
    template_dir = cfg['proxmox_template_dir']
    
    for template in templates:
        if ssh_exec(proxmox_host, f"test -f {template_dir}/{template}", check=False, cfg=cfg):
            return template
    
    # Download last template in list
    print(f"Base template not found. Downloading {templates[-1]}...")
    ssh_exec(proxmox_host, f"pveam download local {templates[-1]} 2>&1 | tail -3", check=False, cfg=cfg)
    return templates[-1]


def create_container(container_cfg, cfg, step_num, total_steps):
    """Generic container creation dispatcher based on type"""
    container_type = container_cfg['type']
    container_name = container_cfg['name']
    
    print(f"\n[{step_num}/{total_steps}] Creating container '{container_name}' (type: {container_type})...")
    
    # Dispatch based on type
    if container_type == 'apt-cache':
        return create_container_apt_cache(container_cfg, cfg)
    elif container_type == 'pgsql':
        return create_container_pgsql(container_cfg, cfg)
    elif container_type == 'haproxy':
        return create_container_haproxy(container_cfg, cfg)
    elif container_type == 'swarm-manager':
        return create_container_swarm_manager(container_cfg, cfg)
    elif container_type == 'swarm-node':
        return create_container_swarm_node(container_cfg, cfg)
    else:
        print(f"ERROR: Unknown container type '{container_type}'", file=sys.stderr)
        return False


def get_template_path(template_name, cfg):
    """Get path to template file by template name"""
    proxmox_host = cfg['proxmox_host']
    template_dir = cfg['proxmox_template_dir']
    
    # Find template config
    template_cfg = None
    for tmpl in cfg['templates']:
        if tmpl['name'] == template_name:
            template_cfg = tmpl
            break
    
    if not template_cfg:
        # Fallback to base template
        base_template = get_base_template(proxmox_host, cfg)
        return f"{template_dir}/{base_template}"
    
    # Find template file by pattern
    template_type = template_cfg['type']
    pattern = cfg['template_config']['patterns'].get(template_type, '').replace('{date}', '*')
    template_file = ssh_exec(proxmox_host,
                           f"ls -t {template_dir}/{pattern} 2>/dev/null | head -1 | xargs basename 2>/dev/null",
                           check=False, capture_output=True, cfg=cfg)
    
    if template_file:
        return f"{template_dir}/{template_file.strip()}"
    else:
        # Fallback to base template
        base_template = get_base_template(proxmox_host, cfg)
        return f"{template_dir}/{base_template}"


def create_container_apt_cache(container_cfg, cfg):
    """Create apt-cacher-ng container - method for type 'apt-cache'"""
    proxmox_host = cfg['proxmox_host']
    container_id = container_cfg['id']
    ip_address = container_cfg['ip_address']
    hostname = container_cfg['hostname']
    gateway = cfg['gateway']
    template_name = container_cfg.get('template', 'ubuntu-tmpl')
    
    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    # Get template path
    template_path = get_template_path(template_name, cfg)
    
    # Get container resources
    resources = container_cfg.get('resources', {})
    if not resources:
        # Fallback to container_resources for backward compatibility
        resources = cfg.get('container_resources', {}).get('apt_cache', {})
    storage = cfg['proxmox_storage']
    bridge = cfg['proxmox_bridge']
    
    # Create container
    print(f"Creating container {container_id} from template...")
    ssh_exec(proxmox_host,
             f"pct create {container_id} {template_path} "
             f"--hostname {hostname} "
             f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
             f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
             f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged 1 --ostype ubuntu --arch amd64",
             check=True, cfg=cfg)
    
    # Start container
    print("Starting container...")
    ssh_exec(proxmox_host, f"pct start {container_id}", check=True, cfg=cfg)
    time.sleep(cfg['waits']['container_startup'])
    
    # Configure network
    print("Configuring network...")
    pct_exec(proxmox_host, container_id,
             f"ip link set eth0 up && ip addr add {ip_address}/24 dev eth0 2>/dev/null || true && ip route add default via {gateway} dev eth0 2>/dev/null || true && sleep 2",
             check=False, cfg=cfg)
    time.sleep(cfg['waits']['network_config'])
    
    # Wait for container
    if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
        print("WARNING: Container may not be fully ready, but continuing...")
    
    # Create user and configure sudo
    default_user = cfg['users']['default_user']
    sudo_group = cfg['users']['sudo_group']
    print("Creating user and configuring sudo...")
    pct_exec(proxmox_host, container_id,
             f"useradd -m -s /bin/bash -G {sudo_group} {default_user} 2>/dev/null || echo User exists; "
             f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' | tee /etc/sudoers.d/{default_user}; "
             f"chmod 440 /etc/sudoers.d/{default_user}; "
             f"mkdir -p /home/{default_user}/.ssh; chown -R {default_user}:{default_user} /home/{default_user}; chmod 700 /home/{default_user}/.ssh",
             check=False, cfg=cfg)
    
    # Setup SSH key
    print("Setting up SSH key...")
    setup_ssh_key(proxmox_host, container_id, ip_address, cfg)
    
    # Configure DNS
    print("Configuring DNS...")
    dns_servers = cfg['dns']['servers']
    dns_cmd = " && ".join([f"echo 'nameserver {dns}' >> /etc/resolv.conf" for dns in dns_servers])
    pct_exec(proxmox_host, container_id,
             f"echo 'nameserver {dns_servers[0]}' > /etc/resolv.conf && {dns_cmd.replace(dns_servers[0], '', 1).lstrip(' && ')}",
             check=False, cfg=cfg)
    
    # Fix apt sources
    print("Fixing apt sources...")
    fix_sources_cmd = (
        "if grep -q oracular /etc/apt/sources.list; then "
        "sed -i 's/oracular/plucky/g' /etc/apt/sources.list && "
        "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true && "
        "sed -i 's/plucky main/plucky main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/plucky-updates main/plucky-updates main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/plucky-security main/plucky-security main universe multiverse/g' /etc/apt/sources.list; "
        "elif grep -q noble /etc/apt/sources.list; then "
        "sed -i 's/noble main/noble main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/noble-updates main/noble-updates main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/noble-security main/noble-security main universe multiverse/g' /etc/apt/sources.list; "
        "fi"
    )
    pct_exec(proxmox_host, container_id, fix_sources_cmd, check=False)
    
    # Update and upgrade
    print("Updating package lists...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt update -y 2>&1 | tail -10",
             check=False)
    
    print("Upgrading to latest Ubuntu distribution (25.04)...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt dist-upgrade -y 2>&1 | tail -10",
             check=False)
    
    # Install apt-cacher-ng
    print("Installing apt-cacher-ng...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt install -y apt-cacher-ng 2>&1 | tail -10",
             check=False)
    
    # Configure port
    apt_cache_port = cfg['apt_cache_port']
    print(f"Configuring apt-cacher-ng to use port {apt_cache_port}...")
    pct_exec(proxmox_host, container_id,
             f"sed -i 's/^Port: .*/Port: {apt_cache_port}/' /etc/apt-cacher-ng/acng.conf 2>/dev/null || echo 'Port: {apt_cache_port}' >> /etc/apt-cacher-ng/acng.conf",
             check=False, cfg=cfg)
    
    # Start service
    print("Starting apt-cacher-ng service...")
    pct_exec(proxmox_host, container_id,
             "systemctl enable apt-cacher-ng && systemctl restart apt-cacher-ng",
             check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['service_start'])
    
    print(f"✓ apt-cache container '{container_cfg['name']}' created")
    return True


def create_template(template_cfg, cfg, step_num, total_steps):
    """Generic template creation dispatcher based on type"""
    template_type = template_cfg['type']
    template_name = template_cfg['name']
    
    print(f"\n[{step_num}/{total_steps}] Creating template '{template_name}' (type: {template_type})...")
    
    # Build IP address from last octet
    network_base = cfg['network_base']
    ip_address = f"{network_base}.{template_cfg['ip']}"
    
    # Prepare template config with full IP
    prepared_cfg = template_cfg.copy()
    prepared_cfg['ip_address'] = ip_address
    
    # Dispatch based on type
    if template_type == 'ubuntu':
        return create_template_ubuntu(prepared_cfg, cfg)
    elif template_type == 'ubuntu+docker':
        return create_template_ubuntu_docker(prepared_cfg, cfg)
    else:
        print(f"ERROR: Unknown template type '{template_type}'", file=sys.stderr)
        return False


def create_template_ubuntu(template_cfg, cfg):
    """Create Ubuntu template - method for type 'ubuntu'"""
    proxmox_host = cfg['proxmox_host']
    container_id = template_cfg['id']
    ip_address = template_cfg['ip_address']
    hostname = template_cfg['hostname']
    gateway = cfg['gateway']
    apt_cache_ip = cfg['apt_cache_ip']
    template_name = template_cfg['name']
    
    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    # Get container resources and settings
    resources = template_cfg.get('resources', {})
    if not resources:
        # Fallback to template_resources or container_resources
        resources = cfg.get('template_resources', {}).get(template_name, {})
        if not resources:
            resources = cfg.get('container_resources', {}).get(template_name, {})
    storage = cfg['proxmox_storage']
    bridge = cfg['proxmox_bridge']
    template_dir = cfg['proxmox_template_dir']
    base_template = get_base_template(proxmox_host, cfg)
    
    # Create container
    print(f"Creating container {container_id}...")
    ssh_exec(proxmox_host,
             f"pct create {container_id} {template_dir}/{base_template} "
             f"--hostname {hostname} "
             f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
             f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
             f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged 0 --ostype ubuntu --arch amd64",
             check=True, cfg=cfg)
    
    # Configure features
    print("Configuring container features...")
    ssh_exec(proxmox_host, f"pct set {container_id} --features nesting=1,keyctl=1,fuse=1", check=False, cfg=cfg)
    
    # Start container
    print("Starting container...")
    ssh_exec(proxmox_host, f"pct start {container_id}", check=True, cfg=cfg)
    time.sleep(cfg['waits']['container_startup'])
    
    # Wait for container
    wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg)
    
    # Configure apt cache FIRST before any apt operations
    print("Configuring apt cache...")
    apt_cache_port = cfg['apt_cache_port']
    pct_exec(proxmox_host, container_id,
             f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true",
             check=False, cfg=cfg)
    
    # Fix apt sources
    print("Fixing apt sources...")
    pct_exec(proxmox_host, container_id,
             "sed -i 's/oracular/plucky/g' /etc/apt/sources.list || true; "
             "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true",
             check=False, cfg=cfg)
    
    # Setup user and SSH
    default_user = cfg['users']['default_user']
    sudo_group = cfg['users']['sudo_group']
    print("Setting up user and SSH access...")
    pct_exec(proxmox_host, container_id,
             f"apt-get update -qq || true; "
             f"id -u {default_user} >/dev/null 2>&1 || useradd -m -s /bin/bash -G {sudo_group} {default_user}; "
             f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/{default_user}; "
             f"chmod 440 /etc/sudoers.d/{default_user}; "
             f"mkdir -p /home/{default_user}/.ssh /root/.ssh; chmod 700 /home/{default_user}/.ssh; "
             "apt-get install -y -qq openssh-server >/dev/null 2>&1 || true; "
             "systemctl enable ssh >/dev/null 2>&1 || true; "
             "systemctl start ssh >/dev/null 2>&1 || true",
             check=False, cfg=cfg)
    
    setup_ssh_key(proxmox_host, container_id, ip_address, cfg)
    
    # Upgrade distribution
    print("Upgrading distribution to latest (25.04)...")
    pct_exec(proxmox_host, container_id,
             f"apt-get update -qq || true; "
             f"DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y >/dev/null 2>&1 || true",
             check=False, cfg=cfg)
    
    # Install base tools
    print("Installing minimal base tools...")
    pct_exec(proxmox_host, container_id,
             "apt-get install -y -qq ca-certificates curl >/dev/null 2>&1 || true",
             check=False, cfg=cfg)
    
    # Cleanup for template
    print("Cleanup for template...")
    pct_exec(proxmox_host, container_id,
             f"bash -c '"
             f"rm -f /etc/apt/apt.conf.d/01proxy || true; "
             f"rm -f /etc/ssh/ssh_host_* || true; "
             f"truncate -s 0 /etc/machine-id || true; "
             f"rm -f /var/lib/dbus/machine-id || true; "
             f"ln -s /etc/machine-id /var/lib/dbus/machine-id || true; "
             f"apt-get clean; "
             f"rm -rf /var/lib/apt/lists/* || true; "
             f"find /var/log -type f -name \"*.log\" -delete 2>/dev/null || true; "
             f"find /var/log -type f -name \"*.gz\" -delete 2>/dev/null || true; "
             f"truncate -s 0 /root/.bash_history 2>/dev/null || true; "
             f"truncate -s 0 /home/{cfg['users']['default_user']}/.bash_history 2>/dev/null || true'",
             check=False, cfg=cfg)
    
    # Stop container
    print("Stopping container...")
    ssh_exec(proxmox_host, f"pct stop {container_id}", check=False, cfg=cfg)
    
    # Create template
    template_dir = cfg['proxmox_template_dir']
    print("Creating template archive...")
    ssh_exec(proxmox_host,
             f"vzdump {container_id} --dumpdir {template_dir} --compress zstd --mode stop 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Rename template
    template_dir = cfg['proxmox_template_dir']
    template_pattern = cfg['template_config']['patterns']['ubuntu']
    final_template_name = template_pattern.replace('{date}', datetime.now().strftime('%Y%m%d'))
    backup_file = ssh_exec(proxmox_host,
                          f"ls -t {template_dir}/vzdump-lxc-{container_id}-*.tar.zst 2>/dev/null | head -1",
                          check=False, capture_output=True, cfg=cfg)
    
    if backup_file:
        ssh_exec(proxmox_host,
                f"mv '{backup_file}' {template_dir}/{final_template_name} && ls -lh {template_dir}/{final_template_name}",
                check=False, cfg=cfg)
    
    # Update template list
    ssh_exec(proxmox_host, "pveam update >/dev/null 2>&1 || true", check=False, cfg=cfg)
    
    # Cleanup other templates
    print("Cleaning up other template archives...")
    preserve_patterns = " ".join([f"! -name '{p}'" for p in cfg['template_config']['preserve']])
    ssh_exec(proxmox_host,
             f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
             f"! -name '{final_template_name}' {preserve_patterns} -delete || true",
             check=False, cfg=cfg)
    
    # Destroy container
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    print(f"✓ Ubuntu template '{template_name}' created")
    return True


def create_template_ubuntu_docker(template_cfg, cfg):
    """Create Docker template - method for type 'ubuntu+docker'"""
    proxmox_host = cfg['proxmox_host']
    container_id = template_cfg['id']
    ip_address = template_cfg['ip_address']
    hostname = template_cfg['hostname']
    gateway = cfg['gateway']
    apt_cache_ip = cfg['apt_cache_ip']
    template_name = template_cfg['name']
    
    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    # Get container resources and settings
    resources = template_cfg.get('resources', {})
    if not resources:
        # Fallback to template_resources or container_resources
        resources = cfg.get('template_resources', {}).get(template_name, {})
        if not resources:
            resources = cfg.get('container_resources', {}).get(template_name, {})
    storage = cfg['proxmox_storage']
    bridge = cfg['proxmox_bridge']
    template_dir = cfg['proxmox_template_dir']
    base_template = get_base_template(proxmox_host, cfg)
    default_user = cfg['users']['default_user']
    sudo_group = cfg['users']['sudo_group']
    apt_cache_port = cfg['apt_cache_port']
    
    # Create container
    print(f"Creating container {container_id}...")
    ssh_exec(proxmox_host,
             f"pct create {container_id} {template_dir}/{base_template} "
             f"--hostname {hostname} "
             f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
             f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
             f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged 0 --ostype ubuntu --arch amd64",
             check=True, cfg=cfg)
    
    # Configure features
    print("Configuring container features...")
    ssh_exec(proxmox_host, f"pct set {container_id} --features nesting=1,keyctl=1,fuse=1", check=False, cfg=cfg)
    
    # Start container
    print("Starting container...")
    ssh_exec(proxmox_host, f"pct start {container_id}", check=True, cfg=cfg)
    time.sleep(cfg['waits']['container_startup'])
    
    # Wait for container
    wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg)
    
    # Setup user
    print("Creating user and configuring sudo...")
    pct_exec(proxmox_host, container_id,
             f"useradd -m -s /bin/bash -G {sudo_group} {default_user} 2>/dev/null || echo User exists; "
             f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' | tee /etc/sudoers.d/{default_user}; "
             f"chmod 440 /etc/sudoers.d/{default_user}; "
             f"mkdir -p /home/{default_user}/.ssh; chown -R {default_user}:{default_user} /home/{default_user}; chmod 700 /home/{default_user}/.ssh",
             check=False, cfg=cfg)
    
    setup_ssh_key(proxmox_host, container_id, ip_address, cfg)
    
    # Fix apt sources
    print("Fixing apt sources...")
    pct_exec(proxmox_host, container_id,
             "sed -i 's/oracular/plucky/g' /etc/apt/sources.list && "
             "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true",
             check=False, cfg=cfg)
    
    # Update packages
    print("Updating package lists...")
    pct_exec(proxmox_host, container_id,
             f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true; "
             f"DEBIAN_FRONTEND=noninteractive apt update -y 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Install prerequisites
    print("Installing prerequisites...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt install -y curl apt-transport-https ca-certificates software-properties-common 2>&1 | tail -5",
             check=False, cfg=cfg)
    
    # Upgrade
    print("Upgrading to latest Ubuntu distribution (25.04)...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt dist-upgrade -y 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Install Docker
    print("Installing Docker...")
    pct_exec(proxmox_host, container_id,
             "curl -fsSL https://get.docker.com -o /tmp/get-docker.sh && sh /tmp/get-docker.sh 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Fallback to docker.io if needed
    print("Verifying Docker install or applying fallback...")
    pct_exec(proxmox_host, container_id,
             "command -v docker >/dev/null 2>&1 || (echo 'get.docker.com failed; installing docker.io from Ubuntu repos...' && "
             "DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20)",
             check=False, cfg=cfg)
    
    # Configure Docker user group
    default_user = cfg['users']['default_user']
    print("Configuring Docker user group...")
    pct_exec(proxmox_host, container_id, f"usermod -aG docker {default_user}", check=False, cfg=cfg)
    
    # Start Docker
    print("Starting Docker service...")
    pct_exec(proxmox_host, container_id,
             "systemctl enable docker && systemctl start docker",
             check=False, cfg=cfg)
    
    # Disable AppArmor
    print("Disabling AppArmor for Docker...")
    pct_exec(proxmox_host, container_id,
             "systemctl stop apparmor && systemctl disable apparmor 2>/dev/null || true",
             check=False, cfg=cfg)
    
    # Verify Docker
    print("Verifying Docker installation...")
    pct_exec(proxmox_host, container_id, "docker --version && docker ps 2>&1 | head -5", check=False, cfg=cfg)
    
    # Cleanup for template
    print("Cleaning up container-specific data for template...")
    pct_exec(proxmox_host, container_id,
             "bash -c '"
             "rm -f /etc/apt/apt.conf.d/01proxy || true; "
             "echo \"localhost\" > /etc/hostname; "
             "hostnamectl set-hostname localhost 2>/dev/null || true; "
             f"rm -f /root/.ssh/authorized_keys 2>/dev/null || true; "
             f"rm -f /home/{cfg['users']['default_user']}/.ssh/authorized_keys 2>/dev/null || true; "
             "rm -f /etc/machine-id; touch /etc/machine-id; chmod 444 /etc/machine-id; "
             "journalctl --vacuum-time=1s 2>/dev/null || true; "
             "rm -rf /var/log/*.log 2>/dev/null || true; "
             "rm -rf /var/log/journal/* 2>/dev/null || true; "
             f"rm -f /root/.bash_history 2>/dev/null || true; "
             f"rm -f /home/{cfg['users']['default_user']}/.bash_history 2>/dev/null || true; "
             "apt clean 2>/dev/null || true; "
             "rm -rf /var/lib/apt/lists/* 2>/dev/null || true; "
             "systemctl stop docker 2>/dev/null || true; "
             "docker system prune -af 2>/dev/null || true'",
             check=False, cfg=cfg)
    
    # Stop container
    template_dir = cfg['proxmox_template_dir']
    print("Stopping container...")
    ssh_exec(proxmox_host, f"pct stop {container_id}", check=False, cfg=cfg)
    
    # Create template
    print("Creating template from container...")
    ssh_exec(proxmox_host,
             f"vzdump {container_id} --dumpdir {template_dir} --compress zstd --mode stop 2>&1 | grep -E '(creating|archive|Finished)'",
             check=False, cfg=cfg)
    time.sleep(2)
    
    # Rename template
    template_dir = cfg['proxmox_template_dir']
    template_pattern = cfg['template_config']['patterns']['ubuntu+docker']
    final_template_name = template_pattern.replace('{date}', datetime.now().strftime('%Y%m%d'))
    backup_file = ssh_exec(proxmox_host,
                          f"ls -t {template_dir}/vzdump-lxc-{container_id}-*.tar.zst 2>/dev/null | head -1",
                          check=False, capture_output=True, cfg=cfg)
    
    if backup_file:
        ssh_exec(proxmox_host,
                f"mv '{backup_file}' {template_dir}/{final_template_name} && echo 'Template created: {final_template_name}'",
                check=False, cfg=cfg)
    
    # Update template list
    ssh_exec(proxmox_host, "pveam update 2>&1 | tail -2", check=False, cfg=cfg)
    
    # Cleanup
    print("Cleaning up other template archives...")
    preserve_patterns = " ".join([f"! -name '{p}'" for p in cfg['template_config']['preserve']])
    ssh_exec(proxmox_host,
             f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' "
             f"! -name '{final_template_name}' {preserve_patterns} -delete || true",
             check=False, cfg=cfg)
    
    # Destroy container
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    print(f"✓ Docker template '{template_name}' created")
    return True


def setup_glusterfs(cfg):
    """Setup GlusterFS distributed storage across Swarm nodes"""
    print("\n[5/7] Setting up GlusterFS distributed storage...")
    
    proxmox_host = cfg['proxmox_host']
    
    if not cfg.get('glusterfs'):
        print("GlusterFS configuration not found, skipping...")
        return True
    
    gluster_cfg = cfg['glusterfs']
    volume_name = gluster_cfg.get('volume_name', 'swarm-storage')
    brick_path = gluster_cfg.get('brick_path', '/gluster/brick')
    mount_point = gluster_cfg.get('mount_point', '/mnt/gluster')
    replica_count = gluster_cfg.get('replica_count', 3)
    
    # Get all node info - manager for management, workers for storage
    manager_node = (cfg['manager_id'], cfg['manager_hostname'], cfg['manager_ip'])
    worker_nodes = [
        (cfg['worker1_id'], cfg['worker1_hostname'], cfg['worker1_ip']),
        (cfg['worker2_id'], cfg['worker2_hostname'], cfg['worker2_ip'])
    ]
    # All nodes for mounting, but only workers for storage bricks
    all_nodes = [manager_node] + worker_nodes
    
    # Install GlusterFS server on all nodes (manager for management, workers for storage)
    print("Installing GlusterFS server on all nodes...")
    apt_cache_port = cfg['apt_cache_port']
    apt_cache_ip = cfg['apt_cache_ip']
    for container_id, hostname, ip_address in all_nodes:
        print(f"  Installing on {hostname}...")
        # Configure apt cache proxy
        pct_exec(proxmox_host, container_id,
                f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true",
                check=False, cfg=cfg)
        
        # Update and install
        install_output = pct_exec(proxmox_host, container_id,
                f"DEBIAN_FRONTEND=noninteractive apt-get update -qq 2>&1 && "
                f"DEBIAN_FRONTEND=noninteractive apt-get install -y glusterfs-server glusterfs-client 2>&1",
                check=False, capture_output=True, cfg=cfg)
        
        # Verify installation
        verify_gluster = pct_exec(proxmox_host, container_id,
                f"command -v gluster >/dev/null 2>&1 && echo installed || echo not_installed",
                check=False, capture_output=True, cfg=cfg)
        
        if "not_installed" in verify_gluster:
            print(f"    ✗ Failed to install GlusterFS on {hostname}")
            if install_output:
                error_msg = install_output[:300] if len(install_output) > 300 else install_output
                print(f"    Error: {error_msg}")
            return False
        
        # Start and enable glusterd
        pct_exec(proxmox_host, container_id,
                f"systemctl enable glusterd 2>/dev/null && systemctl start glusterd 2>/dev/null",
                check=False, cfg=cfg)
        
        # Verify glusterd is running
        glusterd_check = pct_exec(proxmox_host, container_id,
                f"systemctl is-active glusterd 2>/dev/null || echo inactive",
                check=False, capture_output=True, cfg=cfg)
        if "active" in glusterd_check:
            print(f"    ✓ {hostname}: GlusterFS installed and glusterd running")
        else:
            print(f"    ⚠ {hostname}: glusterd not active")
    
    time.sleep(cfg['waits']['glusterfs_setup'])
    
    # Create brick directories (only on worker nodes)
    print("Creating brick directories on worker nodes...")
    for container_id, hostname, ip_address in worker_nodes:
        print(f"  Creating brick on {hostname}...")
        pct_exec(proxmox_host, container_id,
                f"mkdir -p {brick_path} && chmod 755 {brick_path}",
                check=False, cfg=cfg)
    
    # Peer nodes together (from manager)
    manager_id = cfg['manager_id']
    manager_hostname = cfg['manager_hostname']
    manager_ip = cfg['manager_ip']
    
    print("Peering worker nodes together...")
    for container_id, hostname, ip_address in worker_nodes:
        print(f"  Adding {hostname} ({ip_address}) to cluster...")
        # Try to probe, ignore if already connected
        pct_exec(proxmox_host, manager_id,
                f"gluster peer probe {hostname} 2>&1 || gluster peer probe {ip_address} 2>&1",
                check=False, cfg=cfg)
    
    time.sleep(10)  # Wait longer for peers to fully connect
    
    # Verify peer status - wait until all peers are connected
    print("Verifying peer status...")
    max_peer_attempts = 10
    for attempt in range(1, max_peer_attempts + 1):
        peer_status = pct_exec(proxmox_host, manager_id,
                              "gluster peer status 2>&1",
                              check=False, capture_output=True, cfg=cfg)
        if peer_status:
            print(peer_status)
            # Check if all peers are connected
            connected_count = peer_status.count("Peer in Cluster (Connected)")
            if connected_count >= len(worker_nodes):  # All workers connected
                print(f"  ✓ All {connected_count} worker peers connected")
                break
        if attempt < max_peer_attempts:
            print(f"  Waiting for peers to connect... ({attempt}/{max_peer_attempts})")
            time.sleep(3)
    else:
        print("  ⚠ Warning: Not all peers may be fully connected, continuing anyway...")
    
    # Create volume (only if it doesn't exist)
    print(f"Creating GlusterFS volume '{volume_name}'...")
    volume_exists = pct_exec(proxmox_host, manager_id,
                           f"gluster volume info {volume_name} >/dev/null 2>&1 && echo yes || echo no",
                           check=False, capture_output=True, cfg=cfg)
    
    if "yes" not in volume_exists:
        # Build volume create command - use IP addresses for reliability (only worker nodes)
        brick_list = " ".join([f"{ip_address}:{brick_path}" for _, _, ip_address in worker_nodes])
        create_cmd = (
            f"gluster volume create {volume_name} "
            f"replica {replica_count} {brick_list} force 2>&1"
        )
        create_output = pct_exec(proxmox_host, manager_id,
                                create_cmd,
                                check=False, capture_output=True, cfg=cfg)
        print(f"  {create_output}")
        
        # Check if creation was successful
        if "created" in create_output.lower() or "success" in create_output.lower():
            # Start volume
            print(f"Starting volume '{volume_name}'...")
            start_output = pct_exec(proxmox_host, manager_id,
                                   f"gluster volume start {volume_name} 2>&1",
                                   check=False, capture_output=True, cfg=cfg)
            print(f"  {start_output}")
        else:
            print(f"  ✗ Volume creation failed: {create_output}")
            return False
    else:
        print(f"  Volume '{volume_name}' already exists")
    
    # Verify volume status
    print("Verifying volume status...")
    vol_status = pct_exec(proxmox_host, manager_id,
                         f"gluster volume status {volume_name} 2>&1",
                         check=False, capture_output=True, cfg=cfg)
    if vol_status:
        print(vol_status)
    
    # Mount GlusterFS volume on all nodes (for access, not storage)
    print("Mounting GlusterFS volume on all nodes...")
    for container_id, hostname, ip_address in all_nodes:
        print(f"  Mounting on {hostname}...")
        # Create mount point
        pct_exec(proxmox_host, container_id,
                f"mkdir -p {mount_point}",
                check=False, cfg=cfg)
        
        # Add to fstab for persistence
        fstab_entry = f"{manager_hostname}:/{volume_name} {mount_point} glusterfs defaults,_netdev 0 0"
        pct_exec(proxmox_host, container_id,
                f"grep -q '{mount_point}' /etc/fstab || echo '{fstab_entry}' >> /etc/fstab",
                check=False, cfg=cfg)
        
        # Mount
        pct_exec(proxmox_host, container_id,
                f"mount -t glusterfs {manager_hostname}:/{volume_name} {mount_point} 2>&1 || "
                f"mount -t glusterfs {manager_ip}:/{volume_name} {mount_point} 2>&1",
                check=False, cfg=cfg)
        
        # Verify mount - check if it's actually mounted
        mount_verify = pct_exec(proxmox_host, container_id,
                              f"mount | grep -q '{mount_point}' && mount | grep '{mount_point}' | grep -q gluster && echo mounted || echo not_mounted",
                              check=False, capture_output=True, cfg=cfg)
        if "mounted" in mount_verify and "not_mounted" not in mount_verify:
            print(f"    ✓ {hostname}: Volume mounted successfully")
        else:
            # Check what actually happened
            mount_info = pct_exec(proxmox_host, container_id,
                                  f"mount | grep {mount_point} 2>/dev/null || echo 'NOT_MOUNTED'",
                                  check=False, capture_output=True, cfg=cfg)
            if "NOT_MOUNTED" in mount_info or not mount_info:
                print(f"    ✗ {hostname}: Mount failed - volume not mounted")
            else:
                print(f"    ⚠ {hostname}: Mount status unclear - {mount_info[:80]}")
    
    print("✓ GlusterFS distributed storage setup complete")
    print(f"  Volume: {volume_name}")
    print(f"  Mount point: {mount_point} on all nodes")
    print(f"  Replication: {replica_count}x")
    return True


def setup_container_base(container_cfg, cfg, privileged=False):
    """Common container setup: create, start, configure network, user, SSH, DNS, apt"""
    proxmox_host = cfg['proxmox_host']
    container_id = container_cfg['id']
    ip_address = container_cfg['ip_address']
    hostname = container_cfg['hostname']
    gateway = cfg['gateway']
    template_name = container_cfg.get('template', 'ubuntu-tmpl')
    
    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    # Get template path
    template_path = get_template_path(template_name, cfg)
    
    # Get container resources
    resources = container_cfg.get('resources', {})
    if not resources:
        # Fallback to container_resources for backward compatibility
        container_name = container_cfg['name']
        resources = cfg.get('container_resources', {}).get(container_name, {})
    storage = cfg['proxmox_storage']
    bridge = cfg['proxmox_bridge']
    
    # Create container
    print(f"Creating container {container_id} from template...")
    unprivileged = 0 if privileged else 1
    ssh_exec(proxmox_host,
             f"pct create {container_id} {template_path} "
             f"--hostname {hostname} "
             f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
             f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
             f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged {unprivileged} --ostype ubuntu --arch amd64",
             check=True, cfg=cfg)
    
    # Start container
    print("Starting container...")
    ssh_exec(proxmox_host, f"pct start {container_id}", check=True, cfg=cfg)
    time.sleep(cfg['waits']['container_startup'])
    
    # Configure network
    print("Configuring network...")
    pct_exec(proxmox_host, container_id,
             f"ip link set eth0 up && ip addr add {ip_address}/24 dev eth0 2>/dev/null || true && ip route add default via {gateway} dev eth0 2>/dev/null || true && sleep 2",
             check=False, cfg=cfg)
    time.sleep(cfg['waits']['network_config'])
    
    # Wait for container
    if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
        print("WARNING: Container may not be fully ready, but continuing...")
    
    # Create user and configure sudo
    default_user = cfg['users']['default_user']
    sudo_group = cfg['users']['sudo_group']
    print("Creating user and configuring sudo...")
    pct_exec(proxmox_host, container_id,
             f"useradd -m -s /bin/bash -G {sudo_group} {default_user} 2>/dev/null || echo User exists; "
             f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' | tee /etc/sudoers.d/{default_user}; "
             f"chmod 440 /etc/sudoers.d/{default_user}; "
             f"mkdir -p /home/{default_user}/.ssh; chown -R {default_user}:{default_user} /home/{default_user}; chmod 700 /home/{default_user}/.ssh",
             check=False, cfg=cfg)
    
    # Setup SSH key
    print("Setting up SSH key...")
    setup_ssh_key(proxmox_host, container_id, ip_address, cfg)
    
    # Configure DNS
    print("Configuring DNS...")
    dns_servers = cfg['dns']['servers']
    dns_cmd = " && ".join([f"echo 'nameserver {dns}' >> /etc/resolv.conf" for dns in dns_servers])
    pct_exec(proxmox_host, container_id,
             f"echo 'nameserver {dns_servers[0]}' > /etc/resolv.conf && {dns_cmd.replace(dns_servers[0], '', 1).lstrip(' && ')}",
             check=False, cfg=cfg)
    
    # Fix apt sources
    print("Fixing apt sources...")
    fix_sources_cmd = (
        "if grep -q oracular /etc/apt/sources.list; then "
        "sed -i 's/oracular/plucky/g' /etc/apt/sources.list && "
        "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true && "
        "sed -i 's/plucky main/plucky main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/plucky-updates main/plucky-updates main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/plucky-security main/plucky-security main universe multiverse/g' /etc/apt/sources.list; "
        "elif grep -q noble /etc/apt/sources.list; then "
        "sed -i 's/noble main/noble main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/noble-updates main/noble-updates main universe multiverse/g' /etc/apt/sources.list && "
        "sed -i 's/noble-security main/noble-security main universe multiverse/g' /etc/apt/sources.list; "
        "fi"
    )
    pct_exec(proxmox_host, container_id, fix_sources_cmd, check=False)
    
    # Configure apt cache (if apt-cache container exists)
    apt_cache_containers = [c for c in cfg['containers'] if c['type'] == 'apt-cache']
    if apt_cache_containers:
        apt_cache_ip = apt_cache_containers[0]['ip_address']
        apt_cache_port = cfg['apt_cache_port']
        print("Configuring apt cache...")
        pct_exec(proxmox_host, container_id,
                 f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true",
                 check=False, cfg=cfg)
    
    return container_id


def create_container_pgsql(container_cfg, cfg):
    """Create PostgreSQL container - method for type 'pgsql'"""
    proxmox_host = cfg['proxmox_host']
    container_id = setup_container_base(container_cfg, cfg, privileged=False)
    
    params = container_cfg.get('params', {})
    postgresql_version = params.get('version', '17')
    postgresql_port = params.get('port', 5432)
    data_dir = params.get('data_dir', '/var/lib/postgresql/data')
    
    # Update and upgrade (already done in setup_container_base, but ensure packages are up to date)
    print("Updating package lists...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt update -y 2>&1 | tail -10",
             check=False)
    
    print("Upgrading to latest Ubuntu distribution (25.04)...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt dist-upgrade -y 2>&1 | tail -10",
             check=False)
    
    # Install PostgreSQL
    print(f"Installing PostgreSQL {postgresql_version}...")
    pct_exec(proxmox_host, container_id,
             f"DEBIAN_FRONTEND=noninteractive apt install -y postgresql-{postgresql_version} postgresql-contrib 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Configure PostgreSQL
    print("Configuring PostgreSQL...")
    pct_exec(proxmox_host, container_id,
             f"systemctl enable postgresql && systemctl start postgresql",
             check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['service_start'])
    
    # Configure PostgreSQL to listen on all interfaces
    print("Configuring PostgreSQL network settings...")
    pct_exec(proxmox_host, container_id,
             f"sed -i \"s/#listen_addresses = 'localhost'/listen_addresses = '*'/\" /etc/postgresql/{postgresql_version}/main/postgresql.conf 2>/dev/null || "
             f"sed -i \"s/listen_addresses = 'localhost'/listen_addresses = '*'/\" /etc/postgresql/{postgresql_version}/main/postgresql.conf 2>/dev/null || true",
             check=False, cfg=cfg)
    
    # Update pg_hba.conf to allow connections
    pct_exec(proxmox_host, container_id,
             f"echo 'host all all 10.11.3.0/24 md5' >> /etc/postgresql/{postgresql_version}/main/pg_hba.conf 2>/dev/null || true",
             check=False, cfg=cfg)
    
    # Restart PostgreSQL
    pct_exec(proxmox_host, container_id,
             "systemctl restart postgresql",
             check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['service_start'])
    
    # Verify PostgreSQL is running
    pg_check = pct_exec(proxmox_host, container_id,
                       "systemctl is-active postgresql 2>/dev/null || echo inactive",
                       check=False, capture_output=True, cfg=cfg)
    if "active" in pg_check:
        print("✓ PostgreSQL installed and running")
    else:
        print("⚠ PostgreSQL may not be running")
    
    print(f"✓ PostgreSQL container '{container_cfg['name']}' created")
    return True


def create_container_haproxy(container_cfg, cfg):
    """Create HAProxy load balancer container - method for type 'haproxy'"""
    proxmox_host = cfg['proxmox_host']
    container_id = setup_container_base(container_cfg, cfg, privileged=True)
    
    params = container_cfg.get('params', {})
    http_port = params.get('http_port', 80)
    https_port = params.get('https_port', 443)
    stats_port = params.get('stats_port', 8404)
    
    # Get Swarm node IPs for backend
    swarm_nodes = cfg['swarm_managers'] + cfg['swarm_workers']
    backend_servers = []
    for i, node in enumerate(swarm_nodes, 1):
        backend_servers.append(f"    server node{i} {node['ip_address']}:80 check")
    
    # Install HAProxy
    print("Installing HAProxy...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt install -y haproxy 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Create HAProxy configuration
    print("Configuring HAProxy...")
    backend_servers_str = "\n".join(backend_servers)
    haproxy_config = f"""global
    log /dev/log local0
    log /dev/log local1 notice
    chroot /var/lib/haproxy
    stats socket /run/haproxy/admin.sock mode 660 level admin
    stats timeout 30s
    user haproxy
    group haproxy
    daemon

defaults
    log global
    mode http
    option httplog
    option dontlognull
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms
    errorfile 400 /etc/haproxy/errors/400.http
    errorfile 403 /etc/haproxy/errors/403.http
    errorfile 408 /etc/haproxy/errors/408.http
    errorfile 500 /etc/haproxy/errors/500.http
    errorfile 502 /etc/haproxy/errors/502.http
    errorfile 503 /etc/haproxy/errors/503.http
    errorfile 504 /etc/haproxy/errors/504.http

# Stats page
listen stats
    bind *:{stats_port}
    stats enable
    stats uri /
    stats refresh 30s
    stats admin if TRUE

# Frontend for HTTP
frontend http_frontend
    bind *:{http_port}
    default_backend swarm_backend

# Frontend for HTTPS
frontend https_frontend
    bind *:{https_port}
    default_backend swarm_backend

# Backend for Docker Swarm nodes
backend swarm_backend
    balance roundrobin
    option httpchk GET /
    http-check expect status 200
{backend_servers_str}
"""
    
    # Write HAProxy config using base64 to avoid quote issues
    import base64
    config_b64 = base64.b64encode(haproxy_config.encode()).decode()
    pct_exec(proxmox_host, container_id,
             f"echo {config_b64} | base64 -d > /etc/haproxy/haproxy.cfg",
             check=False, cfg=cfg)
    
    # Fix systemd service for LXC (disable PrivateNetwork)
    print("Configuring HAProxy systemd service for LXC...")
    pct_exec(proxmox_host, container_id,
             "sed -i 's/PrivateNetwork=.*/PrivateNetwork=no/' /usr/lib/systemd/system/haproxy.service 2>/dev/null || true",
             check=False, cfg=cfg)
    pct_exec(proxmox_host, container_id,
             "systemctl daemon-reload",
             check=False, cfg=cfg)
    
    # Enable and start HAProxy
    print("Starting HAProxy service...")
    pct_exec(proxmox_host, container_id,
             "systemctl enable haproxy && systemctl start haproxy",
             check=False, cfg=cfg)
    
    # If systemd fails, start manually as fallback
    haproxy_check = pct_exec(proxmox_host, container_id,
                            "systemctl is-active haproxy 2>/dev/null || echo inactive",
                            check=False, capture_output=True, cfg=cfg)
    if "inactive" in haproxy_check:
        print("Systemd start failed, starting HAProxy manually...")
        pct_exec(proxmox_host, container_id,
                "haproxy -f /etc/haproxy/haproxy.cfg -D",
                check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['service_start'])
    
    # Verify HAProxy is running
    haproxy_check = pct_exec(proxmox_host, container_id,
                             "systemctl is-active haproxy 2>/dev/null || echo inactive",
                             check=False, capture_output=True, cfg=cfg)
    if "active" in haproxy_check:
        print("✓ HAProxy installed and running")
    else:
        print("⚠ HAProxy may not be running")
    
    print(f"✓ HAProxy container '{container_cfg['name']}' created")
    return True


def create_container_swarm_manager(container_cfg, cfg):
    """Create Swarm manager container - method for type 'swarm-manager'"""
    # Swarm containers are created during deploy_swarm, this is a placeholder
    # Actual deployment happens in deploy_swarm()
    return True


def create_container_swarm_node(container_cfg, cfg):
    """Create Swarm worker node container - method for type 'swarm-node'"""
    # Swarm containers are created during deploy_swarm, this is a placeholder
    # Actual deployment happens in deploy_swarm()
    return True


def deploy_swarm(cfg):
    """Deploy Docker Swarm"""
    proxmox_host = cfg['proxmox_host']
    gateway = cfg['gateway']
    
    # Get all swarm containers (managers + workers)
    swarm_containers = cfg['swarm_managers'] + cfg['swarm_workers']
    
    if not swarm_containers:
        print("ERROR: No swarm containers found in configuration", file=sys.stderr)
        return False
    
    # Get Docker template path
    template_path = get_template_path('docker-tmpl', cfg)
    print(f"Using template: {template_path}")
    
    # Container configs
    containers = []
    for ct in swarm_containers:
        containers.append((ct['id'], ct['hostname'], ct['ip_address']))
    
    # Deploy containers
    for container_id, hostname, ip_address in containers:
        print(f"\nDeploying container {container_id} ({hostname})...")
        
        if container_exists(proxmox_host, container_id, cfg=cfg):
            print(f"ERROR: Container {container_id} already exists", file=sys.stderr)
            return False
        
        # Get container resources
        resources = cfg['container_resources'].get('swarm_nodes', cfg['container_resources'].get('swarm_nodes', {}))
        storage = cfg['proxmox_storage']
        bridge = cfg['proxmox_bridge']
        
        # Create container
        print(f"Creating container {container_id} from template...")
        ssh_exec(proxmox_host,
                f"pct create {container_id} {template_path} "
                f"--hostname {hostname} "
                f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
                f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
                f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged 0 --ostype ubuntu --arch amd64",
                check=True, cfg=cfg)
        
        # Configure features
        print("Configuring container features...")
        ssh_exec(proxmox_host, f"pct set {container_id} --features nesting=1,keyctl=1,fuse=1", check=False, cfg=cfg)
        
        # Configure sysctl for manager
        is_manager = any(ct['id'] == container_id for ct in cfg['swarm_managers'])
        if is_manager:
            print("Configuring LXC container for sysctl access...")
            ssh_exec(proxmox_host, f"pct set {container_id} -lxc.cgroup2.devices.allow 'c 10:200 rwm' 2>/dev/null || true", check=False, cfg=cfg)
            ssh_exec(proxmox_host, f"pct set {container_id} -lxc.mount.auto 'proc:rw sys:rw' 2>/dev/null || true", check=False, cfg=cfg)
        
        # Start container
        print("Starting container...")
        ssh_exec(proxmox_host, f"pct start {container_id}", check=True, cfg=cfg)
        time.sleep(cfg['waits']['container_startup'])
        
        # Wait for container
        wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg)
        
        # Setup SSH key
        print("Setting up SSH key...")
        setup_ssh_key(proxmox_host, container_id, ip_address, cfg)
        
        # Configure apt cache for deployed nodes
        apt_cache_containers = [c for c in cfg['containers'] if c['type'] == 'apt-cache']
        if apt_cache_containers:
            apt_cache_ip = apt_cache_containers[0]['ip_address']
            apt_cache_port = cfg['apt_cache_port']
            print("Configuring apt cache...")
            pct_exec(proxmox_host, container_id,
                    f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true",
                    check=False, cfg=cfg)
        
        # Verify Docker
        print("Verifying Docker installation...")
        pct_exec(proxmox_host, container_id, "docker --version && docker ps 2>&1 | head -5", check=False, cfg=cfg)
        
        # Manager-specific setup
        if is_manager:
            print("Ensuring SSH service is running on manager...")
            pct_exec(proxmox_host, container_id, "systemctl start ssh 2>/dev/null || true", check=False, cfg=cfg)
            print("Configuring sysctl for Docker containers...")
            pct_exec(proxmox_host, container_id,
                    "sysctl -w net.ipv4.ip_unprivileged_port_start=0 2>/dev/null || true; "
                    "echo 'net.ipv4.ip_unprivileged_port_start=0' >> /etc/sysctl.conf 2>/dev/null || true",
                    check=False, cfg=cfg)
            time.sleep(cfg['waits']['network_config'])
        
        print(f"✓ Container {container_id} ({hostname}) deployed successfully")
    
    # Downgrade Docker on manager
    docker_version = cfg['docker']['version']
    docker_repo = cfg['docker']['repository']
    docker_release = cfg['docker']['release']
    docker_ubuntu_release = cfg['docker']['ubuntu_release']
    print(f"\nDowngrading Docker to {docker_version} (fixes Portainer)...")
    pct_exec(proxmox_host, cfg['manager_id'],
            f"bash -c '"
            f"apt-mark unhold docker-ce docker-ce-cli 2>/dev/null; "
            f"curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && "
            f"echo \"deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu {docker_repo} {docker_release}\" > /etc/apt/sources.list.d/docker.list && "
            f"apt-get update -qq && "
            f"apt-get install -y --allow-downgrades docker-ce=5:{docker_version}-1~ubuntu.{docker_ubuntu_release}~{docker_repo} docker-ce-cli=5:{docker_version}-1~ubuntu.{docker_ubuntu_release}~{docker_repo} && "
            f"apt-mark hold docker-ce docker-ce-cli && "
            f"systemctl restart docker'",
            check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['swarm_init'])
    
    # Initialize Swarm
    manager = cfg['swarm_managers'][0] if cfg['swarm_managers'] else None
    if not manager:
        print("ERROR: No manager found in swarm configuration", file=sys.stderr)
        return False
    
    print("\nInitializing Docker Swarm on manager node...")
    swarm_init = pct_exec(proxmox_host, manager['id'],
                         f"docker swarm init --advertise-addr {manager['ip_address']} 2>&1",
                         check=False, capture_output=True, cfg=cfg)
    
    if "already part of a swarm" in swarm_init:
        print("Swarm already initialized, continuing...")
    elif "Error" in swarm_init:
        print("WARNING: Swarm initialization had errors, but continuing...")
    else:
        print("Swarm initialized successfully")
    
    # Get worker join token
    print("Getting worker join token...")
    join_token_output = pct_exec(proxmox_host, manager['id'],
                        "docker swarm join-token worker -q 2>&1",
                        check=False, capture_output=True, cfg=cfg)
    # Extract token - get the last non-empty line that looks like a token
    join_token = ""
    for line in join_token_output.strip().split('\n'):
        line = line.strip()
        if line and len(line) > 20 and not line.startswith('Error') and not line.startswith('Warning'):
            join_token = line
            break
    
    if not join_token:
        print(f"ERROR: Could not get worker join token. Output: {join_token_output}", file=sys.stderr)
        return False
    
    # Set manager to drain
    print("Setting manager node availability to drain...")
    pct_exec(proxmox_host, manager['id'],
            f"docker node update --availability drain {manager['hostname']} 2>&1",
            check=False, cfg=cfg)
    
    # Join workers
    swarm_port = cfg['swarm_port']
    default_user = cfg['users']['default_user']
    manager_ip = manager['ip_address']
    
    for worker in cfg['swarm_workers']:
        worker_ip = worker['ip_address']
        worker_hostname = worker['hostname']
        worker_id = worker['id']
        print(f"Joining {worker_hostname} ({worker_ip}) to swarm...")
        # Use pct_exec
        join_cmd = f'docker swarm join --token {join_token} {manager_ip}:{swarm_port}'
        join_output = pct_exec(proxmox_host, worker_id, join_cmd,
                              check=False, capture_output=True, cfg=cfg)
        
        if "already part of a swarm" in join_output:
            print(f"Node {worker_hostname} already part of swarm")
        elif "This node joined a swarm" in join_output:
            print(f"✓ Node {worker_hostname} joined swarm successfully")
        else:
            print(f"WARNING: Node {worker_hostname} join had issues:")
            print(join_output)
    
    # Verify swarm
    print("\nVerifying swarm status...")
    pct_exec(proxmox_host, manager['id'], "docker node ls 2>&1", check=False, cfg=cfg)
    
    # Install Portainer
    print("\nInstalling Portainer CE...")
    pct_exec(proxmox_host, manager['id'],
            "docker volume create portainer_data 2>/dev/null || true",
            check=False, cfg=cfg)
    
    # Remove existing portainer if it exists
    pct_exec(proxmox_host, manager['id'],
            "docker stop portainer 2>/dev/null || true; docker rm portainer 2>/dev/null || true",
            check=False, cfg=cfg)
    
    portainer_image = cfg['portainer_image']
    portainer_port = cfg['portainer_port']
    print("Creating Portainer container...")
    portainer_cmd = (
        "docker run -d --name portainer --restart=always "
        "--security-opt apparmor=unconfined --network host "
        "-v /var/run/docker.sock:/var/run/docker.sock "
        "-v portainer_data:/data "
        f"{portainer_image} 2>&1"
    )
    pct_exec(proxmox_host, manager['id'], portainer_cmd, check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['portainer_start'])
    
    print("Verifying Portainer is running...")
    portainer_status = pct_exec(proxmox_host, manager['id'],
                               "docker ps --format '{{.Names}} {{.Status}}' | grep portainer || docker ps -a --format '{{.Names}} {{.Status}}' | grep portainer",
                               check=False, capture_output=True, cfg=cfg)
    if portainer_status:
        print(f"Portainer status: {portainer_status}")
    else:
        print("WARNING: Portainer container not found")
    
    # Check Portainer logs if not running
    portainer_running = pct_exec(proxmox_host, cfg['manager_id'],
                                 "docker ps --format '{{.Names}}' | grep -q '^portainer$' && echo yes || echo no",
                                 check=False, capture_output=True, cfg=cfg)
    if "no" in portainer_running:
        print("Portainer failed to start. Checking logs...")
        logs = pct_exec(proxmox_host, cfg['manager_id'],
                       "docker logs portainer 2>&1 | tail -20",
                       check=False, capture_output=True, cfg=cfg)
        if logs:
            print(logs)
    
    print("✓ Docker Swarm deployed")
    return True


def cmd_deploy():
    """Deploy complete lab: apt-cache, templates, and Docker Swarm"""
    cfg = get_config()
    
    print("=" * 50)
    print("Deploying Lab Environment")
    print("=" * 50)
    
    try:
        # Create templates first
        templates = cfg['templates']
        total_steps = len(templates) + 1  # templates + containers (non-swarm) + swarm + glusterfs
        template_step = 1
        
        for template_cfg in templates:
            if not create_template(template_cfg, cfg, template_step, total_steps):
                sys.exit(1)
            template_step += 1
        
        # Create containers (excluding swarm containers which are handled separately)
        containers = cfg['containers']
        non_swarm_containers = [c for c in containers if c['type'] not in ['swarm-manager', 'swarm-node']]
        container_step = template_step
        
        for container_cfg in non_swarm_containers:
            if not create_container(container_cfg, cfg, container_step, total_steps):
                sys.exit(1)
            container_step += 1
        
        # Deploy swarm (creates swarm containers)
        swarm_step = container_step
        print(f"\n[{swarm_step}/{total_steps}] Deploying Docker Swarm...")
        if not deploy_swarm(cfg):
            sys.exit(1)
        
        # Setup GlusterFS
        gluster_step = swarm_step + 1
        print(f"\n[{gluster_step}/{total_steps}] Setting up GlusterFS distributed storage...")
        if not setup_glusterfs(cfg):
            sys.exit(1)
        
        print("\n" + "=" * 50)
        print("Deployment Complete!")
        print("=" * 50)
        print(f"\nContainers:")
        for ct in containers:
            print(f"  - {ct['id']}: {ct['name']} ({ct['ip_address']})")
        
        # Show services
        manager = cfg['swarm_managers'][0] if cfg['swarm_managers'] else None
        if manager:
            print(f"\nPortainer: https://{manager['ip_address']}:{cfg['portainer_port']}")
        
        pgsql_containers = [c for c in containers if c['type'] == 'pgsql']
        if pgsql_containers:
            pgsql = pgsql_containers[0]
            params = pgsql.get('params', {})
            print(f"PostgreSQL: {pgsql['ip_address']}:{params.get('port', 5432)}")
        
        haproxy_containers = [c for c in containers if c['type'] == 'haproxy']
        if haproxy_containers:
            haproxy = haproxy_containers[0]
            params = haproxy.get('params', {})
            print(f"HAProxy: http://{haproxy['ip_address']}:{params.get('http_port', 80)} (Stats: http://{haproxy['ip_address']}:{params.get('stats_port', 8404)})")
        if cfg.get('glusterfs'):
            gluster_cfg = cfg['glusterfs']
            print(f"\nGlusterFS:")
            print(f"  Volume: {gluster_cfg.get('volume_name', 'swarm-storage')}")
            print(f"  Mount: {gluster_cfg.get('mount_point', '/mnt/gluster')} on all nodes")
        
    except Exception as e:
        print(f"Error during deployment: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def cmd_cleanup():
    """Remove all containers and templates"""
    cfg = get_config()
    
    print("=" * 50)
    print("Cleaning Up Lab Environment")
    print("=" * 50)
    
    response = input("\nThis will destroy ALL containers and templates. Are you sure? (yes/no): ")
    if response.lower() in ['yes', 'y']:
        print("\nStopping and destroying containers...")
        
        # Get all container IDs
        result = ssh_exec(cfg["proxmox_host"],
                         "pct list | awk 'NR>1{print $1}'",
                         check=False, capture_output=True, cfg=cfg)
        
        if result:
            container_ids = [cid.strip().split()[0] for cid in result.split('\n') if cid.strip()]
            for cid in container_ids:
                if cid.isdigit():  # Only process valid numeric IDs
                    print(f"  Destroying CT {cid}...")
                    destroy_container(cfg["proxmox_host"], cid, cfg=cfg)
        
        print("\nRemoving templates...")
        template_dir = cfg['proxmox_template_dir']
        ssh_exec(cfg["proxmox_host"],
                f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -delete || true",
                check=False, cfg=cfg)
        
        print("\n" + "=" * 50)
        print("Cleanup Complete!")
        print("=" * 50)
    else:
        print("Cleanup cancelled.")


def cmd_status():
    """Show current lab status"""
    cfg = get_config()
    
    print("=" * 50)
    print("Lab Status")
    print("=" * 50)
    
    # Check containers
    print("\nContainers:")
    result = ssh_exec(cfg["proxmox_host"], "pct list", check=False, capture_output=True, cfg=cfg)
    if result:
        print(result)
    else:
        print("  No containers found")
    
    # Check templates
    template_dir = cfg['proxmox_template_dir']
    print("\nTemplates:")
    result = ssh_exec(cfg["proxmox_host"],
                     f"ls -lh {template_dir}/*.tar.zst 2>/dev/null || echo 'No templates'",
                     check=False, capture_output=True, cfg=cfg)
    if result:
        print(result)
    else:
        print("  No templates found")
    
    # Check swarm status
    print("\nDocker Swarm:")
    result = pct_exec(cfg["proxmox_host"], cfg["manager_id"],
                    "docker node ls 2>/dev/null || echo 'Swarm not initialized or manager not available'",
                    check=False, capture_output=True, cfg=cfg)
    if result:
        print(result)
    else:
        print("  Swarm not available")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Lab CLI - Manage Proxmox LXC containers and Docker Swarm",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    deploy_parser = subparsers.add_parser('deploy', help='Deploy complete lab: apt-cache, templates, and Docker Swarm')
    deploy_parser.set_defaults(func=cmd_deploy)
    
    cleanup_parser = subparsers.add_parser('cleanup', help='Remove all containers and templates')
    cleanup_parser.set_defaults(func=cmd_cleanup)
    
    status_parser = subparsers.add_parser('status', help='Show current lab status')
    status_parser.set_defaults(func=cmd_status)
    
    args = parser.parse_args()
    
    if hasattr(args, 'func'):
        args.func()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
