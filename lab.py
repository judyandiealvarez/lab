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

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

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
            return config
        else:
            print("Error: PyYAML is required. Install it with: pip install pyyaml", file=sys.stderr)
            sys.exit(1)
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
            manager_id = mgr_id['id'] if isinstance(mgr_id, dict) else mgr_id
        for ct in containers:
                if ct['id'] == manager_id:
                    swarm_managers.append(ct)
                    break
    if 'swarm' in config and 'workers' in config['swarm']:
        for worker_id in config['swarm']['workers']:
            worker_id_val = worker_id['id'] if isinstance(worker_id, dict) else worker_id
        for ct in containers:
                if ct['id'] == worker_id_val:
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
        'glusterfs': config.get('glusterfs', {}),
        'apt-cache-ct': config.get('apt-cache-ct', 'apt-cache')
    }


def ssh_exec(host, command, check=True, capture_output=False, timeout=None, cfg=None):
    """Execute command via SSH using paramiko if available, fallback to subprocess"""
    if HAS_PARAMIKO and cfg:
        try:
            # Parse host (format: user@host or just host)
            if '@' in host:
                username, hostname = host.split('@', 1)
            else:
                username = 'root'
                hostname = host
            
            connect_timeout = cfg.get('ssh', {}).get('connect_timeout', 10)
            exec_timeout = timeout if timeout else 300
            
            # Create SSH client
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect
            client.connect(
                hostname=hostname,
                username=username,
                timeout=connect_timeout,
                look_for_keys=True,
                allow_agent=True
            )
            
            # Execute command
            stdin, stdout, stderr = client.exec_command(command, timeout=exec_timeout)
            
            # Get exit status
            exit_status = stdout.channel.recv_exit_status()
            
            if capture_output:
                output = stdout.read().decode('utf-8').strip()
                error_output = stderr.read().decode('utf-8').strip()
                client.close()
                if exit_status != 0 and check:
                    raise subprocess.CalledProcessError(exit_status, command, output, error_output)
                return output
            else:
                # For non-capture mode, read output to prevent buffer issues
                stdout.read()
                stderr.read()
                client.close()
                if exit_status != 0 and check:
                    raise subprocess.CalledProcessError(exit_status, command)
                return exit_status == 0
                
        except paramiko.SSHException as e:
            if capture_output:
                return None
            if check:
                raise
            return False
        except Exception as e:
            # Fallback to subprocess if paramiko fails
            if capture_output:
                pass  # Will fall through to subprocess
            else:
                if check:
                    raise
                return False
    
    # Fallback to subprocess if paramiko not available or failed
    connect_timeout = cfg['ssh']['connect_timeout'] if cfg and 'ssh' in cfg else 10
    batch_mode = 'yes' if (cfg and cfg.get('ssh', {}).get('batch_mode', True)) else 'no'
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
        if capture_output:
            return None
        return False
    except subprocess.CalledProcessError:
        if capture_output:
            return None
        return False


def pct_exec(proxmox_host, container_id, command, check=True, capture_output=False, timeout=30, cfg=None):
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
        text=True,
        timeout=timeout
        )
        if capture_output:
            return result.stdout.strip()
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        if capture_output:
            return None
        return False
    except subprocess.CalledProcessError:
        if capture_output:
            return None
        return False


def container_exists(proxmox_host, container_id, cfg=None):
    """Check if container exists"""
    container_id_str = str(container_id)
    result = ssh_exec(proxmox_host, f"pct list | grep '^{container_id_str} '", check=False, capture_output=True, cfg=cfg)
    return result is not None and container_id_str in result


def destroy_container(proxmox_host, container_id, cfg=None):
    """Destroy container if it exists"""
    # Check if container exists
    container_id_str = str(container_id)
    check_result = ssh_exec(proxmox_host, f"pct list | grep '^{container_id_str} '", check=False, capture_output=True, cfg=cfg)
    if not check_result or container_id_str not in check_result:
        print(f"  Container {container_id} does not exist, skipping", flush=True)
        return
    
    print(f"  Stopping container {container_id}...", flush=True)
    ssh_exec(proxmox_host, f"pct stop {container_id} 2>/dev/null || true", check=False, cfg=cfg)
    time.sleep(2)  # Give it time to stop
    
    print(f"  Destroying container {container_id}...", flush=True)
    destroy_result = ssh_exec(proxmox_host, f"pct destroy {container_id} 2>&1", check=False, capture_output=True, cfg=cfg)
    
    # Verify it's actually destroyed
    verify_result = ssh_exec(proxmox_host, f"pct list | grep '^{container_id_str} '", check=False, capture_output=True, cfg=cfg)
    if verify_result and container_id_str in verify_result:
        print(f"  ⚠ Container {container_id_str} still exists, forcing destruction...", flush=True)
        ssh_exec(proxmox_host, f"pct destroy {container_id_str} --force 2>&1 || true", check=False, cfg=cfg)
        time.sleep(1)
    
    # Final verification
    final_check = ssh_exec(proxmox_host, f"pct list | grep '^{container_id_str} '", check=False, capture_output=True, cfg=cfg)
    if not final_check or container_id_str not in final_check:
        print(f"  ✓ Container {container_id_str} destroyed", flush=True)
    else:
        print(f"  ✗ Container {container_id_str} still exists after destruction attempt", flush=True)


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
                test_result = pct_exec(proxmox_host, container_id, "echo test", check=False, capture_output=True, timeout=5, cfg=cfg)
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
        check_result = ssh_exec(proxmox_host, f"test -f {template_dir}/{template} && echo exists || echo missing", check=False, capture_output=True, cfg=cfg)
        if check_result and "exists" in check_result:
            return template
    
    # Download last template in list
    template_to_download = templates[-1]
    print(f"Base template not found. Downloading {template_to_download}...", flush=True)
    
    # Run pveam download with live output (no capture_output so we see progress)
    download_cmd = f"pveam download local {template_to_download}"
    print(f"  Running: {download_cmd}", flush=True)
    # Use timeout of 300 seconds (5 minutes) for download
    ssh_exec(proxmox_host, download_cmd, check=False, capture_output=False, timeout=300, cfg=cfg)
    
    # Verify download completed
    verify_result = ssh_exec(proxmox_host, f"test -f {template_dir}/{template_to_download} && echo exists || echo missing", check=False, capture_output=True, cfg=cfg)
    if not verify_result or "exists" not in verify_result:
        print(f"ERROR: Template {template_to_download} was not downloaded successfully", file=sys.stderr)
        return None
    
    print(f"  ✓ Template {template_to_download} downloaded successfully", flush=True)
    return template_to_download


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
    
    # If template_name is None, use base template directly
    if template_name is None:
        base_template = get_base_template(proxmox_host, cfg)
        return f"{template_dir}/{base_template}"
    
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
    pct_exec(proxmox_host, container_id, fix_sources_cmd, check=False, cfg=cfg)
    
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
    # Get apt-cache IP from containers (may not exist yet during template creation)
    apt_cache_containers = [c for c in cfg.get('containers', []) if c.get('type') == 'apt-cache']
    apt_cache_ip = apt_cache_containers[0]['ip_address'] if apt_cache_containers else None
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
    
    # Configure apt cache FIRST before any apt operations (if apt-cache exists)
    if apt_cache_ip:
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
    # Get apt-cache IP from containers (may not exist yet during template creation)
    apt_cache_containers = [c for c in cfg.get('containers', []) if c.get('type') == 'apt-cache']
    apt_cache_ip = apt_cache_containers[0]['ip_address'] if apt_cache_containers else None
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
         "sed -i 's/oracular/plucky/g' /etc/apt/sources.list 2>/dev/null || true; "
         "sed -i 's|old-releases.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list 2>/dev/null || true; "
         "if ! grep -q '^deb.*plucky.*main' /etc/apt/sources.list; then "
         "echo 'deb http://archive.ubuntu.com/ubuntu plucky main universe multiverse' > /etc/apt/sources.list; "
         "echo 'deb http://archive.ubuntu.com/ubuntu plucky-updates main universe multiverse' >> /etc/apt/sources.list; "
         "echo 'deb http://archive.ubuntu.com/ubuntu plucky-security main universe multiverse' >> /etc/apt/sources.list; "
         "fi",
             check=False, cfg=cfg)
    
    # Update packages - remove proxy first to avoid connection issues
    print("Updating package lists...")
    pct_exec(proxmox_host, container_id,
         "rm -f /etc/apt/apt.conf.d/01proxy",
         check=False, cfg=cfg)
    
    # Try update without proxy first
    update_result = pct_exec(proxmox_host, container_id,
                            f"DEBIAN_FRONTEND=noninteractive apt update -y 2>&1",
                            check=False, capture_output=True, cfg=cfg)
    
    # If update fails and we have apt-cache, try with proxy
    if apt_cache_ip and ("Failed to fetch" in update_result or "Unable to connect" in update_result):
        print("  Update failed, trying with apt-cache proxy...")
    pct_exec(proxmox_host, container_id,
             f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true; "
             f"DEBIAN_FRONTEND=noninteractive apt update -y 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Install prerequisites - try without proxy first
    print("Installing prerequisites...")
    install_result = pct_exec(proxmox_host, container_id,
                             "DEBIAN_FRONTEND=noninteractive apt install -y curl apt-transport-https ca-certificates software-properties-common gnupg lsb-release 2>&1",
                             check=False, capture_output=True, cfg=cfg)
    
    # If install fails, remove proxy and try again
    if install_result and ("Unable to locate package" in install_result or "Failed to fetch" in install_result):
        print("  Install failed, removing proxy and retrying...")
        pct_exec(proxmox_host, container_id,
                 "rm -f /etc/apt/apt.conf.d/01proxy; "
                 "DEBIAN_FRONTEND=noninteractive apt update -qq && "
                 "DEBIAN_FRONTEND=noninteractive apt install -y curl apt-transport-https ca-certificates software-properties-common gnupg lsb-release 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Upgrade
    print("Upgrading to latest Ubuntu distribution (25.04)...")
    pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt dist-upgrade -y 2>&1 | tail -10",
             check=False, cfg=cfg)
    
    # Install Docker - remove proxy first to avoid connection issues
    print("Installing Docker...")
    pct_exec(proxmox_host, container_id,
         "rm -f /etc/apt/apt.conf.d/01proxy",
             check=False, cfg=cfg)
    
    docker_install_script = (
        "rm -f /etc/apt/apt.conf.d/01proxy; "
        "DEBIAN_FRONTEND=noninteractive apt update -qq 2>&1 && "
        "if command -v curl >/dev/null 2>&1; then "
        "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh 2>&1 && sh /tmp/get-docker.sh 2>&1 | tail -10 || "
        "  (echo 'get.docker.com failed, trying docker.io...' && DEBIAN_FRONTEND=noninteractive apt install -y docker.io containerd.io 2>&1 | tail -20); "
        "else "
        "  echo 'curl not available, installing docker.io...'; "
        "  DEBIAN_FRONTEND=noninteractive apt install -y docker.io containerd.io 2>&1 | tail -20; "
        "fi"
    )
    docker_result = pct_exec(proxmox_host, container_id, docker_install_script, check=False, capture_output=True, cfg=cfg)
    
    # Verify Docker installation
    print("Verifying Docker install...")
    docker_check = pct_exec(proxmox_host, container_id,
                           "command -v docker >/dev/null 2>&1 && docker --version || echo 'docker_not_found'",
                           check=False, capture_output=True, cfg=cfg)
    
    if "docker_not_found" in docker_check or "docker" not in docker_check.lower():
        print("Docker not found, installing docker.io directly...")
    pct_exec(proxmox_host, container_id,
                "rm -f /etc/apt/apt.conf.d/01proxy; "
                "DEBIAN_FRONTEND=noninteractive apt update -qq && "
                "DEBIAN_FRONTEND=noninteractive apt install -y docker.io containerd.io 2>&1 | tail -20",
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
    swarm_manager_configs = [c for c in cfg['containers'] if c['type'] == 'swarm-manager']
    swarm_worker_configs = [c for c in cfg['containers'] if c['type'] == 'swarm-node']
    
    if not swarm_manager_configs or not swarm_worker_configs:
        print("ERROR: Swarm managers or workers not found", file=sys.stderr)
        return False
    
    manager = swarm_manager_configs[0]
    manager_node = (manager['id'], manager['hostname'], manager['ip_address'])
    worker_nodes = [(w['id'], w['hostname'], w['ip_address']) for w in swarm_worker_configs]
    # All nodes for mounting, but only workers for storage bricks
    all_nodes = [manager_node] + worker_nodes
    
    # Install GlusterFS server on all nodes (manager for management, workers for storage)
    print("Installing GlusterFS server on all nodes...")
    apt_cache_containers = [c for c in cfg.get('containers', []) if c.get('type') == 'apt-cache']
    apt_cache_ip = apt_cache_containers[0]['ip_address'] if apt_cache_containers else None
    apt_cache_port = cfg['apt_cache_port'] if apt_cache_ip else None
    
    # First, ensure apt sources are correct on all nodes
    for container_id, hostname, ip_address in all_nodes:
        print(f"  Fixing apt sources on {hostname}...")
        pct_exec(proxmox_host, container_id,
                "sed -i 's/oracular/plucky/g' /etc/apt/sources.list 2>/dev/null || true; "
                "if ! grep -q '^deb.*plucky.*main' /etc/apt/sources.list; then "
                "echo 'deb http://archive.ubuntu.com/ubuntu plucky main universe multiverse' > /etc/apt/sources.list; "
                "echo 'deb http://archive.ubuntu.com/ubuntu plucky-updates main universe multiverse' >> /etc/apt/sources.list; "
                "echo 'deb http://archive.ubuntu.com/ubuntu plucky-security main universe multiverse' >> /etc/apt/sources.list; "
                "fi",
                check=False, cfg=cfg)
    
    for container_id, hostname, ip_address in all_nodes:
        print(f"  Installing on {hostname}...", flush=True)
        
        # Try with apt-cache first, then without if it fails
        install_success = False
        max_retries = 2
        
        for attempt in range(1, max_retries + 1):
            if attempt == 1 and apt_cache_ip and apt_cache_port:
                # Try with apt-cache
                print(f"    Attempt {attempt}: Using apt-cache proxy...", flush=True)
                pct_exec(proxmox_host, container_id,
                        f"echo 'Acquire::http::Proxy \"http://{apt_cache_ip}:{apt_cache_port}\";' > /etc/apt/apt.conf.d/01proxy || true",
                        check=False, timeout=10, cfg=cfg)
            else:
                # Remove proxy and try without
                print(f"    Attempt {attempt}: Removing proxy and trying direct...", flush=True)
                pct_exec(proxmox_host, container_id,
                        "rm -f /etc/apt/apt.conf.d/01proxy",
                        check=False, timeout=10, cfg=cfg)
            
            # Update package lists
            print(f"    Updating package lists...", flush=True)
            update_result = pct_exec(proxmox_host, container_id,
                    "DEBIAN_FRONTEND=noninteractive apt-get update -qq 2>&1",
                    check=False, capture_output=True, timeout=120, cfg=cfg)
            
            if update_result and ("Failed to fetch" in update_result or "Unable to connect" in update_result):
                print(f"    ⚠ apt update failed, will retry without proxy...", flush=True)
                if attempt < max_retries:
                    continue
            
            # Install GlusterFS
            print(f"    Installing glusterfs-server and glusterfs-client...", flush=True)
            install_output = pct_exec(proxmox_host, container_id,
                    "DEBIAN_FRONTEND=noninteractive apt-get install -y glusterfs-server glusterfs-client 2>&1",
                    check=False, capture_output=True, timeout=300, cfg=cfg)
            
            # Verify installation
            verify_gluster = pct_exec(proxmox_host, container_id,
                    "command -v gluster >/dev/null 2>&1 && echo installed || echo not_installed",
                    check=False, capture_output=True, timeout=10, cfg=cfg)
            
            if verify_gluster and "installed" in verify_gluster:
                print(f"    ✓ GlusterFS installed successfully", flush=True)
                install_success = True
                break
            else:
                if install_output:
                    error_msg = install_output[-500:] if len(install_output) > 500 else install_output
                    print(f"    ⚠ Installation attempt {attempt} failed: {error_msg[-200:]}", flush=True)
                if attempt < max_retries:
                    print(f"    Retrying without proxy...", flush=True)
                    time.sleep(2)
        
        if not install_success:
            print(f"    ✗ Failed to install GlusterFS on {hostname} after {max_retries} attempts", flush=True)
            return False
        
        # Start and enable glusterd
        print(f"    Starting glusterd service...", flush=True)
        pct_exec(proxmox_host, container_id,
                "systemctl enable glusterd 2>/dev/null && systemctl start glusterd 2>/dev/null",
                check=False, timeout=30, cfg=cfg)
        
        # Verify glusterd is running
        time.sleep(3)
        glusterd_check = pct_exec(proxmox_host, container_id,
                "systemctl is-active glusterd 2>/dev/null || echo 'inactive'",
                check=False, capture_output=True, timeout=10, cfg=cfg)
        
        if glusterd_check and "active" in glusterd_check:
            print(f"    ✓ {hostname}: GlusterFS installed and glusterd running", flush=True)
        else:
            print(f"    ⚠ {hostname}: GlusterFS installed but glusterd may not be running", flush=True)
    
    time.sleep(cfg['waits']['glusterfs_setup'])
    
    # Create brick directories (only on worker nodes)
    print("Creating brick directories on worker nodes...")
    for worker in swarm_worker_configs:
        container_id = worker['id']
        hostname = worker['hostname']
        print(f"  Creating brick on {hostname}...")
        pct_exec(proxmox_host, container_id,
                f"mkdir -p {brick_path} && chmod 755 {brick_path}",
                check=False, cfg=cfg)
    
    # Peer nodes together (from manager)
    manager_id = manager['id']
    manager_hostname = manager['hostname']
    manager_ip = manager['ip_address']
    
    print("Peering worker nodes together...")
    for worker in swarm_worker_configs:
        container_id = worker['id']
        hostname = worker['hostname']
        ip_address = worker['ip_address']
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
        if connected_count >= len(swarm_worker_configs):  # All workers connected
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
        brick_list = " ".join([f"{w['ip_address']}:{brick_path}" for w in swarm_worker_configs])
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
    for node in [manager] + swarm_worker_configs:
        container_id = node['id']
        hostname = node['hostname']
        ip_address = node['ip_address']
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
                f"mount -t glusterfs {manager['ip_address']}:/{volume_name} {mount_point} 2>&1",
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
    
    # Try to create container - the tar errors for postfix dev files are non-fatal
    create_result = ssh_exec(proxmox_host,
         f"pct create {container_id} {template_path} "
         f"--hostname {hostname} "
         f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
         f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
         f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged {unprivileged} --ostype ubuntu --arch amd64 2>&1",
                            check=False, capture_output=True, cfg=cfg)
    
    # Check if container was actually created despite tar warnings
    # Tar errors for postfix dev files are non-fatal - check if container config exists
    config_check = ssh_exec(proxmox_host,
                           f"test -f /etc/pve/lxc/{container_id}.conf && echo exists || echo missing",
                           check=False, capture_output=True, timeout=10, cfg=cfg)
    
    if not config_check or "missing" in config_check:
        # Container was not created - check if it's due to tar errors
        if "tar:" in create_result and "Cannot mknod" in create_result:
            # Try to create container again with --skip-old-files or ignore tar errors
            print(f"  ⚠ Container creation had tar errors, retrying with error tolerance...", flush=True)
            # Wait a moment for cleanup
            time.sleep(2)
            # Try creating again - sometimes it works on retry
            retry_result = ssh_exec(proxmox_host,
                 f"pct create {container_id} {template_path} "
                 f"--hostname {hostname} "
                 f"--memory {resources['memory']} --swap {resources['swap']} --cores {resources['cores']} "
                 f"--net0 name=eth0,bridge={bridge},firewall=1,gw={gateway},ip={ip_address}/24,ip6=dhcp,type=veth "
                 f"--rootfs {storage}:{resources['rootfs_size']} --unprivileged {unprivileged} --ostype ubuntu --arch amd64 2>&1",
                            check=False, capture_output=True, cfg=cfg)
            # Check again
            config_check = ssh_exec(proxmox_host,
                               f"test -f /etc/pve/lxc/{container_id}.conf && echo exists || echo missing",
                               check=False, capture_output=True, timeout=10, cfg=cfg)
            if not config_check or "missing" in config_check:
                print(f"ERROR: Container {container_id} creation failed after retry", file=sys.stderr)
                print(f"Error output: {retry_result[-500:] if retry_result else create_result[-500:]}", file=sys.stderr)
                return False
        else:
            # Other error - fail immediately
            print(f"ERROR: Container {container_id} creation failed", file=sys.stderr)
            if create_result:
                print(f"Error output: {create_result[-500:]}", file=sys.stderr)
            return False
    else:
        # Container config exists - tar errors were non-fatal
        if "tar:" in create_result or "Cannot mknod" in create_result:
            print(f"  ⚠ Non-fatal tar errors during container creation (postfix dev files)", flush=True)
    
    # Verify container exists
    if not container_exists(proxmox_host, container_id, cfg=cfg):
        print(f"ERROR: Container {container_id} was not created", file=sys.stderr)
        return False
    
    # Start container
    print("Starting container...")
    start_result = ssh_exec(proxmox_host, f"pct start {container_id} 2>&1", check=False, capture_output=True, cfg=cfg)
    if start_result and ("error" in start_result.lower() or "failed" in start_result.lower() or "not found" in start_result.lower()):
        print(f"ERROR: Failed to start container {container_id}: {start_result}", file=sys.stderr)
        return False
    time.sleep(cfg['waits']['container_startup'])
    
    # Verify container is actually running before trying to exec
    status_check = ssh_exec(proxmox_host, f"pct status {container_id} 2>&1", check=False, capture_output=True, cfg=cfg)
    if not status_check or "running" not in status_check:
        print(f"ERROR: Container {container_id} is not running after start. Status: {status_check}", file=sys.stderr)
        return False
    
    # Configure network
    print("Configuring network...")
    pct_exec(proxmox_host, container_id,
         f"ip link set eth0 up && ip addr add {ip_address}/24 dev eth0 2>/dev/null || true && ip route add default via {gateway} dev eth0 2>/dev/null || true && sleep 2",
         check=False, timeout=10, cfg=cfg)
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
    pct_exec(proxmox_host, container_id, fix_sources_cmd, check=False, cfg=cfg)
    
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
    # Use base template directly to avoid tar errors from custom template
    original_template = container_cfg.get('template')
    container_cfg['template'] = None  # Use base template
    container_id = setup_container_base(container_cfg, cfg, privileged=False)
    # Restore original template setting
    if original_template:
        container_cfg['template'] = original_template
    if not container_id:
        print(f"ERROR: Failed to create container {container_cfg['id']}", file=sys.stderr)
        return False
    
    params = container_cfg.get('params', {})
    postgresql_version = params.get('version', '17')
    postgresql_port = params.get('port', 5432)
    data_dir = params.get('data_dir', '/var/lib/postgresql/data')
    
    # Update and upgrade (already done in setup_container_base, but ensure packages are up to date)
    print("Updating package lists...", flush=True)
    pct_exec(proxmox_host, container_id,
         "DEBIAN_FRONTEND=noninteractive apt update -y 2>&1 | tail -10",
         check=False, timeout=120, cfg=cfg)
    
    print("Upgrading to latest Ubuntu distribution (25.04)...", flush=True)
    pct_exec(proxmox_host, container_id,
         "DEBIAN_FRONTEND=noninteractive apt dist-upgrade -y 2>&1 | tail -10",
         check=False, timeout=300, cfg=cfg)
    
    # Install PostgreSQL
    print(f"Installing PostgreSQL {postgresql_version}...", flush=True)
    pct_exec(proxmox_host, container_id,
         f"DEBIAN_FRONTEND=noninteractive apt install -y postgresql-{postgresql_version} postgresql-contrib 2>&1 | tail -10",
         check=False, timeout=300, cfg=cfg)
    
    # Configure PostgreSQL
    print("Configuring PostgreSQL...", flush=True)
    pct_exec(proxmox_host, container_id,
         f"systemctl enable postgresql && systemctl start postgresql",
         check=False, timeout=30, cfg=cfg)
    
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
    
    # Install HAProxy - Ubuntu 25.04 may not have haproxy in main repo, try universe
    print("Installing HAProxy...")
    # First, fix any dpkg issues
    pct_exec(proxmox_host, container_id,
         "dpkg --configure -a 2>&1 || true",
         check=False, timeout=60, cfg=cfg)
    
    install_result = pct_exec(proxmox_host, container_id,
         "DEBIAN_FRONTEND=noninteractive apt update -qq 2>&1 && "
         "DEBIAN_FRONTEND=noninteractive apt install -y haproxy 2>&1",
         check=False, capture_output=True, timeout=120, cfg=cfg)
    
    # Verify installation
    haproxy_check = pct_exec(proxmox_host, container_id,
                           "command -v haproxy >/dev/null 2>&1 && echo installed || echo not_installed",
                           check=False, capture_output=True, timeout=10, cfg=cfg)
    
    if haproxy_check and "not_installed" in haproxy_check:
        print("  ⚠ haproxy package not found, trying to install from universe...", flush=True)
        # Fix dpkg again before retry
        pct_exec(proxmox_host, container_id,
             "dpkg --configure -a 2>&1 || true",
             check=False, timeout=60, cfg=cfg)
        pct_exec(proxmox_host, container_id,
             "DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends haproxy 2>&1 || "
             "echo 'haproxy installation failed'",
             check=False, timeout=120, cfg=cfg)
        # Check again
        haproxy_check = pct_exec(proxmox_host, container_id,
                               "command -v haproxy >/dev/null 2>&1 && echo installed || echo not_installed",
                               check=False, capture_output=True, timeout=10, cfg=cfg)
        if haproxy_check and "not_installed" in haproxy_check:
            print("  ✗ Failed to install HAProxy", flush=True)
            return False
    
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
    
    # Get swarm container configs from containers list
    swarm_manager_configs = [c for c in cfg['containers'] if c['type'] == 'swarm-manager']
    swarm_worker_configs = [c for c in cfg['containers'] if c['type'] == 'swarm-node']
    
    if not swarm_manager_configs or not swarm_worker_configs:
        print("ERROR: Swarm manager or worker containers not found in configuration", file=sys.stderr)
        return False
    
    # Get Docker template path
    template_path = get_template_path('docker-tmpl', cfg)
    print(f"Using template: {template_path}")
    
    # Deploy all swarm containers (managers + workers)
    all_swarm_configs = swarm_manager_configs + swarm_worker_configs
    
    for container_cfg in all_swarm_configs:
        container_id = container_cfg['id']
        hostname = container_cfg['hostname']
        ip_address = container_cfg['ip_address']
        print(f"\nDeploying container {container_id} ({hostname})...")
        
        # Destroy if exists
        if container_exists(proxmox_host, container_id, cfg=cfg):
            print(f"  Destroying existing container {container_id}...")
            destroy_container(proxmox_host, container_id, cfg=cfg)
        
        # Get container resources from container config
        resources = container_cfg.get('resources', {})
        if not resources:
            # Default fallback
            resources = {'memory': 4096, 'swap': 4096, 'cores': 8, 'rootfs_size': 40}
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
        is_manager = container_cfg['type'] == 'swarm-manager'
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
        docker_verify = pct_exec(proxmox_host, container_id, 
                               "command -v docker >/dev/null 2>&1 && docker --version && docker ps 2>&1 | head -5 || echo 'Docker not found'",
                               check=False, capture_output=True, cfg=cfg)
        if "Docker not found" in docker_verify or "docker" not in docker_verify.lower():
            print("Docker not installed, installing Docker...", flush=True)
            # Use Docker's official installation script
            docker_install_cmd = (
                "rm -f /etc/apt/apt.conf.d/01proxy; "
                "DEBIAN_FRONTEND=noninteractive apt update -qq 2>&1 && "
                "if command -v curl >/dev/null 2>&1; then "
                "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh 2>&1 && "
                "  sh /tmp/get-docker.sh 2>&1 | tail -20 || "
                "  (echo 'get.docker.com failed, trying docker.io...' && "
                "   DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20); "
                "else "
                "  echo 'curl not available, installing docker.io...'; "
                "  DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20; "
                "fi"
            )
            install_result = pct_exec(proxmox_host, container_id, docker_install_cmd,
                                    check=False, capture_output=True, timeout=300, cfg=cfg)
            
            # Verify Docker was installed
            docker_check = pct_exec(proxmox_host, container_id,
                                  "command -v docker >/dev/null 2>&1 && echo installed || echo not_installed",
                                  check=False, capture_output=True, timeout=10, cfg=cfg)
            if docker_check and "installed" in docker_check:
                print("  ✓ Docker installed successfully", flush=True)
            else:
                print("  ⚠ Docker installation may have failed", flush=True)
        
        # Start Docker
        print("  Starting Docker service...", flush=True)
        pct_exec(proxmox_host, container_id,
                    "systemctl enable docker 2>/dev/null && systemctl start docker 2>/dev/null",
                    check=False, timeout=30, cfg=cfg)
        
        # Verify Docker is running
        time.sleep(3)
        docker_status = pct_exec(proxmox_host, container_id,
                               "systemctl is-active docker 2>/dev/null || echo inactive",
                               check=False, capture_output=True, timeout=10, cfg=cfg)
        if docker_status and "active" in docker_status:
            print("  ✓ Docker service is running", flush=True)
        else:
            print("  ⚠ Docker service may not be running", flush=True)
        
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
    
    # Ensure Docker is installed and running on manager (after all containers are created)
    manager_config = swarm_manager_configs[0]
    manager_id = manager_config['id']
    
    # Check if Docker is installed
    docker_check = pct_exec(proxmox_host, manager_id,
                          "command -v docker >/dev/null 2>&1 && echo 'docker_installed' || echo 'docker_missing'",
                          check=False, capture_output=True, cfg=cfg)
    
    if "docker_missing" in docker_check:
        print("\nInstalling Docker on manager...", flush=True)
        docker_install_cmd = (
            "rm -f /etc/apt/apt.conf.d/01proxy; "
            "DEBIAN_FRONTEND=noninteractive apt update -qq 2>&1 && "
            "if command -v curl >/dev/null 2>&1; then "
            "  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh 2>&1 && "
            "  sh /tmp/get-docker.sh 2>&1 | tail -20 || "
            "  (echo 'get.docker.com failed, trying docker.io...' && "
            "   DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20); "
            "else "
            "  echo 'curl not available, installing docker.io...'; "
            "  DEBIAN_FRONTEND=noninteractive apt install -y docker.io 2>&1 | tail -20; "
            "fi"
        )
        install_result = pct_exec(proxmox_host, manager_id, docker_install_cmd,
                                check=False, capture_output=True, timeout=300, cfg=cfg)
        
        # Verify Docker was installed
        docker_check = pct_exec(proxmox_host, manager_id,
                              "command -v docker >/dev/null 2>&1 && echo installed || echo not_installed",
                              check=False, capture_output=True, timeout=10, cfg=cfg)
        if docker_check and "installed" in docker_check:
            print("  ✓ Docker installed successfully", flush=True)
        else:
            print("  ⚠ Docker installation may have failed", flush=True)
    
    # Start Docker service
    print("Starting Docker service on manager...", flush=True)
    pct_exec(proxmox_host, manager_id,
        "systemctl enable docker && systemctl start docker && systemctl status docker --no-pager | head -5",
            check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['swarm_init'])
    
    # Initialize Swarm (use the first manager config)
    manager_config = swarm_manager_configs[0]
    manager_id = manager_config['id']
    manager_ip = manager_config['ip_address']
    manager_hostname = manager_config['hostname']
    
    print("\nInitializing Docker Swarm on manager node...")
    swarm_init = pct_exec(proxmox_host, manager_id,
                         f"docker swarm init --advertise-addr {manager_ip} 2>&1",
                         check=False, capture_output=True, cfg=cfg)
    
    if "already part of a swarm" in swarm_init:
        print("Swarm already initialized, continuing...")
    elif "Error" in swarm_init:
        print("WARNING: Swarm initialization had errors, but continuing...")
    else:
        print("Swarm initialized successfully")
    
    # Get worker join token
    print("Getting worker join token...")
    join_token_output = pct_exec(proxmox_host, manager_id,
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
    pct_exec(proxmox_host, manager_id,
        f"docker node update --availability drain {manager_hostname} 2>&1",
            check=False, cfg=cfg)
    
    # Join workers
    swarm_port = cfg['swarm_port']
    
    for worker_config in swarm_worker_configs:
        worker_ip = worker_config['ip_address']
        worker_hostname = worker_config['hostname']
        worker_id = worker_config['id']
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
    pct_exec(proxmox_host, manager_id, "docker node ls 2>&1", check=False, cfg=cfg)
    
    # Install Portainer
    print("\nInstalling Portainer CE...")
    pct_exec(proxmox_host, manager_id,
            "docker volume create portainer_data 2>/dev/null || true",
            check=False, cfg=cfg)
    
    # Remove existing portainer if it exists
    pct_exec(proxmox_host, manager_id,
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
    pct_exec(proxmox_host, manager_id, portainer_cmd, check=False, cfg=cfg)
    
    time.sleep(cfg['waits']['portainer_start'])
    
    print("Verifying Portainer is running...")
    portainer_status = pct_exec(proxmox_host, manager_id,
                               "docker ps --format '{{.Names}} {{.Status}}' | grep portainer || docker ps -a --format '{{.Names}} {{.Status}}' | grep portainer",
                               check=False, capture_output=True, cfg=cfg)
    if portainer_status:
        print(f"Portainer status: {portainer_status}")
    else:
        print("WARNING: Portainer container not found")
    
    # Check Portainer logs if not running
    portainer_running = pct_exec(proxmox_host, manager_id,
                                 "docker ps --format '{{.Names}}' | grep -q '^portainer$' && echo yes || echo no",
                                 check=False, capture_output=True, cfg=cfg)
    if "no" in portainer_running:
        print("Portainer failed to start. Checking logs...")
        logs = pct_exec(proxmox_host, manager_id,
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
        # Get apt-cache container name from config
        apt_cache_ct_name = cfg.get('apt-cache-ct', 'apt-cache')
        
        # Create apt-cache container FIRST (before templates)
        containers = cfg['containers']
        apt_cache_container = None
        for c in containers:
            if c['name'] == apt_cache_ct_name:
                apt_cache_container = c
                break
        
        step = 1
        templates = cfg['templates']
        non_swarm_containers = [c for c in containers if c['type'] not in ['swarm-manager', 'swarm-node']]
        # Remove apt-cache from non_swarm_containers since we handle it separately
        non_swarm_containers = [c for c in non_swarm_containers if c['name'] != apt_cache_ct_name]
        
        total_steps = (1 if apt_cache_container else 0) + len(templates) + len(non_swarm_containers) + 1 + 1  # apt-cache + templates + containers + swarm + glusterfs
        
        if apt_cache_container:
            print(f"\n[{step}/{total_steps}] Creating apt-cache container first...")
            # Create apt-cache using base template directly (before custom templates exist)
            # Temporarily override template to use base template
            original_template = apt_cache_container.get('template')
            apt_cache_container['template'] = None  # Signal to use base template
            if not create_container(apt_cache_container, cfg, step, total_steps):
                sys.exit(1)
            # Restore original template setting
            if original_template:
                apt_cache_container['template'] = original_template
            
            # Verify apt-cache is running and ready before proceeding
            print("  Verifying apt-cache service is ready...", flush=True)
            apt_cache_ip = apt_cache_container['ip_address']
            apt_cache_port = cfg['apt_cache_port']
            proxmox_host = cfg['proxmox_host']
            container_id = apt_cache_container['id']
            
            # Check if apt-cacher-ng service is running
            max_attempts = 10
            for i in range(1, max_attempts + 1):
                service_check = pct_exec(proxmox_host, container_id,
                                        "systemctl is-active apt-cacher-ng 2>/dev/null || echo 'inactive'",
                                        check=False, capture_output=True, timeout=10, cfg=cfg)
                if service_check and "active" in service_check:
                    # Test if port is accessible
                    port_check = pct_exec(proxmox_host, container_id,
                                         f"nc -z localhost {apt_cache_port} 2>/dev/null && echo 'port_open' || echo 'port_closed'",
                                         check=False, capture_output=True, timeout=10, cfg=cfg)
                    if port_check and "port_open" in port_check:
                        print(f"  ✓ apt-cache service is ready on {apt_cache_ip}:{apt_cache_port}", flush=True)
                        break
                if i < max_attempts:
                    print(f"  Waiting for apt-cache service... ({i}/{max_attempts})", flush=True)
                    time.sleep(3)
                else:
                    print(f"  ✗ ERROR: apt-cache service is not ready after {max_attempts} attempts", flush=True)
                    print(f"  Cannot proceed with template creation without apt-cache", flush=True)
                    sys.exit(1)
            
            step += 1
        else:
            print(f"\n[{step}/{total_steps}] ERROR: apt-cache container '{apt_cache_ct_name}' not found in configuration", flush=True)
            print(f"  Cannot proceed with template creation without apt-cache", flush=True)
            sys.exit(1)
        
        # Create templates (can now use apt-cache)
        for template_cfg in templates:
            if not create_template(template_cfg, cfg, step, total_steps):
                sys.exit(1)
            step += 1
        
        # Create other containers (excluding swarm containers which are handled separately)
        for container_cfg in non_swarm_containers:
            if not create_container(container_cfg, cfg, step, total_steps):
                sys.exit(1)
            step += 1
        
        # Deploy swarm (creates swarm containers)
        swarm_step = step
        print(f"\n[{swarm_step}/{total_steps}] Deploying Docker Swarm...")
        if not deploy_swarm(cfg):
            sys.exit(1)
        step += 1
        
        # Setup GlusterFS
        gluster_step = step
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
        manager_configs = [c for c in cfg['containers'] if c['type'] == 'swarm-manager']
        if manager_configs:
            manager = manager_configs[0]
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
    try:
        cfg = get_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    try:
        print("=" * 50)
        print("Cleaning Up Lab Environment")
        print("=" * 50)
        print("\nDestroying ALL containers and templates...", flush=True)
    
        print("\nStopping and destroying containers...", flush=True)
        
        # Get all container IDs
        print("  Getting list of containers...", flush=True)
        result = ssh_exec(cfg["proxmox_host"],
                         "pct list",
                         check=False, capture_output=True, timeout=30, cfg=cfg)
        
        container_ids = []
        if result:
            # Parse pct list output - format is: VMID       Status     Lock         Name
            lines = result.strip().split('\n')
            for line in lines[1:]:  # Skip header
                parts = line.split()
                if parts and parts[0].isdigit():
                    container_ids.append(parts[0])
        
        total = len(container_ids)
        if total > 0:
            print(f"  Found {total} containers to destroy: {', '.join(container_ids)}", flush=True)
            
            for idx, cid in enumerate(container_ids, 1):
                print(f"\n[{idx}/{total}] Processing container {cid}...", flush=True)
                destroy_container(cfg["proxmox_host"], cid, cfg=cfg)
            
            # Final verification
            print("\n  Verifying all containers are destroyed...", flush=True)
            remaining_result = ssh_exec(cfg["proxmox_host"],
                                             "pct list",
                                             check=False, capture_output=True, timeout=30, cfg=cfg)
            remaining_ids = []
            if remaining_result:
                remaining_lines = remaining_result.strip().split('\n')
                for line in remaining_lines[1:]:  # Skip header
                    parts = line.split()
                    if parts and parts[0].isdigit():
                        remaining_ids.append(parts[0])
            
            if remaining_ids:
                print(f"  ⚠ Warning: {len(remaining_ids)} containers still exist: {', '.join(remaining_ids)}", flush=True)
            else:
                print("  ✓ All containers destroyed", flush=True)
        else:
            print("  No containers found", flush=True)
        
        print("\nRemoving templates...", flush=True)
        template_dir = cfg['proxmox_template_dir']
        print(f"  Cleaning template directory {template_dir}...", flush=True)
        result = ssh_exec(cfg["proxmox_host"],
        f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -print | wc -l",
        check=False, capture_output=True, cfg=cfg)
        template_count = result.strip() if result else "0"
        print(f"  Removing {template_count} template files...", flush=True)
        ssh_exec(cfg["proxmox_host"],
                f"find {template_dir} -maxdepth 1 -type f -name '*.tar.zst' -delete || true",
                check=False, cfg=cfg)
        print("  ✓ Templates removed", flush=True)
        
        print("\n" + "=" * 50)
        print("Cleanup Complete!")
        print("=" * 50)
    except Exception as e:
        print(f"Error during cleanup: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


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
    # Get manager from containers
    manager_configs = [c for c in cfg['containers'] if c['type'] == 'swarm-manager']
    if not manager_configs:
        print("No swarm manager found")
        return
    manager_id = manager_configs[0]['id']
    result = pct_exec(cfg["proxmox_host"], manager_id,
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
