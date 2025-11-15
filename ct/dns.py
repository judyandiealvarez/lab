"""
DNS container type - creates a SiNS DNS server container
"""
import sys
import time
import base64
import logging

# Import helper functions from libs
from libs import common, container
from libs.config import LabConfig, ContainerConfig
from cli import Apt, SystemCtl, Generic, CommandWrapper

# Get logger for this module
logger = logging.getLogger(__name__)

setup_container_base = container.setup_container_base
destroy_container = common.destroy_container
pct_exec = common.pct_exec


def create_container(container_cfg: ContainerConfig, cfg: LabConfig):
    """Create SiNS DNS server container - method for type 'dns'"""
    proxmox_host = cfg.proxmox_host
    # DNS server needs privileged container for port 53
    container_id = setup_container_base(container_cfg, cfg, privileged=True)
    
    if not container_id:
        logger.error(f"Failed to create container {container_cfg.id}")
        return False
    
    params = container_cfg.params
    dns_port = params.get('dns_port', 53)
    web_port = params.get('web_port', 80)
    postgres_host = params.get('postgres_host', '10.11.3.18')
    postgres_port = params.get('postgres_port', 5432)
    postgres_db = params.get('postgres_db', 'dns_server')
    postgres_user = params.get('postgres_user', 'postgres')
    postgres_password = params.get('postgres_password', 'postgres')
    
    # Install .NET 8 runtime prerequisites
    logger.info("Installing prerequisites for .NET 8...")
    prereq_cmd = Apt.install_cmd(["wget", "apt-transport-https", "lsb-release"])
    prereq_output = pct_exec(proxmox_host, container_id, prereq_cmd, check=False, capture_output=True, timeout=60, cfg=cfg)
    prereq_result = CommandWrapper.parse_result(prereq_output)
    if not prereq_result:
        logger.error(f"Failed to install prerequisites: {prereq_result.error_type.value} - {prereq_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Add Microsoft package repository
    logger.info("Adding Microsoft package repository...")
    repo_cmd = (
        "wget https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb -O /tmp/packages-microsoft-prod.deb 2>&1 && "
        "dpkg -i /tmp/packages-microsoft-prod.deb 2>&1"
    )
    repo_output = pct_exec(proxmox_host, container_id, repo_cmd, check=False, capture_output=True, timeout=60, cfg=cfg)
    repo_result = CommandWrapper.parse_result(repo_output)
    if repo_result.has_error or (repo_result.output and "404" in repo_result.output):
        logger.error(f"Failed to add Microsoft repository: {repo_result.error_type.value} - {repo_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Install .NET 8 SDK (needed to build the application)
    logger.info("Installing .NET 8 SDK...")
    update_cmd = Apt.update_cmd(quiet=True)
    update_output = pct_exec(proxmox_host, container_id, update_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    update_result = CommandWrapper.parse_result(update_output)
    if not update_result:
        logger.error(f"Failed to update packages: {update_result.error_type.value} - {update_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    dotnet_cmd = Apt.install_cmd(["dotnet-sdk-8.0"])
    dotnet_output = pct_exec(proxmox_host, container_id, dotnet_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
    dotnet_result = CommandWrapper.parse_result(dotnet_output)
    if not dotnet_result:
        logger.error(f"Failed to install .NET SDK: {dotnet_result.error_type.value} - {dotnet_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify .NET is installed
    check_cmd = Apt.command_exists_check_cmd("dotnet")
    check_output = pct_exec(proxmox_host, container_id, check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if not Apt.parse_command_exists(check_output):
        logger.error(".NET installation failed - dotnet command not found")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    dotnet_version_cmd = "dotnet --version 2>&1"
    dotnet_version_output = pct_exec(proxmox_host, container_id, dotnet_version_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if dotnet_version_output:
        logger.info(f".NET installed: {dotnet_version_output.strip()}")
    
    # Install postgresql-client and git
    logger.info("Installing prerequisites...")
    prereq2_cmd = Apt.install_cmd(["postgresql-client", "git", "curl"])
    prereq2_output = pct_exec(proxmox_host, container_id, prereq2_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    prereq2_result = CommandWrapper.parse_result(prereq2_output)
    if not prereq2_result:
        logger.error(f"Failed to install prerequisites: {prereq2_result.error_type.value} - {prereq2_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Wait for PostgreSQL to be ready
    logger.info("Waiting for PostgreSQL to be ready...")
    max_attempts = 10
    pg_ready = False
    for i in range(1, max_attempts + 1):
        pg_check_cmd = f"PGPASSWORD={postgres_password} pg_isready -h {postgres_host} -p {postgres_port} -U {postgres_user} 2>&1 || echo 'not_ready'"
        pg_check_output = pct_exec(proxmox_host, container_id, pg_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if pg_check_output and "accepting connections" in pg_check_output:
            logger.info("PostgreSQL is ready")
            pg_ready = True
            break
        if i < max_attempts:
            logger.info(f"Waiting for PostgreSQL... ({i}/{max_attempts})")
            time.sleep(3)
    
    if not pg_ready:
        logger.warning("PostgreSQL may not be ready, but continuing...")
    
    # Create database if needed
    create_db_cmd = (
        f"PGPASSWORD={postgres_password} psql -h {postgres_host} -p {postgres_port} -U {postgres_user} -d postgres "
        f"-c 'CREATE DATABASE {postgres_db};' 2>&1 || echo 'Database may already exist'"
    )
    db_output = pct_exec(proxmox_host, container_id, create_db_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
    db_result = CommandWrapper.parse_result(db_output)
    if db_result.has_error and db_result.error_type.value not in ["already_exists"]:
        logger.warning(f"Database creation had issues: {db_result.error_type.value} - {db_result.error_message}")
    
    # Clone SiNS repository
    logger.info("Cloning SiNS repository...")
    clone_cmd = "cd /opt && rm -rf sins && git clone https://github.com/judyandiealvarez/SiNS.git sins 2>&1"
    clone_output = pct_exec(proxmox_host, container_id, clone_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    clone_result = CommandWrapper.parse_result(clone_output)
    if clone_result.has_error or (clone_result.output and ("fatal" in clone_result.output.lower())):
        logger.error(f"Failed to clone SiNS repository: {clone_result.error_type.value} - {clone_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify repository was cloned
    verify_repo_cmd = "test -d /opt/sins && echo exists || echo missing"
    verify_repo_output = pct_exec(proxmox_host, container_id, verify_repo_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if verify_repo_output and "exists" not in verify_repo_output:
        logger.error("SiNS repository was not cloned")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Create appsettings.Production.json BEFORE building (so it gets included in publish)
    logger.info("Configuring SiNS application settings...")
    appsettings = f"""{{
  "ConnectionStrings": {{
    "DefaultConnection": "Host={postgres_host};Port={postgres_port};Database={postgres_db};Username={postgres_user};Password={postgres_password}"
  }},
  "Logging": {{
    "LogLevel": {{
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }}
  }},
  "AllowedHosts": "*",
  "Kestrel": {{
    "Endpoints": {{
      "Http": {{
        "Url": "http://0.0.0.0:{web_port}"
      }}
    }}
  }}
}}
"""
    appsettings_b64 = base64.b64encode(appsettings.encode()).decode()
    appsettings_cmd = f"echo {appsettings_b64} | base64 -d > /opt/sins/sins/appsettings.Production.json 2>&1"
    appsettings_output = pct_exec(proxmox_host, container_id, appsettings_cmd, check=False, capture_output=True, cfg=cfg)
    appsettings_result = CommandWrapper.parse_result(appsettings_output)
    if appsettings_result.has_error:
        logger.error(f"Failed to create appsettings: {appsettings_result.error_type.value} - {appsettings_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Build SiNS application (this will include appsettings.Production.json in the output)
    logger.info("Building SiNS application...")
    build_cmd = "cd /opt/sins/sins && dotnet publish -c Release -o /opt/sins/app 2>&1"
    build_output = pct_exec(proxmox_host, container_id, build_cmd, check=False, capture_output=True, timeout=300, cfg=cfg)
    build_result = CommandWrapper.parse_result(build_output)
    if build_result.has_error or (build_result.output and "Build FAILED" in build_result.output):
        logger.error(f"Failed to build SiNS application: {build_result.error_type.value} - {build_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify build output exists
    verify_build_cmd = "test -f /opt/sins/app/sins.dll && echo exists || echo missing"
    verify_build_output = pct_exec(proxmox_host, container_id, verify_build_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if verify_build_output and "exists" not in verify_build_output:
        logger.error("SiNS application was not built - sins.dll not found")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Also ensure appsettings.json exists in the output directory
    appsettings2_cmd = f"echo {appsettings_b64} | base64 -d > /opt/sins/app/appsettings.json 2>&1"
    appsettings2_output = pct_exec(proxmox_host, container_id, appsettings2_cmd, check=False, capture_output=True, cfg=cfg)
    appsettings2_result = CommandWrapper.parse_result(appsettings2_output)
    if appsettings2_result.has_error:
        logger.warning(f"Failed to create appsettings.json in output: {appsettings2_result.error_type.value} - {appsettings2_result.error_message}")
    
    # Create systemd service for SiNS
    logger.info("Creating systemd service...")
    connection_string = f"Host={postgres_host};Port={postgres_port};Database={postgres_db};Username={postgres_user};Password={postgres_password}"
    # Escape the connection string for systemd (quote it)
    connection_string_escaped = connection_string.replace('"', '\\"')
    systemd_service = f"""[Unit]
Description=SiNS DNS Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/sins/app
ExecStart=/usr/bin/dotnet /opt/sins/app/sins.dll
Restart=always
RestartSec=10
Environment="ASPNETCORE_URLS=http://0.0.0.0:{web_port}"
Environment="ASPNETCORE_ENVIRONMENT=Production"
Environment="ConnectionStrings__DefaultConnection={connection_string_escaped}"

[Install]
WantedBy=multi-user.target
"""
    service_b64 = base64.b64encode(systemd_service.encode()).decode()
    service_cmd = (
        f"systemctl stop sins 2>/dev/null || true; "
        f"echo {service_b64} | base64 -d > /etc/systemd/system/sins.service && "
        "systemctl daemon-reload && "
        "systemctl enable sins && "
        "systemctl start sins 2>&1"
    )
    service_output = pct_exec(proxmox_host, container_id, service_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
    service_result = CommandWrapper.parse_result(service_output)
    if service_result.has_error:
        logger.error(f"Failed to start SiNS service: {service_result.error_type.value} - {service_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    time.sleep(cfg.waits.service_start)
    
    # Verify service is running
    is_active_cmd = SystemCtl.is_active_check_cmd("sins")
    is_active_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if SystemCtl.parse_is_active(is_active_output):
        logger.info("SiNS DNS server is running")
    else:
        logger.warning("SiNS DNS server may not be running, checking logs...")
        logs_cmd = "journalctl -u sins --no-pager -n 20 2>&1"
        logs_output = pct_exec(proxmox_host, container_id, logs_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
        if logs_output:
            logger.warning(f"Logs: {logs_output[:300]}...")
        # Check if process is running
        process_check_cmd = "pgrep -f sins.dll >/dev/null 2>&1 && echo running || echo not_running"
        process_check_output = pct_exec(proxmox_host, container_id, process_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
        if process_check_output and "running" in process_check_output:
            logger.info("SiNS process is running despite inactive systemd status")
        else:
            logger.error(f"SiNS DNS server is not running")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    logger.info(f"DNS container '{container_cfg.name}' created successfully")
    return True
