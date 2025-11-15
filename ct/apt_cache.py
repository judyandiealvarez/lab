"""
Apt-cache container type - creates an apt-cacher-ng container
"""
import sys
import time
import logging

# Import helper functions from libs
from libs import common, container
from libs.config import LabConfig, ContainerConfig
from cli import PCT, SystemCtl, Apt, Generic, CommandWrapper

# Default timeout for long-running apt/dpkg commands (seconds)
# apt-get dist-upgrade regularly goes quiet for >5 minutes while unpacking
# large package batches, so allow up to 10 minutes of silence before killing it.
APT_LONG_TIMEOUT = 600
# How long to wait for other apt/dpkg processes to release locks (seconds)
APT_LOCK_WAIT = 600

# Get logger for this module
logger = logging.getLogger(__name__)

destroy_container = common.destroy_container
wait_for_container = common.wait_for_container
setup_ssh_key = common.setup_ssh_key
get_template_path = container.get_template_path
ssh_exec = common.ssh_exec
pct_exec = common.pct_exec


def _build_wait_command(wait_seconds):
    return f"""
end=$((SECONDS+{wait_seconds}))
while [ $SECONDS -lt $end ]; do
  if ! pgrep -x apt >/dev/null \\
     && ! pgrep -x apt-get >/dev/null \\
     && ! pgrep -x dpkg >/dev/null \\
     && ! pgrep -f unattended-upgrade >/dev/null \\
     && ! pgrep -f apt.systemd.daily >/dev/null; then
    locks_present=0
    for lock in /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock; do
      if [ -e "$lock" ]; then
        locks_present=1
        break
      fi
    done
    if [ "$locks_present" -eq 0 ]; then
      echo apt_ready
      exit 0
    fi
  fi
  sleep 2
done
echo apt_wait_timeout
exit 1
"""


def _wait_for_locks(proxmox_host, container_id, cfg, wait_seconds):
    wait_cmd = _build_wait_command(wait_seconds)
    wait_output = pct_exec(
        proxmox_host,
        container_id,
        wait_cmd,
        check=False,
        capture_output=True,
        timeout=wait_seconds + 10,
        cfg=cfg,
    )
    return wait_output and "apt_ready" in wait_output, wait_output


def wait_for_package_manager(proxmox_host, container_id, cfg, wait_time=APT_LOCK_WAIT):
    """Wait for other apt/dpkg processes and locks to clear; force clean up on timeout."""
    waited, wait_output = _wait_for_locks(proxmox_host, container_id, cfg, wait_time)
    if waited:
        return True
    logger.warning(f"Timed out waiting for apt/dpkg locks to clear (will force cleanup): {wait_output}")

    cleanup_cmd = """
for name in apt apt-get apt-cache dpkg unattended-upgrade; do
  pkill -9 "$name" 2>/dev/null || true
done
pkill -9 -f unattended-upgrade 2>/dev/null || true
pkill -9 -f apt.systemd.daily 2>/dev/null || true
rm -f /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/lib/apt/lists/lock
dpkg --configure -a >/tmp/dpkg-configure.log 2>&1 || true
echo apt_cleanup_done
"""
    cleanup_output = pct_exec(
        proxmox_host,
        container_id,
        cleanup_cmd,
        check=False,
        capture_output=True,
        timeout=60,
        cfg=cfg,
    )
    if cleanup_output and "apt_cleanup_done" in cleanup_output:
        logger.info("Forced cleanup cleared apt/dpkg locks; retrying wait")
    else:
        logger.warning(f"Forced cleanup reported issues: {cleanup_output}")

    waited_again, wait_output = _wait_for_locks(proxmox_host, container_id, cfg, 60)
    if waited_again:
        return True
    logger.error(f"Unable to clear apt/dpkg locks even after forced cleanup: {wait_output}")
    return False


def _run_with_lock_retry(proxmox_host, container_id, command, cfg, timeout, retries=6, delay=10):
    """
    Execute an apt/dpkg command and retry if lock errors still occur (handles races where
    unattended-upgrade restarts after we cleared it).
    """
    last_output = None
    for attempt in range(1, retries + 1):
        output = pct_exec(
            proxmox_host,
            container_id,
            command,
            check=False,
            capture_output=True,
            timeout=timeout,
            cfg=cfg,
        )
        last_output = output
        if output and (
            "Could not get lock" in output
            or "Unable to lock" in output
            or "Resource temporarily unavailable" in output
            or "is another process using it?" in output
        ):
            logger.warning(
                "apt/dpkg lock contention detected while running '%s' (attempt %s/%s); waiting %ss",
                command.split()[0],
                attempt,
                retries,
                delay,
            )
            time.sleep(delay)
            continue
        return output
    return last_output


def run_apt_command(proxmox_host, container_id, command, cfg, timeout=APT_LONG_TIMEOUT):
    """Wrapper for apt/dpkg commands that enforces lock waiting and long timeout."""
    if not wait_for_package_manager(proxmox_host, container_id, cfg):
        return None
    return _run_with_lock_retry(proxmox_host, container_id, command, cfg, timeout=timeout)


def create_container(container_cfg: ContainerConfig, cfg: LabConfig):
    """Create apt-cacher-ng container - method for type 'apt-cache'"""
    proxmox_host = cfg.proxmox_host
    container_id = container_cfg.id
    ip_address = container_cfg.ip_address
    hostname = container_cfg.hostname
    gateway = cfg.gateway
    template_name = container_cfg.template or 'ubuntu-tmpl'
    
    # Destroy if exists
    destroy_container(proxmox_host, container_id, cfg=cfg)
    
    # Get template path
    template_path = get_template_path(template_name, cfg)
    
    # Get container resources
    resources = container_cfg.resources
    if not resources:
        # Default fallback
        from libs.config import ContainerResources
        resources = ContainerResources(memory=2048, swap=2048, cores=4, rootfs_size=20)
    storage = cfg.proxmox_storage
    bridge = cfg.proxmox_bridge
    
    # Create container
    logger.info(f"Creating container {container_id} from template...")
    create_cmd = PCT.create_cmd(
        container_id=container_id,
        template_path=template_path,
        hostname=hostname,
        memory=resources.memory,
        swap=resources.swap,
        cores=resources.cores,
        ip_address=ip_address,
        gateway=gateway,
        bridge=bridge,
        storage=storage,
        rootfs_size=resources.rootfs_size,
        unprivileged=True,
        ostype="ubuntu",
        arch="amd64"
    )
    create_output = ssh_exec(proxmox_host, create_cmd, check=False, capture_output=True, cfg=cfg)
    create_result = CommandWrapper.parse_result(create_output)
    if not create_result:
        logger.error(f"Failed to create container: {create_result.error_type.value} - {create_result.error_message}")
        return False
    
    # Start container
    logger.info("Starting container...")
    start_cmd = PCT.start_cmd(container_id)
    start_output = ssh_exec(proxmox_host, start_cmd, check=False, capture_output=True, cfg=cfg)
    start_result = CommandWrapper.parse_result(start_output)
    if start_result.has_error and start_result.error_type.value not in ["already_exists"]:
        logger.error(f"Failed to start container: {start_result.error_type.value} - {start_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Verify container is actually running
    time.sleep(cfg.waits.container_startup)
    status_cmd = PCT.status_cmd(container_id)
    status_output = ssh_exec(proxmox_host, status_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    if not PCT.parse_status_output(status_output, container_id):
        logger.error(f"Container {container_id} is not running after start. Error: {start_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Configure network
    logger.info("Configuring network...")
    network_cmd = f"ip link set eth0 up && echo 'eth0 up' && (ip addr add {ip_address}/24 dev eth0 || echo 'ip already configured') && echo 'ip configured' && (ip route add default via {gateway} dev eth0 || echo 'route already configured') && echo 'route configured' && sleep 2 && echo 'network_config_done'"
    network_output = pct_exec(proxmox_host, container_id, network_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    network_result = CommandWrapper.parse_result(network_output)
    
    # Verify network configuration actually worked
    if network_result.has_error:
        logger.error(f"Network configuration failed: {network_result.error_type.value} - {network_result.error_message}")
        if network_output:
            logger.error(f"Network config output: {network_output}")
        
        # Try to verify if network is actually configured despite the error
        logger.info("Verifying network configuration...")
        verify_cmd = f"ip addr show eth0 | grep -q '{ip_address}' && ip route | grep -q 'default via {gateway}' && echo 'network_ok' || echo 'network_failed'"
        verify_output = pct_exec(proxmox_host, container_id, verify_cmd, check=False, capture_output=True, timeout=5, cfg=cfg)
        if verify_output and "network_ok" in verify_output:
            logger.warning("Network configuration succeeded despite error message")
        else:
            logger.error("Network configuration failed - retrying...")
            # Retry network configuration
            retry_output = pct_exec(proxmox_host, container_id, network_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            retry_result = CommandWrapper.parse_result(retry_output)
            if retry_result.has_error:
                logger.error(f"Network configuration retry also failed: {retry_result.error_type.value} - {retry_result.error_message}")
                destroy_container(proxmox_host, container_id, cfg=cfg)
                return False
    
    time.sleep(cfg.waits.network_config)
    
    # Wait for container
    if not wait_for_container(proxmox_host, container_id, ip_address, cfg=cfg):
        logger.error("Container did not become ready")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Allow cloud-init to finish so it releases apt/dpkg locks
    logger.info("Waiting for cloud-init (if present) to finish initial provisioning...")
    cloud_init_cmd = (
        "if command -v cloud-init >/dev/null 2>&1; then "
        "cloud-init status --wait >/tmp/cloud-init-wait.log 2>&1 || true; "
        "cloud-init clean --logs >/dev/null 2>&1 || true; "
        "fi"
    )
    cloud_init_output = pct_exec(proxmox_host, container_id, cloud_init_cmd, check=False, capture_output=True, timeout=120, cfg=cfg)
    if cloud_init_output:
        logger.info("cloud-init wait output: %s", cloud_init_output.strip().splitlines()[-1] if cloud_init_output.strip() else "done")
    
    # Disable credential import for systemd-sysctl to avoid failures inside LXC
    logger.info("Configuring systemd-sysctl to run without host credential import...")
    sysctl_override_cmd = (
        "mkdir -p /etc/systemd/system/systemd-sysctl.service.d && "
        "printf '[Service]\\nImportCredential=\\n' > /etc/systemd/system/systemd-sysctl.service.d/override.conf"
    )
    override_output = pct_exec(proxmox_host, container_id, sysctl_override_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
    override_result = CommandWrapper.parse_result(override_output)
    if override_result.has_error:
        logger.warning(f"Failed to write systemd-sysctl override: {override_result.error_type.value} - {override_result.error_message}")
    else:
        reload_cmd = (
            "systemctl daemon-reload && "
            "systemctl stop systemd-sysctl.service 2>/dev/null || true; "
            "systemctl start systemd-sysctl.service 2>/dev/null || true; "
            "systemctl is-active systemd-sysctl.service || true"
        )
        reload_output = pct_exec(proxmox_host, container_id, reload_cmd, check=False, capture_output=True, timeout=15, cfg=cfg)
        if reload_output and "active" in reload_output:
            logger.info("systemd-sysctl.service is active with credential import disabled")
        else:
            logger.warning(f"systemd-sysctl reload/start reported issues: {reload_output}")
    
    # Create user and configure sudo
    default_user = cfg.users.default_user
    sudo_group = cfg.users.sudo_group
    logger.info("Creating user and configuring sudo...")
    user_cmd = (
        f"useradd -m -s /bin/bash -G {sudo_group} {default_user} 2>/dev/null || echo User exists; "
        f"echo '{default_user} ALL=(ALL) NOPASSWD: ALL' | tee /etc/sudoers.d/{default_user}; "
        f"chmod 440 /etc/sudoers.d/{default_user}; "
        f"mkdir -p /home/{default_user}/.ssh; chown -R {default_user}:{default_user} /home/{default_user}; chmod 700 /home/{default_user}/.ssh"
    )
    user_output = pct_exec(proxmox_host, container_id, user_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
    user_result = CommandWrapper.parse_result(user_output)
    if user_result.has_error and user_result.error_type.value not in ["already_exists"]:
        logger.error(f"Failed to setup user: {user_result.error_type.value} - {user_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Disable AppArmor reload hooks (unsupported inside this container)
    logger.info("Disabling AppArmor parser to prevent maintainer script failures...")
    disable_apparmor_cmd = (
        "APPARMOR_BIN=/usr/sbin/apparmor_parser; "
        "if command -v dpkg-divert >/dev/null 2>&1 && [ -x \"$APPARMOR_BIN\" ]; then "
        "  dpkg-divert --quiet --local --rename --add \"$APPARMOR_BIN\" >/dev/null 2>&1 || true; "
        "  cat <<'EOF' >\"$APPARMOR_BIN\"\n"
        "#!/bin/sh\n"
        "if [ \"$1\" = \"--version\" ] || [ \"$1\" = \"-V\" ]; then\n"
        "  exec /usr/sbin/apparmor_parser.distrib \"$@\"\n"
        "fi\n"
        "exit 0\n"
        "EOF\n"
        "  chmod +x \"$APPARMOR_BIN\"; "
        "fi"
    )
    disable_output = pct_exec(proxmox_host, container_id, disable_apparmor_cmd, check=False, capture_output=True, timeout=15, cfg=cfg)
    disable_result = CommandWrapper.parse_result(disable_output)
    if disable_result.has_error:
        logger.warning(f"Failed to stub AppArmor parser: {disable_result.error_type.value} - {disable_result.error_message}")
    
    # Setup SSH key
    logger.info("Setting up SSH key...")
    if not setup_ssh_key(proxmox_host, container_id, ip_address, cfg):
        logger.error("Failed to setup SSH key")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Configure DNS
    logger.info("Configuring DNS...")
    dns_servers = cfg.dns.servers
    dns_cmd = " && ".join([f"echo 'nameserver {dns}' >> /etc/resolv.conf" for dns in dns_servers])
    dns_full_cmd = f"echo 'nameserver {dns_servers[0]}' > /etc/resolv.conf && {dns_cmd.replace(dns_servers[0], '', 1).lstrip(' && ')} && echo 'DNS configuration completed'"
    dns_output = pct_exec(proxmox_host, container_id, dns_full_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
    dns_result = CommandWrapper.parse_result(dns_output)
    if dns_result.has_error:
        logger.error(f"Failed to configure DNS: {dns_result.error_type.value} - {dns_result.error_message}")
        logger.error(f"Full DNS configuration output: {dns_output}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Disable automatic apt timers/services that fight for locks
    logger.info("Disabling automatic apt services/timers to avoid lock contention...")
    disable_auto_cmd = (
        "for unit in apt-daily.service apt-daily.timer apt-daily-upgrade.service apt-daily-upgrade.timer; do "
        "systemctl stop \"$unit\" 2>/dev/null || true; "
        "systemctl disable \"$unit\" 2>/dev/null || true; "
        "systemctl mask \"$unit\" 2>/dev/null || true; "
        "done"
    )
    auto_output = pct_exec(proxmox_host, container_id, disable_auto_cmd, check=False, capture_output=True, timeout=20, cfg=cfg)
    auto_result = CommandWrapper.parse_result(auto_output)
    if auto_result.has_error:
        logger.warning(f"Failed to disable automatic apt units: {auto_result.error_type.value} - {auto_result.error_message}")
    
    # Fix apt sources
    logger.info("Fixing apt sources...")
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
    sources_output = pct_exec(proxmox_host, container_id, fix_sources_cmd, check=False, capture_output=True, cfg=cfg)
    sources_result = CommandWrapper.parse_result(sources_output)
    if sources_result.has_error:
        logger.error(f"Failed to fix apt sources: {sources_result.error_type.value} - {sources_result.error_message}")
        logger.error(f"Full apt sources fix output: {sources_output}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Update and upgrade
    logger.info("Updating package lists...")
    update_cmd = Apt.update_cmd(quiet=False)
    update_output = run_apt_command(proxmox_host, container_id, update_cmd, cfg)
    if update_output is None:
        logger.error("Failed to update packages: apt lock wait/timeout failure")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    update_result = CommandWrapper.parse_result(update_output)
    if update_result.has_error:
        logger.error(f"Failed to update packages: {update_result.error_type.value} - {update_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    logger.info("Upgrading to latest Ubuntu distribution (25.04)...")
    upgrade_cmd = Apt.upgrade_cmd(dist_upgrade=True)
    upgrade_output = run_apt_command(proxmox_host, container_id, upgrade_cmd, cfg)
    if upgrade_output is None:
        logger.error("Upgrade failed: apt lock wait/timeout failure")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Log the actual output to see what happened
    if upgrade_output:
        logger.info(f"Upgrade output (last 500 chars): {upgrade_output[-500:]}")
    
    upgrade_result = CommandWrapper.parse_result(upgrade_output)
    
    # Check if upgrade actually completed successfully
    upgrade_completed = False
    actual_apt_error = False
    
    if upgrade_output:
        # Check for real apt errors (E: prefix) - these are critical
        lines = upgrade_output.split('\n')
        for line in lines:
            # Real apt errors start with "E:"
            if line.strip().startswith("E:") and "logger:" not in line.lower() and "socket" not in line.lower():
                actual_apt_error = True
                logger.error(f"Real apt error found: {line.strip()}")
                break
        
        # Check if upgrade completed: look for "Processing triggers" or "Setting up" at the end
        if "Processing triggers" in upgrade_output or "Setting up" in upgrade_output:
            upgrade_completed = True
    
    # Only fail on actual apt errors or if upgrade didn't complete
    if actual_apt_error or (not upgrade_completed and upgrade_result.has_error):
        logger.error(f"Upgrade failed")
        logger.error(f"Error type: {upgrade_result.error_type.value if upgrade_result else 'UNKNOWN'}")
        logger.error(f"Error message: {upgrade_result.error_message if upgrade_result else 'No result'}")
        
        # Check systemd logs for failed services
        logger.info("Checking systemd logs for failed services...")
        journal_cmd = "journalctl -p err -n 50 --no-pager"
        journal_output = pct_exec(proxmox_host, container_id, journal_cmd, check=False, capture_output=True, timeout=30, cfg=cfg)
        
        if journal_output:
            logger.error(f"Systemd error logs:\n{journal_output}")
        
        # Check specific systemd-sysctl.service status if mentioned in output
        if "systemd-sysctl" in str(upgrade_output):
            logger.info("Checking systemd-sysctl.service status...")
            sysctl_status_cmd = "systemctl status systemd-sysctl.service --no-pager -l"
            sysctl_output = pct_exec(proxmox_host, container_id, sysctl_status_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if sysctl_output:
                logger.error(f"systemd-sysctl.service status:\n{sysctl_output}")
            
            logger.info("Checking systemd-sysctl.service journal logs (journalctl -xeu)...")
            sysctl_journal_cmd = "journalctl -xeu systemd-sysctl.service --no-pager"
            sysctl_journal = pct_exec(proxmox_host, container_id, sysctl_journal_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if sysctl_journal:
                logger.error(f"systemd-sysctl.service journal (-xeu):\n{sysctl_journal}")
            
            # Try to fix the service if it failed
            logger.info("Attempting to fix systemd-sysctl.service...")
            # Check if it's a credentials issue - try to reset and restart
            fix_cmd = "systemctl reset-failed systemd-sysctl.service && systemctl daemon-reload"
            fix_output = pct_exec(proxmox_host, container_id, fix_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if fix_output:
                logger.info(f"Fix attempt output: {fix_output}")
            
            # Try to start it manually
            start_cmd = "systemctl start systemd-sysctl.service"
            start_output = pct_exec(proxmox_host, container_id, start_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            
            # Check status again
            status_after_cmd = "systemctl status systemd-sysctl.service --no-pager -l"
            status_after = pct_exec(proxmox_host, container_id, status_after_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if status_after:
                if "Active: active" in status_after or "Active: inactive" in status_after:
                    logger.info(f"systemd-sysctl.service status after fix attempt:\n{status_after}")
                else:
                    logger.warning(f"systemd-sysctl.service still failed after fix attempt:\n{status_after}")
        
        logger.error(f"Full upgrade output: {upgrade_output}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # If upgrade completed but has service errors, check logs and try to fix
    if upgrade_completed and upgrade_result.has_error:
        logger.warning(f"Upgrade completed but had post-upgrade service errors: {upgrade_result.error_message}")
        if "systemd-sysctl" in str(upgrade_output):
            logger.warning("systemd-sysctl.service failed during upgrade - checking status and attempting fix...")
            
            # Check status
            logger.info("Checking systemd-sysctl.service status...")
            sysctl_status_cmd = "systemctl status systemd-sysctl.service --no-pager -l"
            sysctl_output = pct_exec(proxmox_host, container_id, sysctl_status_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if sysctl_output:
                logger.error(f"systemd-sysctl.service status:\n{sysctl_output}")
            
            logger.info("Checking systemd-sysctl.service journal logs (journalctl -xeu)...")
            sysctl_journal_cmd = "journalctl -xeu systemd-sysctl.service --no-pager"
            sysctl_journal = pct_exec(proxmox_host, container_id, sysctl_journal_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if sysctl_journal:
                logger.error(f"systemd-sysctl.service journal (-xeu):\n{sysctl_journal}")
            
            # Try to fix the service
            logger.info("Attempting to fix systemd-sysctl.service...")
            # Reset failed state and reload daemon
            fix_cmd = "systemctl reset-failed systemd-sysctl.service && systemctl daemon-reload"
            fix_output = pct_exec(proxmox_host, container_id, fix_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if fix_output:
                logger.info(f"Fix attempt output: {fix_output}")
            
            # Try to start it manually
            start_cmd = "systemctl start systemd-sysctl.service"
            start_output = pct_exec(proxmox_host, container_id, start_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            
            # Check status again after fix attempt
            status_after_cmd = "systemctl status systemd-sysctl.service --no-pager -l"
            status_after = pct_exec(proxmox_host, container_id, status_after_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
            if status_after:
                if "Active: active" in status_after or "Active: inactive" in status_after:
                    logger.info(f"systemd-sysctl.service fixed - status after fix:\n{status_after}")
                else:
                    logger.error(f"systemd-sysctl.service still failed after fix attempt:\n{status_after}")
                    # If it's a credentials issue (243), fix it by creating credential store
                    if "CREDENTIALS" in status_after or "status=243" in status_after:
                        logger.info("Fixing CREDENTIALS error (243) by creating credential store directories...")
                        # Create credential store directories
                        cred_fix_cmd = "mkdir -p /etc/credstore /run/credstore && chmod 755 /etc/credstore /run/credstore"
                        cred_fix_output = pct_exec(proxmox_host, container_id, cred_fix_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
                        if cred_fix_output:
                            logger.info(f"Credential directories created: {cred_fix_output}")
                        
                        # Reset and try to start again
                        logger.info("Resetting service and attempting to start again...")
                        restart_cmd = "systemctl reset-failed systemd-sysctl.service && systemctl daemon-reload && systemctl start systemd-sysctl.service"
                        restart_output = pct_exec(proxmox_host, container_id, restart_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
                        if restart_output:
                            logger.info(f"Restart attempt output: {restart_output}")
                        
                        # Check status one more time
                        final_status_cmd = "systemctl status systemd-sysctl.service --no-pager -l"
                        final_status = pct_exec(proxmox_host, container_id, final_status_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
                        if final_status:
                            if "Active: active" in final_status or "Active: inactive" in final_status:
                                logger.info(f"systemd-sysctl.service fixed after credential fix:\n{final_status}")
                            else:
                                logger.error(f"systemd-sysctl.service still failed after credential fix:\n{final_status}")
                                # Last resort: mask the service to prevent it from failing again
                                logger.error("Masking systemd-sysctl.service to prevent future failures...")
                                mask_cmd = "systemctl mask systemd-sysctl.service"
                                mask_output = pct_exec(proxmox_host, container_id, mask_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
                                if mask_output:
                                    logger.info(f"Service masked: {mask_output}")
                                # Verify it's masked
                                mask_check_cmd = "systemctl is-enabled systemd-sysctl.service"
                                mask_check = pct_exec(proxmox_host, container_id, mask_check_cmd, check=False, capture_output=True, timeout=10, cfg=cfg)
                                if mask_check and "masked" in mask_check:
                                    logger.info("systemd-sysctl.service successfully masked")
                                else:
                                    logger.error(f"Failed to verify masking: {mask_check}")
    
    # Install apt-cacher-ng
    logger.info("Installing apt-cacher-ng...")
    install_cmd = Apt.install_cmd(["apt-cacher-ng"])
    install_output = run_apt_command(proxmox_host, container_id, install_cmd, cfg)
    if install_output is None:
        logger.error("Failed to install apt-cacher-ng: apt lock wait/timeout failure")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Log output for debugging
    if install_output:
        logger.info(f"Install output (last 200 chars): {install_output[-200:]}")
    else:
        logger.warning("Install command returned no output - checking if package was installed anyway")
    
    # Check for actual apt errors vs logger warnings
    actual_install_error = False
    if install_output:
        lines = install_output.split('\n')
        for line in lines:
            if line.strip().startswith("E:") and "logger:" not in line.lower() and "socket" not in line.lower():
                actual_install_error = True
                logger.error(f"Real apt install error found: {line.strip()}")
                break
    
    # Verify installation regardless of output
    logger.info("Verifying apt-cacher-ng installation...")
    check_cmd = Apt.command_exists_check_cmd("apt-cacher-ng")
    check_output = pct_exec(proxmox_host, container_id, check_cmd, check=False, capture_output=True, cfg=cfg)
    package_installed = Apt.parse_command_exists(check_output)
    
    if not package_installed:
        # Package not installed - check what went wrong
        install_result = CommandWrapper.parse_result(install_output)
        if actual_install_error:
            logger.error("Failed to install apt-cacher-ng: Real apt error detected")
        elif install_output is None:
            logger.error("Failed to install apt-cacher-ng: Command returned no output (possible timeout or failure)")
        else:
            logger.error(f"Failed to install apt-cacher-ng: {install_result.error_type.value if install_result else 'UNKNOWN'} - {install_result.error_message if install_result else 'No result'}")
        logger.error(f"Install output: {install_output}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Package is installed; only warn if parser saw errors
    if install_output:
        install_result = CommandWrapper.parse_result(install_output)
        if install_result.has_error:
            logger.warning(f"apt-cacher-ng install reported issues but package is present: {install_result.error_type.value} - {install_result.error_message}")
    
    logger.info("apt-cacher-ng installed successfully")
    
    # Configure port
    apt_cache_port = cfg.apt_cache_port
    logger.info(f"Configuring apt-cacher-ng to use port {apt_cache_port}...")
    config_cmd = f"sed -i 's/^Port: .*/Port: {apt_cache_port}/' /etc/apt-cacher-ng/acng.conf 2>/dev/null || echo 'Port: {apt_cache_port}' >> /etc/apt-cacher-ng/acng.conf"
    config_output = pct_exec(proxmox_host, container_id, config_cmd, check=False, capture_output=True, cfg=cfg)
    config_result = CommandWrapper.parse_result(config_output)
    if config_result.has_error:
        logger.error(f"Failed to configure port: {config_result.error_type.value} - {config_result.error_message}")
        logger.error(f"Full port configuration output: {config_output}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Start service
    logger.info("Starting apt-cacher-ng service...")
    service_cmd = SystemCtl.enable_and_start_cmd("apt-cacher-ng")
    service_output = pct_exec(proxmox_host, container_id, service_cmd, check=False, capture_output=True, cfg=cfg)
    
    # Wait a bit for service to start
    time.sleep(cfg.waits.service_start)
    
    # Verify service is actually running (don't rely on command output)
    logger.info("Verifying apt-cacher-ng service is running...")
    is_active_cmd = SystemCtl.is_active_check_cmd("apt-cacher-ng")
    is_active_output = pct_exec(proxmox_host, container_id, is_active_cmd, check=False, capture_output=True, cfg=cfg)
    service_running = SystemCtl.parse_is_active(is_active_output)
    
    if not service_running:
        # Service not running - check what went wrong
        service_result = CommandWrapper.parse_result(service_output)
        logger.error(f"apt-cacher-ng service is not running")
        if service_output:
            logger.error(f"Service start output: {service_output}")
        if service_result.has_error:
            logger.error(f"Service start error: {service_result.error_type.value} - {service_result.error_message}")
        destroy_container(proxmox_host, container_id, cfg=cfg)
        return False
    
    # Check for any errors in service start output
    if service_output:
        service_result = CommandWrapper.parse_result(service_output)
        if service_result.has_error:
            logger.error(f"Failed to start apt-cacher-ng service: {service_result.error_type.value} - {service_result.error_message}")
            logger.error(f"Full service output: {service_output}")
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False
    
    logger.info("apt-cacher-ng service is running")
    
    logger.info(f"apt-cache container '{container_cfg.name}' created successfully")
    return True
