"""
Apt-cache container type - creates an apt-cacher-ng container
"""

import logging
import subprocess
import time

from libs import common, container
from libs.config import ContainerConfig, LabConfig
from cli import Apt, CommandWrapper, FileOps, Sed, SystemCtl
from ct.helpers import run_pct_command as _run_pct_command

logger = logging.getLogger(__name__)

setup_container_base = container.setup_container_base
pct_exec = common.pct_exec
destroy_container = common.destroy_container

APT_LONG_TIMEOUT = 600
APT_LOCK_WAIT = 600
APT_LOCK_PATTERNS = [
    "could not get lock",
    "unable to lock",
    "resource temporarily unavailable",
    "is another process using it",
]
APT_CHECK_CMD = (
    "DEBIAN_FRONTEND=noninteractive "
    "apt-get update -o Acquire::Retries=0 -o APT::Get::List-Cleanup=false"
)


def wait_for_package_manager(proxmox_host, container_id, cfg, wait_time=APT_LOCK_WAIT):
    """Use apt update to detect dpkg/apt locks."""
    max_attempts = max(3, wait_time // 30)
    delay = 5
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
    for attempt in range(1, max_attempts + 1):
        try:
            update_output = pct_exec(
                proxmox_host,
                container_id,
                APT_CHECK_CMD,
                check=False,
                capture_output=True,
                timeout=APT_LONG_TIMEOUT,
                cfg=cfg,
            )
            logger.info(
                "apt update succeeded on attempt %s/%s", attempt, max_attempts
            )
            return True, update_output
        except subprocess.CalledProcessError as exc:
            output_text = (exc.output or "").lower() if exc.output else ""
            if any(pattern in output_text for pattern in APT_LOCK_PATTERNS):
                logger.warning(
                    "apt update reported lock contention (attempt %s/%s). Retrying after cleanup.",
                    attempt,
                    max_attempts,
                )
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
                    logger.info(
                        "Forced cleanup cleared apt/dpkg locks; retrying apt update"
                    )
                else:
                    logger.warning(
                        "Forced cleanup reported issues: %s", cleanup_output
                    )
                time.sleep(delay)
                continue
            logger.error(
                "apt update failed in container %s with unexpected error: %s",
                container_id,
                exc.output,
            )
            return False, exc.output
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("apt update failed in container %s: %s", container_id, exc)
            return False, None
    logger.error(
        "apt update never succeeded after %s attempts in container %s",
        max_attempts,
        container_id,
    )
    return False, None


def _run_with_lock_retry(  # pylint: disable=too-many-arguments
    proxmox_host, container_id, command, cfg, timeout, retries=6, delay=10
):
    """Execute apt command with retries when lock contention occurs."""
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
        output_lower = output.lower() if output else ""
        if any(pattern in output_lower for pattern in APT_LOCK_PATTERNS):
            logger.warning(
                "apt/dpkg lock contention detected while running %s (attempt %s/%s); waiting %ss",
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
    success, update_output = wait_for_package_manager(proxmox_host, container_id, cfg)
    if not success:
        return None
    command_stripped = command.strip()
    if command_stripped.startswith("apt ") or command_stripped.startswith("apt-get "):
        parts = command_stripped.split()
        if len(parts) >= 2 and parts[1] == "update":
            return update_output
    return _run_with_lock_retry(
        proxmox_host, container_id, command, cfg, timeout=timeout
    )


def create_container(container_cfg: ContainerConfig, cfg: LabConfig):
    """Create apt-cacher-ng container."""
    container_id = setup_container_base(
        container_cfg, cfg, privileged=False, configure_proxy=False
    )
    if not container_id:
        return False

    proxmox_host = cfg.proxmox_host
    steps = [
        ("cloud-init wait", _wait_for_cloud_init),
        ("AppArmor parser stub", _disable_apparmor_parser),
        ("disable automatic apt units", _disable_automatic_apt_units),
        ("systemd sysctl override", _configure_sysctl_override),
        ("system upgrade", _run_system_upgrade),
        ("apt-cacher-ng installation", _install_apt_cacher),
        ("apt-cacher-ng port configuration", _configure_cache_port),
        ("apt-cacher-ng service enablement", _enable_cache_service),
    ]

    for description, handler in steps:
        if not handler(proxmox_host, container_id, cfg):
            logger.error("%s failed", description)
            destroy_container(proxmox_host, container_id, cfg=cfg)
            return False

    logger.info(
        "apt-cache container '%s' created successfully", container_cfg.name
    )
    return True


def _wait_for_cloud_init(proxmox_host, container_id, cfg):
    command = (
        "if command -v cloud-init >/dev/null 2>&1; then "
        "cloud-init status --wait >/tmp/cloud-init-wait.log 2>&1 or true; "
        "cloud-init clean --logs >/dev/null 2>&1 or true; "
        "fi"
    )
    return _run_pct_command(
        proxmox_host,
        container_id,
        command,
        cfg,
        "cloud-init wait",
        warn_only=True,
        timeout=180,
    )


def _disable_apparmor_parser(proxmox_host, container_id, cfg):
    script = """
APPARMOR_BIN=/usr/sbin/apparmor_parser
if command -v dpkg-divert >/dev/null 2>&1 && [ -f "$APPARMOR_BIN" ]; then
  dpkg-divert --quiet --local --rename --add "$APPARMOR_BIN" >/dev/null 2>&1 || true
  if [ -f "$APPARMOR_BIN.distrib" ]; then
    cat <<'APPARMOR_STUB' > "$APPARMOR_BIN"
#!/bin/sh
if [ "$1" = "--version" ] || [ "$1" = "-V" ]; then
  exec /usr/sbin/apparmor_parser.distrib "$@"
fi
exit 0
APPARMOR_STUB
    chmod +x "$APPARMOR_BIN" 2>/dev/null || true
  fi
fi
echo apparmor_stub_done
"""
    return _run_pct_command(
        proxmox_host,
        container_id,
        script,
        cfg,
        "AppArmor parser stub",
        warn_only=True,
        timeout=60,
    )


def _disable_automatic_apt_units(proxmox_host, container_id, cfg):
    command = (
        "for unit in apt-daily.service apt-daily.timer "
        "apt-daily-upgrade.service apt-daily-upgrade.timer; do "
        'systemctl stop "$unit" 2>/dev/null || true; '
        'systemctl disable "$unit" 2>/dev/null || true; '
        'systemctl mask "$unit" 2>/dev/null || true; '
        "done"
    )
    return _run_pct_command(
        proxmox_host,
        container_id,
        command,
        cfg,
        "disable automatic apt units",
        warn_only=True,
    )


def _configure_sysctl_override(proxmox_host, container_id, cfg):
    mkdir_cmd = FileOps.mkdir_cmd(
        "/etc/systemd/system/systemd-sysctl.service.d", parents=True
    )
    if not _run_pct_command(
        proxmox_host,
        container_id,
        mkdir_cmd,
        cfg,
        "create sysctl override directory",
        warn_only=True,
    ):
        return False
    override_cmd = FileOps.write_cmd(
        "/etc/systemd/system/systemd-sysctl.service.d/override.conf",
        "[Service]\nImportCredential=\n",
    )
    if not _run_pct_command(
        proxmox_host,
        container_id,
        override_cmd,
        cfg,
        "write sysctl override",
        warn_only=True,
    ):
        return False
    reload_cmd = (
        "systemctl daemon-reload && "
        "systemctl stop systemd-sysctl.service 2>/dev/null || true && "
        "systemctl start systemd-sysctl.service 2>/dev/null || true"
    )
    return _run_pct_command(
        proxmox_host,
        container_id,
        reload_cmd,
        cfg,
        "reload systemd-sysctl",
        warn_only=True,
    )


def _run_system_upgrade(proxmox_host, container_id, cfg):
    commands = [
        ("apt update", Apt.update_cmd(quiet=True)),
        ("distribution upgrade", Apt.upgrade_cmd(dist_upgrade=True)),
    ]
    for description, command in commands:
        output = run_apt_command(
            proxmox_host,
            container_id,
            command,
            cfg,
            timeout=APT_LONG_TIMEOUT,
        )
        if output is None:
            logger.error("%s failed due to apt lock contention", description)
            return False
        # Check if output contains actual error indicators, not just warnings
        # Exit code 256 from pct_exec might be a false positive if output shows successful operation
        result = CommandWrapper.parse_result(output)
        if result.has_error:
            # If error is just "returned error: 256" from logger/syslog failures but output
            # shows successful package operations, this is a false positive
            error_msg_lower = (result.error_message or "").lower()
            output_lower = (output or "").lower()
            # Check if output shows successful package operations
            success_indicators = (
                "setting up" in output_lower[-1000:]
                or "processing triggers" in output_lower[-1000:]
                or "created symlink" in output_lower[-1000:]
                or "0 upgraded" in output_lower[-1000:]
                or "0 newly installed" in output_lower[-1000:]
            )
            # Check if error is just from logger/syslog failures
            logger_failure = (
                "returned error: 256" in error_msg_lower
                and ("logger:" in output_lower or "logging to syslog failed" in output_lower)
            )
            if logger_failure and success_indicators:
                logger.warning(
                    "%s reported exit code 256 from logger/syslog failures but "
                    "package operation succeeded, treating as success",
                    description,
                )
                return True
            logger.error(
                "%s failed: %s - %s",
                description,
                result.error_type.value,
                result.error_message,
            )
            # Log full output for debugging
            if output:
                logger.error("Full command output (last 1000 chars): %s", output[-1000:])
            return False
    return True


def _install_apt_cacher(proxmox_host, container_id, cfg):
    logger.info("Installing apt-cacher-ng package...")
    install_cmd = Apt.install_cmd(["apt-cacher-ng"])
    logger.info("Running install command: %s", install_cmd)
    output = run_apt_command(
        proxmox_host,
        container_id,
        install_cmd,
        cfg,
        timeout=APT_LONG_TIMEOUT,
    )
    if output is None:
        logger.error(
            "apt-cacher-ng installation failed due to apt lock contention"
        )
        return False
    logger.info("Install command output length: %s", len(output) if output else 0)
    result = CommandWrapper.parse_result(output)
    if result.has_error:
        # Check if error is just from logger/syslog failures but package installed successfully
        error_msg_lower = (result.error_message or "").lower()
        output_lower = (output or "").lower()
        success_indicators = (
            "setting up apt-cacher-ng" in output_lower
            or "created symlink" in output_lower
            or "processing triggers" in output_lower[-1000:]
        )
        logger_failure = (
            "returned error: 256" in error_msg_lower
            and ("logger:" in output_lower or "logging to syslog failed" in output_lower)
        )
        if logger_failure and success_indicators:
            logger.warning(
                "apt-cacher-ng install reported exit code 256 from logger/syslog failures "
                "but package installed successfully, treating as success"
            )
            # Still verify binary exists
            check_cmd = Apt.command_exists_check_cmd("apt-cacher-ng")
            check_output = pct_exec(
                proxmox_host,
                container_id,
                check_cmd,
                check=False,
                capture_output=True,
                cfg=cfg,
            )
            if Apt.parse_command_exists(check_output):
                return True
        logger.error(
            "apt-cacher-ng install reported issues: %s - %s",
            result.error_type.value,
            result.error_message,
        )
        logger.error("Install output: %s", output[-500:] if output else "No output")
        return False
    logger.info("Install command completed successfully")
    # Verify binary exists
    check_cmd = Apt.command_exists_check_cmd("apt-cacher-ng")
    check_output = pct_exec(
        proxmox_host,
        container_id,
        check_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if not Apt.parse_command_exists(check_output):
        logger.error("apt-cacher-ng binary not found after installation")
        return False
    # Verify service unit exists
    service_check_cmd = "systemctl list-unit-files apt-cacher-ng.service 2>&1 | grep -q apt-cacher-ng.service && echo 'exists' || echo 'missing'"
    service_check = pct_exec(
        proxmox_host,
        container_id,
        service_check_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if not service_check or "exists" not in service_check:
        logger.error(
            "apt-cacher-ng service unit not found after installation. "
            "Check: %s",
            service_check,
        )
        # Check if package is actually installed
        dpkg_check = "dpkg -l | grep apt-cacher-ng 2>&1"
        dpkg_output = pct_exec(
            proxmox_host,
            container_id,
            dpkg_check,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        logger.error("dpkg status: %s", dpkg_output)
        return False
    return True


def _configure_cache_port(proxmox_host, container_id, cfg):
    port = cfg.apt_cache_port
    replace_cmd = Sed.replace_cmd(
        "/etc/apt-cacher-ng/acng.conf",
        "^Port:.*",
        f"Port: {port}",
        flags="",
    )
    if _run_pct_command(
        proxmox_host,
        container_id,
        replace_cmd,
        cfg,
        "update apt-cacher-ng port",
        warn_only=True,
    ):
        return True
    append_cmd = FileOps.write_cmd(
        "/etc/apt-cacher-ng/acng.conf",
        f"Port: {port}\n",
        append=True,
    )
    return _run_pct_command(
        proxmox_host,
        container_id,
        append_cmd,
        cfg,
        "append apt-cacher-ng port",
        warn_only=True,
    )


def _enable_cache_service(proxmox_host, container_id, cfg):
    start_cmd = SystemCtl.enable_and_start_cmd("apt-cacher-ng")
    if not _run_pct_command(
        proxmox_host,
        container_id,
        start_cmd,
        cfg,
        "start apt-cacher-ng service",
        warn_only=False,
    ):
        return False
    # Wait a moment for service to start
    time.sleep(2)
    is_active_cmd = SystemCtl.is_active_check_cmd("apt-cacher-ng")
    status = pct_exec(
        proxmox_host,
        container_id,
        is_active_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    if SystemCtl.parse_is_active(status):
        # Verify service stays active
        time.sleep(2)
        status2 = pct_exec(
            proxmox_host,
            container_id,
            is_active_cmd,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        if SystemCtl.parse_is_active(status2):
            return True
        # Service started but stopped - check why
        status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1 | head -20"
        status_output = pct_exec(
            proxmox_host,
            container_id,
            status_cmd,
            check=False,
            capture_output=True,
            cfg=cfg,
        )
        logger.error(
            "apt-cacher-ng service started but stopped. Status: %s", status_output
        )
        return False
    # Service didn't start - check why
    status_cmd = "systemctl status apt-cacher-ng --no-pager -l 2>&1 | head -20"
    status_output = pct_exec(
        proxmox_host,
        container_id,
        status_cmd,
        check=False,
        capture_output=True,
        cfg=cfg,
    )
    logger.error(
        "apt-cacher-ng service is not active. Status: %s", status_output
    )
    return False
