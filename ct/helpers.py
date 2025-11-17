"""
Common helper utilities for container modules.
"""

import logging

from libs import common
from cli import CommandWrapper

pct_exec = common.pct_exec


def run_pct_command(  # pylint: disable=too-many-arguments
    proxmox_host,
    container_id,
    command,
    cfg,
    description,
    *,
    timeout=120,
    warn_only=False,
    logger=None,
):
    """Execute pct command with consistent logging."""
    log = logger or logging.getLogger(__name__)
    output = pct_exec(
        proxmox_host,
        container_id,
        command,
        capture_output=True,
        timeout=timeout,
        cfg=cfg,
    )
    result = CommandWrapper.parse_result(output)
    if result.has_error:
        log_fn = log.warning if warn_only else log.error
        log_fn(
            "%s failed: %s - %s",
            description,
            result.error_type.value,
            result.error_message,
        )
        if output:
            log_fn("%s output: %s", description, output.splitlines()[-1])
        return False
    return True


def run_apt_step(  # pylint: disable=too-many-arguments
    proxmox_host,
    container_id,
    cfg,
    command,
    description,
    *,
    runner,
    logger=None,
):
    """Run apt/dpkg command with lock handling and logging."""
    log = logger or logging.getLogger(__name__)
    output = runner(
        proxmox_host,
        container_id,
        command,
        cfg,
    )
    if output is None:
        log.error("%s failed due to apt lock contention", description)
        return False
    result = CommandWrapper.parse_result(output)
    if result.has_error:
        # Check if error is just from logger/syslog failures but package installed successfully
        error_msg_lower = (result.error_message or "").lower()
        output_lower = (output or "").lower()
        success_indicators = (
            "setting up" in output_lower[-1000:]
            or "processing triggers" in output_lower[-1000:]
            or "created symlink" in output_lower[-1000:]
        )
        logger_failure = (
            "returned error: 256" in error_msg_lower
            and ("logger:" in output_lower or "logging to syslog failed" in output_lower)
        )
        if logger_failure and success_indicators:
            log.warning(
                "%s reported exit code 256 from logger/syslog failures but "
                "package operation succeeded, treating as success",
                description,
            )
            return True
        log.error(
            "%s failed: %s - %s",
            description,
            result.error_type.value,
            result.error_message,
        )
        return False
    return True
