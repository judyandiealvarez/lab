"""
Template cleanup action
"""
from .base import Action
from cli import FileOps, Apt

class TemplateCleanupAction(Action):
    """Cleanup template before archiving"""
    description = "template cleanup"

    def execute(self) -> bool:
        """Cleanup template"""
        default_user = self.cfg.users.default_user
        cleanup_commands = [
            ("Remove apt proxy configuration", FileOps().force().remove("/etc/apt/apt.conf.d/01proxy")),
            ("Remove SSH host keys", FileOps().force().allow_glob().remove("/etc/ssh/ssh_host_*")),
            ("Truncate machine-id", FileOps().truncate("/etc/machine-id")),
            ("Remove DBus machine-id", FileOps().force().remove("/var/lib/dbus/machine-id")),
            ("Recreate DBus machine-id symlink", FileOps().symlink("/etc/machine-id", "/var/lib/dbus/machine-id")),
            ("Remove apt lists", FileOps().force().recursive().allow_glob().remove("/var/lib/apt/lists/*")),
            ("Remove log files", FileOps().suppress_errors().find_delete("/var/log", "*.log")),
            ("Remove compressed logs", FileOps().suppress_errors().find_delete("/var/log", "*.gz")),
            ("Clear root history", FileOps().suppress_errors().truncate("/root/.bash_history")),
            (f"Clear {default_user} history", FileOps().suppress_errors().truncate(f"/home/{default_user}/.bash_history")),
        ]
        for desc, cmd in cleanup_commands:
            output, exit_code = self.pct_service.execute(self.container_id, cmd)
            if exit_code is not None and exit_code != 0:
                self.logger.warning("Failed to %s: %s", desc, output)
        # Clean apt cache
        clean_cmd = Apt().clean()
        output, exit_code = self.pct_service.execute(self.container_id, clean_cmd)
        if exit_code is not None and exit_code != 0:
            self.logger.warning("Failed to clean apt cache: %s", output)
        return True

