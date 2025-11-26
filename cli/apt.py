"""
APT/APT-GET command wrapper with fluent API
"""
import logging
import shlex
from typing import List, Optional, Dict
from .base import CommandWrapper
logger = logging.getLogger(__name__)

class AptCommands(CommandWrapper):
    """Commands and parsers for apt-related operations"""
    @staticmethod

    def is_installed_check_cmd(package: str) -> str:
        """Generate command to check if package is installed"""
        return f"dpkg -l | grep -q '^ii.*{package}' && echo installed || echo not_installed"
    @staticmethod

    def command_exists_check_cmd(command_name: str) -> str:
        """Generate command to check if command exists in PATH"""
        return f"command -v {command_name} >/dev/null && echo exists || echo not_found"
    @staticmethod

    def parse_is_installed(output: Optional[str]) -> bool:
        """Parse output to check if package is installed"""
        return CommandWrapper.contains_token(output, "installed")
    @staticmethod

    def parse_command_exists(output: Optional[str]) -> bool:
        """Parse output to check if command exists"""
        return CommandWrapper.contains_token(output, "exists")

class Apt(CommandWrapper):
    """Wrapper for APT/APT-GET commands with fluent API - generates command strings"""
    def __init__(self):
        """Initialize with default settings"""
        self._quiet: bool = False
        self._use_apt_get: bool = False
        self._options: Optional[Dict[str, str]] = None
        self._dist_upgrade: bool = False
        self._no_install_recommends: bool = False
        self._reinstall: bool = False
        self._purge: bool = False
        self._names_only: bool = False
        self._installed: bool = False
        self._upgradable: bool = False
        self._recursive: bool = False

    def quiet(self, value: bool = True) -> "Apt":
        """Set quiet mode (returns self for chaining)."""
        self._quiet = value
        return self

    def use_apt_get(self, value: bool = True) -> "Apt":
        """Use apt-get instead of apt (returns self for chaining)."""
        self._use_apt_get = value
        return self

    def dist_upgrade(self, value: bool = True) -> "Apt":
        """Use dist-upgrade instead of upgrade (returns self for chaining)."""
        self._dist_upgrade = value
        return self

    def no_install_recommends(self, value: bool = True) -> "Apt":
        """Don't install recommended packages (returns self for chaining)."""
        self._no_install_recommends = value
        return self

    def reinstall(self, value: bool = True) -> "Apt":
        """Reinstall packages (returns self for chaining)."""
        self._reinstall = value
        return self

    def purge(self, value: bool = True) -> "Apt":
        """Purge packages instead of removing (returns self for chaining)."""
        self._purge = value
        return self

    def names_only(self, value: bool = True) -> "Apt":
        """Show only package names (returns self for chaining)."""
        self._names_only = value
        return self

    def installed(self, value: bool = True) -> "Apt":
        """Show only installed packages (returns self for chaining)."""
        self._installed = value
        return self

    def upgradable(self, value: bool = True) -> "Apt":
        """Show only upgradable packages (returns self for chaining)."""
        self._upgradable = value
        return self

    def recursive(self, value: bool = True) -> "Apt":
        """Recursive dependency search (returns self for chaining)."""
        self._recursive = value
        return self

    def options(self, opts: Dict[str, str]) -> "Apt":
        """Set apt options (returns self for chaining)."""
        self._options = opts
        return self

    def _build_command(
        self,
        tool: str,
        action: str,
        flags: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None,
        packages: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> str:
        """Build apt/apt-get command with proper flag and option handling"""
        parts = []
        # Environment variables
        env_parts = []
        if env_vars:
            for key, value in env_vars.items():
                env_parts.append(f"{key}={value}")
        if env_parts:
            parts.append(" ".join(env_parts))
        # Tool and action
        parts.append(f"{tool} {action}")
        # Flags
        if flags:
            parts.extend(flags)
        # Options (-o key=value) - merge instance and method-level
        merged_options = {}
        if self._options:
            merged_options.update(self._options)
        if options:
            merged_options.update(options)
        if merged_options:
            for key, value in merged_options.items():
                # For Dpkg::Options::, split multiple options and add each separately
                if key == "Dpkg::Options::" and " " in str(value):
                    # Split the value and add each option separately
                    options_list = str(value).split()
                    for opt in options_list:
                        parts.append(f"-o {key}={opt}")
                else:
                    # Quote the value properly to handle spaces
                    parts.append(f"-o {key}={shlex.quote(str(value))}")
        # Packages
        if packages:
            parts.extend(packages)
        return " ".join(parts)

    def _get_tool(self) -> str:
        """Get tool name based on instance setting"""
        return "apt-get" if self._use_apt_get else "apt"

    def _get_flags(self, base_flags: List[str]) -> List[str]:
        """Get flags list with quiet flag based on instance setting"""
        flags = base_flags.copy()
        if self._quiet and "-qq" not in flags:
            flags.append("-qq")
        return flags

    def update(self) -> str:
        """Generate command to update package lists"""
        tool = self._get_tool()
        flags = self._get_flags([])
        # Add option to hide progress by default
        update_options = self._options.copy() if self._options else {}
        if "APT::Get::Hide-Progress" not in update_options:
            update_options["APT::Get::Hide-Progress"] = "true"
        return self._build_command(tool, "update", flags=flags, options=update_options)

    def upgrade(self) -> str:
        """Generate command to upgrade packages"""
        tool = self._get_tool()
        action = "dist-upgrade" if self._dist_upgrade else "upgrade"
        flags = self._get_flags(["-y"])
        # Add option to hide progress by default
        upgrade_options = self._options.copy() if self._options else {}
        if "APT::Get::Hide-Progress" not in upgrade_options:
            upgrade_options["APT::Get::Hide-Progress"] = "true"
        return self._build_command(tool, action, flags=flags, options=upgrade_options)

    def install(self, packages: List[str]) -> str:
        """Generate command to install packages"""
        tool = self._get_tool()
        flags = self._get_flags(["-y"])
        if self._no_install_recommends:
            flags.append("--no-install-recommends")
        if self._reinstall:
            flags.append("--reinstall")
        # Add option to hide progress by default
        install_options = self._options.copy() if self._options else {}
        if "APT::Get::Hide-Progress" not in install_options:
            install_options["APT::Get::Hide-Progress"] = "true"
        return self._build_command(tool, "install", flags=flags, options=install_options, packages=packages)

    def remove(self, packages: List[str]) -> str:
        """Generate command to remove packages"""
        tool = self._get_tool()
        action = "purge" if self._purge else "remove"
        flags = self._get_flags(["-y"])
        return self._build_command(tool, action, flags=flags, options=self._options, packages=packages)

    def autoremove(self) -> str:
        """Generate command to remove automatically installed packages"""
        tool = self._get_tool()
        flags = self._get_flags(["-y"])
        return self._build_command(tool, "autoremove", flags=flags, options=self._options)

    def autoclean(self) -> str:
        """Generate command to clean old package cache"""
        tool = self._get_tool()
        return self._build_command(tool, "autoclean")

    def clean(self) -> str:
        """Generate command to clean package cache"""
        tool = self._get_tool()
        return self._build_command(tool, "clean")

    def fix_broken(self) -> str:
        """Generate command to fix broken packages"""
        tool = self._get_tool()
        flags = self._get_flags(["--fix-broken", "-y"])
        return self._build_command(tool, "install", flags=flags, options=self._options)

    def search(self, pattern: str) -> str:
        """Generate command to search for packages"""
        if self._use_apt_get:
            # apt-get doesn't have search, use apt-cache instead
            flags = ["--names-only"] if self._names_only else []
            return self._build_command("apt-cache", "search", flags=flags, packages=[pattern])
        tool = "apt"
        flags = ["--names-only"] if self._names_only else []
        return self._build_command(tool, "search", flags=flags, packages=[pattern])

    def show(self, package: str) -> str:
        """Generate command to show package information"""
        if self._use_apt_get:
            # apt-get doesn't have show, use apt-cache instead
            return self._build_command("apt-cache", "show", packages=[package])
        return self._build_command("apt", "show", packages=[package])

    def list(self, pattern: Optional[str] = None) -> str:
        """Generate command to list packages"""
        if self._use_apt_get:
            # apt-get doesn't have list, use dpkg
            return f"dpkg -l {pattern or ''}"
        tool = "apt"
        flags = []
        if self._installed:
            flags.append("--installed")
        if self._upgradable:
            flags.append("--upgradable")
        packages = [pattern] if pattern else None
        return self._build_command(tool, "list", flags=flags, packages=packages)

    def policy(self, package: Optional[str] = None) -> str:
        """Generate command to show package pinning policy"""
        packages = [package] if package else None
        return self._build_command("apt-cache", "policy", packages=packages)

    def depends(self, package: str) -> str:
        """Generate command to show package dependencies"""
        flags = ["--recurse"] if self._recursive else []
        return self._build_command("apt-cache", "depends", flags=flags, packages=[package])

    def rdepends(self, package: str) -> str:
        """Generate command to show reverse dependencies"""
        flags = ["--recurse"] if self._recursive else []
        return self._build_command("apt-cache", "rdepends", flags=flags, packages=[package])
    # Static methods for backward compatibility (mark commands)
    @staticmethod

    def mark_hold_cmd(packages: List[str]) -> str:
        """Generate command to hold packages (prevent upgrades)"""
        packages_str = " ".join(packages)
        return f"apt-mark hold {packages_str}"
    @staticmethod

    def mark_unhold_cmd(packages: List[str]) -> str:
        """Generate command to unhold packages"""
        packages_str = " ".join(packages)
        return f"apt-mark unhold {packages_str}"
    @staticmethod

    def mark_auto_cmd(packages: List[str]) -> str:
        """Generate command to mark packages as automatically installed"""
        packages_str = " ".join(packages)
        return f"apt-mark auto {packages_str}"
    @staticmethod

    def mark_manual_cmd(packages: List[str]) -> str:
        """Generate command to mark packages as manually installed"""
        packages_str = " ".join(packages)
        return f"apt-mark manual {packages_str}"
    # Class methods for backward compatibility
    @classmethod

    def update_cmd(
        cls,
        quiet: Optional[bool] = None,
        use_apt_get: Optional[bool] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> str:
        """Backward compatibility: Generate command to update package lists"""
        apt = cls()
        if quiet is not None:
            apt.quiet(quiet)
        if use_apt_get is not None:
            apt.use_apt_get(use_apt_get)
        if options is not None:
            apt.options(options)
        return apt.update()
    @classmethod

    def upgrade_cmd(
        cls,
        dist_upgrade: Optional[bool] = None,
        use_apt_get: Optional[bool] = None,
        quiet: Optional[bool] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> str:
        """Backward compatibility: Generate command to upgrade packages"""
        apt = cls()
        if dist_upgrade is not None:
            apt.dist_upgrade(dist_upgrade)
        if use_apt_get is not None:
            apt.use_apt_get(use_apt_get)
        if quiet is not None:
            apt.quiet(quiet)
        if options is not None:
            apt.options(options)
        return apt.upgrade()
    @classmethod

    def install_cmd(
        cls,
        packages: List[str],
        no_install_recommends: Optional[bool] = None,
        use_apt_get: Optional[bool] = None,
        quiet: Optional[bool] = None,
        reinstall: Optional[bool] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> str:
        """Backward compatibility: Generate command to install packages"""
        apt = cls()
        if no_install_recommends is not None:
            apt.no_install_recommends(no_install_recommends)
        if use_apt_get is not None:
            apt.use_apt_get(use_apt_get)
        if quiet is not None:
            apt.quiet(quiet)
        if reinstall is not None:
            apt.reinstall(reinstall)
        if options is not None:
            apt.options(options)
        return apt.install(packages)
    @classmethod

    def remove_cmd(
        cls,
        packages: List[str],
        purge: Optional[bool] = None,
        use_apt_get: Optional[bool] = None,
        quiet: Optional[bool] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> str:
        """Backward compatibility: Generate command to remove packages"""
        apt = cls()
        if purge is not None:
            apt.purge(purge)
        if use_apt_get is not None:
            apt.use_apt_get(use_apt_get)
        if quiet is not None:
            apt.quiet(quiet)
        if options is not None:
            apt.options(options)
        return apt.remove(packages)