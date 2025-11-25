"""
AppArmor parser stub action
"""
import logging
from .base import Action
logger = logging.getLogger(__name__)

class AppArmorParserStubAction(Action):
    """Action to disable AppArmor parser to avoid maintainer script failures"""
    description = "AppArmor parser stub"

    def execute(self) -> bool:
        """Disable AppArmor parser to avoid maintainer script failures"""
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
        output, exit_code = self.ssh_service.execute(script, timeout=60, sudo=True)
        if exit_code is not None and exit_code != 0:
            logger.error("AppArmor parser stub failed with exit code %s", exit_code)
            if output:
                logger.error("AppArmor parser stub output: %s", output.splitlines()[-1])
            return False
        return True

