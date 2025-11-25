"""
Base Action class for container setup steps
"""
import logging
from typing import Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from services.ssh import SSHService
    from services.apt import APTService
    from services.pct import PCTService
    from libs.config import LabConfig, ContainerConfig
logger = logging.getLogger(__name__)

class Action:
    """Base class for container setup actions"""
    description: str = ""

    def __init__(
        self,
        ssh_service: Optional["SSHService"] = None,
        apt_service: Optional["APTService"] = None,
        pct_service: Optional["PCTService"] = None,
        container_id: Optional[str] = None,
        cfg: Optional["LabConfig"] = None,
        container_cfg: Optional["ContainerConfig"] = None,
    ):
        """
        Initialize action with services
        Args:
            ssh_service: SSH service for executing commands
            apt_service: APT service for package management
            pct_service: PCT service for container operations
            container_id: Container ID
            cfg: Lab configuration
            container_cfg: Container configuration
        """
        self.ssh_service = ssh_service
        self.apt_service = apt_service
        self.pct_service = pct_service
        self.container_id = container_id
        self.cfg = cfg
        self.container_cfg = container_cfg
        # For backward compatibility
        self._container_cfg = container_cfg

    def execute(self) -> bool:
        """
        Execute the action
        Returns:
            True if successful, False otherwise
        """
        raise NotImplementedError("Subclasses must implement execute()")
