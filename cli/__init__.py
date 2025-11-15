"""
CLI command wrappers with error parsing and structured results
"""
from .base import CommandResult, ErrorType, CommandWrapper
from .pct import PCT
from .systemctl import SystemCtl
from .apt import Apt
from .docker import Docker
from .gluster import Gluster
from .vzdump import Vzdump
from .generic import Generic

__all__ = [
    'CommandResult',
    'ErrorType',
    'CommandWrapper',
    'PCT',
    'SystemCtl',
    'Apt',
    'Docker',
    'Gluster',
    'Vzdump',
    'Generic',
]
