"""
Generic command wrapper for arbitrary commands
"""
import logging
from typing import Optional
from .base import CommandWrapper

logger = logging.getLogger(__name__)


class Generic(CommandWrapper):
    """Wrapper for generic/arbitrary commands - just provides parsing"""
    
    # Generic wrapper doesn't generate commands, it's for parsing arbitrary command results
    # The codebase will pass command strings directly to ssh_exec/pct_exec
    pass
