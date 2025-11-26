"""Base class for command classes."""
from dataclasses import dataclass
from typing import Any


@dataclass
class Command:
    """Base class for command classes."""
    cfg: Any

