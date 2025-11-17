"""
Container module loader - dynamically loads container types as plugins
"""

import importlib
import logging

logger = logging.getLogger(__name__)

# Map container type names to module names (handles special characters)
TYPE_TO_MODULE = {
    "apt-cache": "apt_cache",
    "pgsql": "pgsql",
    "haproxy": "haproxy",
    "dns": "dns",
    "swarm-manager": "swarm_manager",
    "swarm-node": "swarm_node",
}


def load_container_handler(container_type):
    """
    Dynamically load and return the container handler function for a given type

    Args:
        container_type: The container type string (e.g., 'apt-cache', 'pgsql', etc.)

    Returns:
        The create_container function for the given type, or None if not found
    """
    # Get module name for this type
    module_name = TYPE_TO_MODULE.get(container_type)
    if not module_name:
        return None

    # Import the module dynamically
    try:
        module = importlib.import_module(f"ct.{module_name}")
        # Get the create function (should be named create_container)
        if hasattr(module, "create_container"):
            return module.create_container
    except ImportError as err:
        logger.error(
            "Failed to load container module '%s': %s", module_name, err
        )
        return None

    return None


def register_container_type(container_type, module_name):
    """
    Register a new container type dynamically

    Args:
        container_type: The container type string
        module_name: The module name (without .py extension)
    """
    TYPE_TO_MODULE[container_type] = module_name
