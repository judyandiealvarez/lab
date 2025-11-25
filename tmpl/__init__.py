"""
Template module loader - dynamically loads template types as plugins
"""
import importlib
import sys
import logging
from pathlib import Path
logger = logging.getLogger(__name__)
# Map template type names to module names (handles special characters)
TYPE_TO_MODULE = {
    "ubuntu": "ubuntu",
    "ubuntu+docker": "ubuntu_docker",
}

def load_template_handler(template_type):
    """
    Dynamically load and return the template handler function for a given type
    Args:
        template_type: The template type string (e.g., 'ubuntu', 'ubuntu+docker')
    Returns:
        The create_template_* function for the given type, or None if not found
    """
    # Get module name for this type
    module_name = TYPE_TO_MODULE.get(template_type)
    if not module_name:
        return None
    # Import the module dynamically
    try:
        module = importlib.import_module(f"tmpl.{module_name}")
        # Get the create function (should be named create_template)
        if hasattr(module, "create_template"):
            return module.create_template
    except ImportError as e:
        logger.error("Failed to load template module '%s': %s", module_name, e)
        return None
    return None

def register_template_type(template_type, module_name):
    """
    Register a new template type dynamically
    Args:
        template_type: The template type string
        module_name: The module name (without .py extension)
    """
    TYPE_TO_MODULE[template_type] = module_name