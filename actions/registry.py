"""
Action registry - dynamically finds action classes by name
"""
import importlib
import pkgutil
from typing import Dict, Type
from .base import Action

# Cache of discovered action classes
_action_cache: Dict[str, Type[Action]] = {}

def _discover_actions() -> Dict[str, Type[Action]]:
    """
    Dynamically discover all action classes in the actions package
    Returns:
        Dictionary mapping action names to action classes
    """
    if _action_cache:
        return _action_cache
    # Import actions package
    import actions
    # Discover all modules in actions package
    for importer, modname, ispkg in pkgutil.iter_modules(actions.__path__, actions.__name__ + "."):
        if ispkg or modname.endswith(".base") or modname.endswith(".registry"):
            continue
        try:
            module = importlib.import_module(modname)
            # Find all Action subclasses in the module
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and
                    issubclass(attr, Action) and
                    attr != Action and
                    hasattr(attr, "description")):
                    # Use description as the key (normalized)
                    key = attr.description.replace(" ", "-").lower()
                    _action_cache[key] = attr
                    # Also add class name variations
                    class_name_lower = attr_name.lower().replace("action", "").replace("_", "-")
                    _action_cache[class_name_lower] = attr
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("Failed to import action module %s: %s", modname, e)
    return _action_cache

def get_action_class(action_name: str) -> Type[Action]:
    """
    Get action class by name (dynamically discovered, matches by description)
    Args:
        action_name: Action name from YAML (matches action.description)
    Returns:
        Action class
    Raises:
        ValueError: If action name not found
    """
    registry = _discover_actions()
    # Normalize action name from YAML (case-insensitive, flexible whitespace)
    normalized_yaml = action_name.lower().strip()
    # Try to find by matching description
    matches = []
    for action_class in registry.values():
        # Compare normalized descriptions
        desc_normalized = action_class.description.lower().strip()
        if desc_normalized == normalized_yaml:
            return action_class
        # Also try with dashes/spaces normalized
        desc_normalized_alt = desc_normalized.replace(" ", "-").replace("_", "-")
        yaml_normalized_alt = normalized_yaml.replace(" ", "-").replace("_", "-")
        if desc_normalized_alt == yaml_normalized_alt:
            matches.append(action_class)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"Action '{action_name}' matches multiple actions: {[cls.description for cls in matches]}")
    # List available actions for better error message
    available = [cls.description for cls in set(registry.values())]
    raise ValueError(f"Action '{action_name}' not found. Available actions: {available}")

