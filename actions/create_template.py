"""
Action to create a template
"""
import logging
from tmpl import load_template_handler
from .base import Action

logger = logging.getLogger(__name__)


class CreateTemplateAction(Action):
    """Create template"""
    description = "create template"

    def execute(self) -> bool:
        """Create template using template handler"""
        if not self.container_cfg or not self.cfg:
            logger.error("Container config or lab config is missing")
            return False

        # Get template handler for this template type
        template_type = getattr(self.container_cfg, "type", None)
        if not template_type:
            logger.error("Template type is missing from container config")
            return False

        create_template_fn = load_template_handler(template_type)
        if not create_template_fn:
            logger.error("Template handler for type '%s' not found", template_type)
            return False

        # Get plan from action context if available
        plan = getattr(self, "plan", None)

        if not create_template_fn(self.container_cfg, self.cfg, plan=plan):
            logger.error("=" * 50)
            logger.error("Template Creation Failed")
            logger.error("=" * 50)
            logger.error("Template: %s", self.container_cfg.name)
            logger.error("Error: Failed to create template '%s'", self.container_cfg.name)
            logger.error("=" * 50)
            return False

        logger.info("Template '%s' created successfully", self.container_cfg.name)
        return True
