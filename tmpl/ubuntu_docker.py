"""
Ubuntu+Docker template type - creates an Ubuntu template with Docker pre-installed using Container class
"""
import logging
from libs.config import TemplateConfig, LabConfig
from libs.container_manager import Container
from libs.container import destroy_container

logger = logging.getLogger(__name__)

def create_template(template_cfg: TemplateConfig, cfg: LabConfig, plan=None):
    """Create Ubuntu+Docker template - method for type 'ubuntu+docker'"""
    # Convert TemplateConfig to ContainerConfig for use with Container class
    from libs.config import ContainerConfig
    # Determine template to use: "base" means use base template (None), otherwise use template name
    template_for_container = None if (template_cfg.template == "base" or not template_cfg.template) else template_cfg.template
    container_cfg = ContainerConfig(
        name=template_cfg.name,
        id=template_cfg.id,
        ip=template_cfg.ip,
        hostname=template_cfg.hostname,
        type=template_cfg.type,
        resources=template_cfg.resources,
        ip_address=template_cfg.ip_address,
        template=template_for_container,
        actions=template_cfg.actions,
    )
    # Use Container class to create and configure container
    container = Container(container_cfg, cfg, plan=plan)
    if not container.create():
        logger.error("Failed to create template container")
        # Ensure container is destroyed on failure
        destroy_container(cfg.proxmox_host, container_cfg.id, cfg=cfg)
        return False
    # Container is destroyed by create_template_archive action after archive is created
    logger.info("Ubuntu+Docker template '%s' created successfully", template_cfg.name)
    return True
