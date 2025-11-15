"""
Logging configuration for lab deployment tool
Provides a centralized logger with console and optional file output
"""
import logging
import sys
from pathlib import Path


def setup_logging(level=logging.INFO, log_file=None, format_string=None):
    """
    Setup logging configuration
    
    Args:
        level: Logging level (default: INFO)
        log_file: Optional path to log file (default: None, console only)
        format_string: Custom format string (default: uses standard format)
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Create formatter
    formatter = logging.Formatter(format_string, datefmt=date_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    # Console handler (always)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger


def get_logger(name=None):
    """
    Get a logger instance for a module
    
    Args:
        name: Logger name (default: None, uses calling module name)
    
    Returns:
        Logger instance
    """
    if name is None:
        # Try to get the calling module name
        import inspect
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'lab')
    
    return logging.getLogger(name)


# Initialize default logger
_default_logger = None


def init_logger(level=logging.INFO, log_file=None):
    """
    Initialize the default logger (called once at startup)
    
    Args:
        level: Logging level
        log_file: Optional log file path
    """
    global _default_logger
    setup_logging(level=level, log_file=log_file)
    _default_logger = get_logger('lab')
    return _default_logger


def get_default_logger():
    """
    Get the default logger instance
    
    Returns:
        Default logger instance
    """
    global _default_logger
    if _default_logger is None:
        # Initialize with defaults if not already initialized
        _default_logger = init_logger()
    return _default_logger

