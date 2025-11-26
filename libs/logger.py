"""
Logging configuration for lab deployment tool
Provides a centralized logger with console and optional file output
"""
import inspect
import logging
import sys
from datetime import datetime
from pathlib import Path
DEFAULT_LOGGER_NAME = "lab"

def setup_logging(level=logging.INFO, log_file=None, format_string=None):
    """
    Setup logging configuration
    Args:
        level: Logging level (default: INFO)
        log_file: Optional path to log file (default: None, console only)
        format_string: Custom format string (default: uses standard format)
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)-25s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
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
        frame = inspect.currentframe()
        caller = frame.f_back if frame else None
        if caller:
            name = caller.f_globals.get("__name__", DEFAULT_LOGGER_NAME)
        else:
            name = DEFAULT_LOGGER_NAME
    return logging.getLogger(name)

def init_logger(level=logging.INFO, log_file=None, always_log_to_file=True):
    """
    Initialize the default logger (called once at startup)
    Args:
        level: Logging level
        log_file: Optional log file path (if None and always_log_to_file=True, creates timestamped log)
        always_log_to_file: If True and log_file is None, creates a timestamped log file in logs/ directory
    """
    if log_file is None and always_log_to_file:
        # Create logs directory
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = logs_dir / f"lab_{timestamp}.log"
    setup_logging(level=level, log_file=log_file)
    return logging.getLogger(DEFAULT_LOGGER_NAME)

def get_default_logger():
    """
    Get the default logger instance
    Returns:
        Default logger instance
    """
    return logging.getLogger(DEFAULT_LOGGER_NAME)