"""
Centralized logging configuration for the Fabric Monitoring System.
"""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def setup_logging(
    name: str, log_file: str | None = None, level: int = logging.INFO, log_to_stdout: bool = True
) -> logging.Logger:
    """
    Configure and return a logger with standard formatting.

    Args:
        name: The name of the logger (usually __name__).
        log_file: Optional path to a log file. If provided, logs will be written to this file
                  with daily rotation.
        level: Logging level (default: logging.INFO).
        log_to_stdout: Whether to print logs to stdout (default: True).

    Returns:
        Configured logger instance.
    """
    # Ensure stdout is line-buffered for immediate output if we are logging to it
    if log_to_stdout and hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers to avoid duplicates if setup is called multiple times
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    if log_to_stdout:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger to avoid double logging if root is configured
    logger.propagate = False

    return logger
