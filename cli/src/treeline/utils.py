"""Utility functions for Treeline."""

import logging
import os
from datetime import datetime
from pathlib import Path


def get_treeline_dir() -> Path:
    """Get the treeline data directory in the user's home directory.

    Returns:
        Path to the treeline directory (default: ~/.treeline)
        Can be overridden with TREELINE_DIR environment variable for testing.
    """
    treeline_dir_override = os.getenv("TREELINE_DIR")
    if treeline_dir_override:
        return Path(treeline_dir_override)
    return Path.home() / ".treeline"


def get_log_dir() -> Path:
    """Get the treeline log directory.

    Returns:
        Path to the log directory (~/.treeline/logs)
    """
    return get_treeline_dir() / "logs"


def get_log_file_path() -> Path:
    """Get the current log file path.

    Log files are named by date: treeline-YYYY-MM-DD.log

    Returns:
        Path to the current log file
    """
    log_dir = get_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    return log_dir / f"treeline-{date_str}.log"


def setup_logging(level: int = logging.DEBUG) -> logging.Logger:
    """Configure logging for Treeline CLI.

    Logs are written to ~/.treeline/logs/treeline-YYYY-MM-DD.log

    Args:
        level: Logging level (default: DEBUG for file, errors only to console)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("treeline")

    # Only configure if not already configured
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # File handler - captures all logs with detailed formatting
    log_file = get_log_file_path()
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "treeline") -> logging.Logger:
    """Get a logger instance for the given name.

    If logging hasn't been set up yet, this will configure it automatically.

    Args:
        name: Logger name (will be prefixed with 'treeline.')

    Returns:
        Logger instance
    """
    # Ensure base logging is configured
    setup_logging()

    if name == "treeline":
        return logging.getLogger("treeline")
    return logging.getLogger(f"treeline.{name}")
