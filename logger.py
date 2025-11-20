# logger.py
"""
Enhanced logging system for OTMS
Logs errors, warnings, and info to files
"""

import logging
from pathlib import Path
from datetime import datetime
import sys

# Create logs directory
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Configure logging
def setup_logger(name: str = "OTMS") -> logging.Logger:
    """Setup application logger with file and console handlers"""
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # File handler (daily rotation)
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        LOGS_DIR / f"otms_{today}.log",
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create default logger
logger = setup_logger()

# Convenience functions
def log_info(message: str):
    """Log info message"""
    logger.info(message)

def log_warning(message: str):
    """Log warning message"""
    logger.warning(message)

def log_error(message: str, exc_info=False):
    """Log error message"""
    logger.error(message, exc_info=exc_info)

def log_critical(message: str, exc_info=False):
    """Log critical error"""
    logger.critical(message, exc_info=exc_info)

def log_debug(message: str):
    """Log debug message"""
    logger.debug(message)