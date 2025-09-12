"""
Logging configuration
"""
import logging
import sys
from typing import Optional

from ..config.settings import app_config


def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """Setup and return a logger instance"""
    logger = logging.getLogger(name or __name__)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    logger.setLevel(getattr(logging, app_config.log_level.upper()))
    return logger