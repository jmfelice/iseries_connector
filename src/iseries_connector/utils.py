"""Common utilities for iSeries connector modules."""

import logging
import uuid
from typing import Optional

class RequestIdFilter(logging.Filter):
    """Filter to add request ID to log records."""
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = str(uuid.uuid4())
        return True

def setup_logging(name: str, level: Optional[int] = None) -> logging.Logger:
    """Set up logging with request ID tracking.
    
    Args:
        name (str): The name for the logger
        level (Optional[int]): The logging level. Defaults to INFO if not specified.
        
    Returns:
        logging.Logger: Configured logger instance
    """
    if level is None:
        level = logging.INFO
        
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] - %(message)s'
    )
    logger = logging.getLogger(name)
    logger.addFilter(RequestIdFilter())
    return logger 