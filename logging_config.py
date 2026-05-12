"""
Elasticsearch logging configuration for token optimizer.
Sends application logs to Elasticsearch for indexing and Kibana visualization.
"""

import logging
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from pythonjsonlogger import jsonlogger
from typing import Dict, Any

class ElasticsearchHandler(logging.Handler):
    """Custom logging handler for Elasticsearch"""
    
    def __init__(self, es_client: Elasticsearch, index_name: str = "token-optimizer"):
        super().__init__()
        self.es = es_client
        self.index_name = f"{index_name}-{datetime.now().strftime('%Y.%m.%d')}"
    
    def emit(self, record: logging.LogRecord):
        """Send log record to Elasticsearch"""
        try:
            doc = self.format_record(record)
            self.es.index(index=self.index_name, document=doc)
        except Exception:
            self.handleError(record)
    
    def format_record(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Format log record as JSON document"""
        return {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "extra": {
                "process_id": record.process,
                "thread_id": record.thread,
                "thread_name": record.threadName
            }
        }


def setup_logging(es_host: str = "localhost:9200", enable_console: bool = True) -> logging.Logger:
    """
    Configure logging with Elasticsearch backend.
    
    Args:
        es_host: Elasticsearch host:port
        enable_console: Also log to console
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("token_optimizer")
    logger.setLevel(logging.DEBUG)
    
    # Console handler with JSON formatting
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        json_formatter = jsonlogger.JsonFormatter()
        console_handler.setFormatter(json_formatter)
        logger.addHandler(console_handler)
    
    # Elasticsearch handler
    try:
        es = Elasticsearch([f"http://{es_host}"])
        es.info()  # Test connection
        es_handler = ElasticsearchHandler(es)
        es_handler.setLevel(logging.DEBUG)
        logger.addHandler(es_handler)
        logger.info("✅ Elasticsearch logging configured")
    except Exception as e:
        logger.warning(f"⚠️ Could not connect to Elasticsearch: {e}")
        logger.warning("Logging to console only")
    
    return logger


# Convenience loggers for different modules
def get_training_logger() -> logging.Logger:
    """Logger for training operations"""
    return logging.getLogger("token_optimizer.training")


def get_prediction_logger() -> logging.Logger:
    """Logger for prediction operations"""
    return logging.getLogger("token_optimizer.prediction")


def get_api_logger() -> logging.Logger:
    """Logger for API operations"""
    return logging.getLogger("token_optimizer.api")


if __name__ == "__main__":
    # Test logging setup
    logger = setup_logging()
    
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    print("✅ Logs should appear in Kibana at http://localhost:5601")
