import logging
import os
import datetime
import threading

from app.core.config import settings


class SpringStyleFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(record.created)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Keep milliseconds, remove microseconds

    def format(self, record):
        # Get thread name
        thread_name = threading.current_thread().name
        # Format output
        log_time = self.formatTime(record)
        level = f"{record.levelname:<5}"
        pid = os.getpid()
        module = f"{record.name:<30.30}"  # Max 30 characters, left aligned
        msg = record.getMessage()
        return f"{log_time} {level} {pid} --- [{thread_name}] {module} : {msg}"


def configure_logger():
    """Configure logging for the entire application"""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL))

    # Clear existing handlers (avoid duplicate output)
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(SpringStyleFormatter())
    console_handler.setLevel(getattr(logging, settings.LOG_LEVEL))

    # File handler for persistent logging
    if not os.path.exists('logs'):
        os.makedirs('logs')

    file_handler = logging.FileHandler('logs/app.log', encoding='utf-8')
    file_handler.setFormatter(SpringStyleFormatter())
    file_handler.setLevel(logging.DEBUG)

    # Error file handler
    error_handler = logging.FileHandler('logs/error.log', encoding='utf-8')
    error_handler.setFormatter(SpringStyleFormatter())
    error_handler.setLevel(logging.ERROR)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)

    # Set specific loggers for different modules
    logging.getLogger('uvicorn').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING) 