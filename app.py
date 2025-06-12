# -*- coding: utf-8 -*-
import json
import logging
import time
import traceback
import datetime
import threading
import requests
from flask import Flask, request, jsonify

from flask_cors import CORS, cross_origin
import sys
import io

import os

import timer
from download_service import download_bp
from meta_service import meta_bp
from prometheus_flask_exporter import PrometheusMetrics

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
os.environ['FLASK_ENV'] = 'development'
os.environ['FLASK_DEBUG'] = '1'

app = Flask(__name__)

app.register_blueprint(download_bp)
app.register_blueprint(meta_bp)

metrics = PrometheusMetrics(app)  # 自动暴露 /metrics

# Create a logger for the main application
logger = logging.getLogger(__name__)


class SpringStyleFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(record.created)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 保留毫秒，去除最后3位微秒

    def format(self, record):
        # 获取线程名
        thread_name = threading.current_thread().name
        # 格式化输出
        log_time = self.formatTime(record)
        level = f"{record.levelname:<5}"
        pid = os.getpid()
        module = f"{record.name:<30.30}"  # 最多30字符，左对齐
        msg = record.getMessage()
        return f"{log_time} {level} {pid} --- [{thread_name}] {module} : {msg}"


def configure_logger():
    """Configure logging for the entire application"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 清空原有 handler（避免重复输出）
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(SpringStyleFormatter())
    console_handler.setLevel(logging.INFO)

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
    logging.getLogger('werkzeug').setLevel(logging.WARNING)  # Reduce Flask's built-in logging
    logging.getLogger('requests').setLevel(logging.WARNING)  # Reduce requests logging
    logging.getLogger('urllib3').setLevel(logging.WARNING)  # Reduce urllib3 logging


configure_logger()


@app.before_request
def log_request_info():
    """Log incoming request information"""
    logger.info(f"Incoming request: {request.method} {request.path} from {request.remote_addr}")
    logger.debug(f"Request headers: {dict(request.headers)}")
    if request.is_json:
        logger.debug(f"Request JSON: {request.get_json()}")
    elif request.form:
        logger.debug(f"Request form data: {dict(request.form)}")


@app.after_request
def log_response_info(response):
    """Log outgoing response information"""
    logger.info(f"Response: {response.status_code} for {request.method} {request.path}")
    if response.status_code >= 400:
        logger.warning(f"Error response {response.status_code}: {response.get_data(as_text=True)[:200]}...")
    return response


@app.before_request
def authenticate():
    """Authentication middleware with comprehensive logging"""
    # 跳过 OPTIONS 请求，以便前端通过 CORS 预检
    if request.method == 'OPTIONS':
        logger.debug("Skipping authentication for OPTIONS request")
        return  # 不拦截预检请求，否则浏览器会报 401
    if request.path == "/metrics":
        return
    logger.info(f"Authenticating request to {request.path}")

    token = request.headers.get('token')
    if not token:
        logger.warning(f"Unauthorized access attempt to {request.path}: No token provided")
        return jsonify({"error": "Unauthorized: No token provided"}), 401

    try:
        logger.debug(f"Validating token for path: {request.path}")

        auth_request_data = {"path": request.path}
        logger.debug(f"Sending auth request: {auth_request_data}")

        response = requests.post(
            "http://localhost:8080/auth/hasPermission",
            headers={"token": token},
            json=auth_request_data,
            timeout=5  # Add timeout to prevent hanging
        )

        logger.debug(f"Auth server response status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Auth server returned status {response.status_code} for token validation")
            return jsonify({"error": "Auth server error"}), 500

        res_json = response.json()
        logger.debug(f"Auth server response: {res_json}")

        if res_json.get("code") == 0:
            logger.info(f"Authentication successful for {request.path}")
            return
        else:
            logger.warning(f"Authentication failed for {request.path}: Invalid token")
            return jsonify({"error": "Unauthorized: Invalid token"}), 401

    except requests.exceptions.Timeout:
        logger.error("Auth server timeout during token validation")
        return jsonify({"error": "Auth server timeout"}), 500
    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to auth server")
        return jsonify({"error": "Auth server unavailable"}), 500
    except requests.exceptions.RequestException as e:
        logger.error(f"Auth request failed: {str(e)}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error during authentication: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    logger.warning(f"404 Not Found: {request.method} {request.path}")
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"500 Internal Server Error: {str(error)}")
    logger.error(traceback.format_exc())
    return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    """Handle unexpected errors"""
    logger.critical(f"Unexpected error: {str(error)}")
    logger.critical(traceback.format_exc())
    return jsonify({"error": "An unexpected error occurred"}), 500


# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    return jsonify({"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}), 200


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    import signal

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        # Add cleanup code here if needed
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info("Starting Flask application")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Flask environment: {os.environ.get('FLASK_ENV', 'production')}")
        logger.info(f"Debug mode: {os.environ.get('FLASK_DEBUG', '0')}")
        logger.info("=" * 60)

        # Setup signal handlers for graceful shutdown
        setup_signal_handlers()

        # Start the timer service
        logger.info("Starting timer service...")
        timer.main()
        logger.info("Timer service started successfully")

        # Start the Flask application
        logger.info("Starting Flask server on 0.0.0.0:8081")
        app.run(host="0.0.0.0", port=8081, debug=True)

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error during application startup: {str(e)}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
    finally:
        logger.info("Application shutdown complete")