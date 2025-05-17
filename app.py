# -*- coding: utf-8 -*-
import json
import logging
import time
import traceback

from flask import Flask

from flask_cors import CORS, cross_origin
import sys
import io

import os

import timer
from download_service import download_bp
from meta_service import meta_bp

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
os.environ['FLASK_ENV'] = 'development'
os.environ['FLASK_DEBUG'] = '1'


app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*","methods": "*", "headers":"*"}})
app.config['CORS_HEADERS'] = 'Content-Type'
app.register_blueprint(download_bp)
app.register_blueprint(meta_bp)


@app.before_request
def before_request():
    timer.main()


if __name__ == "__main__":
    try:
        logging.info("Starting Kafka consumers")
        app = Flask(__name__)

        logging.info("Starting Flask application")
        app.run(host="0.0.0.0", port=8081)

    except Exception as e:
        logging.critical(f"Main process error: {e}")
        traceback.print_exc()
        print("Main process error", flush=True)
            # logging.critical(traceback.format_exc())