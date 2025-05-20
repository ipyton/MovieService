# -*- coding: utf-8 -*-
import json
import logging
import time
import traceback

import requests
from flask import Flask, request, jsonify

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
def authenticate():
    # 跳过 OPTIONS 请求，以便前端通过 CORS 预检
    if request.method == 'OPTIONS':
        return  # 不拦截预检请求，否则浏览器会报 401

    token = request.headers.get('token')
    if not token:
        return jsonify({"error": "Unauthorized: No token provided"}), 401

    try:
        response = requests.post(
            "http://localhost:8080/auth/hasPermission",
            headers={"token": token},
            json={"path": request.path}  # 修复 body 参数
        )
        if response.status_code != 200:
            return jsonify({"error": "Auth server error"}), 500

        res_json = response.json()  # 修复 .get_json()
        print(res_json, flush=True)

        if res_json.get("code") == 0:
            return
        else:
            return jsonify({"error": "Unauthorized: Invalid token"}), 401

    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


if __name__ == "__main__":
    try:
        logging.info("Starting Flask application")
        timer.main()
        app.run(host="0.0.0.0", port=8081)


    except Exception as e:
        logging.critical(f"Main process error: {e}")
        traceback.print_exc()
        print("Main process error", flush=True)
            # logging.critical(traceback.format_exc())