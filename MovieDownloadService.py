import json

from flask import Flask
from flask import request
from cassandra.cluster import Cluster
import aria2p
from flask_cors import CORS, cross_origin

cluster = Cluster(['192.168.0.1', '192.168.0.2'])

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'


instance = cluster.connect()

@app.route('/movie/add_source', methods=['POST'])
@cross_origin()
def add_event():
    movieId = request.args.get("movieId")
    source = request.args.get("source")
    instance.execute("insert into movie.resource (movieId, resource, status) values (?,?,?)", movieId, source, "init")
    return "success"


@app.route('/movie/modify_event', methods=['POST'])
@cross_origin()
def modify_event():
    action = request.args.get("action")
    source =request.args.get("source")
    if action == "delete":


        pass
    elif action == "pause":


        pass
    elif action == "start":


        pass


@app.route('/movie/get_download_status', methods=['GET'])
@cross_origin()
def get_download_status():
    print(request.args.get("keyword"))
    return "download status"


if __name__ == '__main__':
    app.run()
