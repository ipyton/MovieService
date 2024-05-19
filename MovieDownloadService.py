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
aria2 = aria2p.API(
    aria2p.Client(
        host="http://localhost",
        port=6800,
        secret=""
    )
)

instance = cluster.connect()

@app.route('/movie/add_source', methods=['POST'])
@cross_origin()
def add_source():
    movieId = request.args.get("movieId")
    source = request.args.get("source")
    instance.execute("insert into movie.resource (movieId, resource, status) values (?,?,?)", movieId, source, "init")
    aria2.get_downloads()
    return "success"



@app.route('/movie/start', methods=['POST'])
@cross_origin()
def download():
    movieId = request.args.get("movieId")
    source =request.args.get("source")
    instance.execute("insert into movie.resource (movieId, resource, status) values (?,?,?)", movieId, source,
                     "downloading")
    if source.split(":") == "magnet":
        aria2.add_magnet(source)
    else:
        aria2.add_uris([source])

@app.route('/movie/pause', methods=['POST'])
@cross_origin()
def pause():
    movieId = request.args.get("movieId")
    gid = request.args.get("gid")
    source =request.args.get("source")
    instance.execute("insert into movie.resource (movieId, resource, status) values (?,?,?)", movieId, source,
                     "canceled")
    download = aria2.get_download(gid)
    aria2.pause(download)

@app.route('/movie/resume', methods=['POST'])
@cross_origin()
def resume():
    movieId = request.args.get("movieId")
    gid = request.args.get("gid")
    source =request.args.get("source")
    instance.execute("insert into movie.resource (movieId, resource, status) values (?,?,?)", movieId, source,
                     "downloading")
    download = aria2.get_download(gid)
    aria2.resume(download)

def resume():
    movieId = request.args.get("movieId")
    gid = request.args.get("gid")
    source = request.args.get("source")
    instance.execute("insert into movie.resource (movieId, resource, status) values (?,?,?)", movieId, source,
                     "canceled")
    download = aria2.get_download(gid)
    aria2.remove(download)


@app.route('/movie/get_download_status', methods=['GET'])
@cross_origin()
def get_download_status():
    downloads = aria2.get_downloads()
    for download in downloads:
        print(download.name, download.download_speed)
    return "download status"


if __name__ == '__main__':
    app.run()
