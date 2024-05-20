import json

from flask import Flask
from flask import request
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
import aria2p
from flask_cors import CORS, cross_origin
from cassandra.auth import PlainTextAuthProvider

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'
aria = aria2p.API(
    aria2p.Client(
        host="http://localhost",
        port=6800,
        secret=""
    )
)
# profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_ONE)

auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider, )
instance = cluster.connect("movie")
instance.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
prepared = instance.prepare(query="insert into movie.resource (movieId, resource, status) values (?,?,?)")
prepared_query = instance.prepare("select * from movie.resource where movieId = ?")


# add_source.consistency_level = ConsistencyLevel.LOCAL_ONE

@app.route('/movie/get_source', methods=['POST'])
@cross_origin()
def get_source():
    movieId = request.get_json()["movieId"]
    print(movieId)

    if movieId is None:
        return "error"
    result = instance.execute(prepared_query, [movieId])
    result_list = []
    for (movieid, resource, status) in result:
        result_list.append({"movieId": movieid, "resouce": resource, "status": status})
    return json.dumps(result_list, ensure_ascii=False)


@app.route('/movie/add_source', methods=['POST'])
@cross_origin()
def add_source():
    movieId = request.get_json()["movieId"]
    source = request.get_json()["source"]
    print(movieId)
    print(source)
    if movieId is None or source is None:
        return "error"
    instance.execute(prepared.bind((movieId, source, "init")))

    aria.get_downloads(None)
    return "success"


@app.route('/movie/start', methods=['POST'])
@cross_origin()
def download():
    movieId = request.get_json()["movieId"]
    source = request.get_json()["source"]
    instance.execute(prepared, [movieId, source, "downloading"])
    if source.split(":") == "magnet":
        download = aria.add_magnet(source)
        return download.gid
    else:
        download = aria.add_uris([source])
        return download.gid

@app.route('/movie/pause', methods=['POST'])
@cross_origin()
def pause():
    movieId = request.get_json()["movieId"]
    source = request.get_json()["source"]
    gid = request.get_json().get("gid")

    instance.execute(prepared, [movieId, source, "paused"])
    download = aria.get_download(gid)
    aria.pause(download)


@app.route('/movie/resume', methods=['POST'])
@cross_origin()
def resume():
    movieId = request.args.get("movieId")
    gid = request.args.get("gid")
    source = request.args.get("source")
    instance.execute(prepared, [movieId, source, "downloading"])
    download = aria.get_download(gid)
    aria.resume(download)


@app.route('/movie/remove', methods=['POST'])
@cross_origin()
def remove():
    movieId = request.args.get("movieId")
    gid = request.args.get("gid")
    source = request.args.get("source")
    instance.execute(prepared, [movieId, source, "canceled"])
    download = aria.get_download(gid)
    aria.remove([download])


@app.route('/movie/get_download_status', methods=['GET'])
@cross_origin()
def get_download_status():
    downloads = aria.get_downloads()
    result_list = []
    for download in downloads:
        download.update()
        result_list.append({"name": download.name, "speed": download.download_speed, "gid": download.gid, "total_size": download.total_length, "complete_size": download.completed_length})
        print(download.name, download.download_speed, download.gid)
    return json.dumps(result_list, ensure_ascii=False)


if __name__ == '__main__':
    app.run(host="0.0.0.0")
