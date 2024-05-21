import json

from flask import Flask
from flask import request
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
import aria2p
from flask_cors import CORS, cross_origin
from cassandra.auth import PlainTextAuthProvider

app = Flask(__name__)
# CORS(app,  resources={
#    r"/*": {
#        "origins": ["http://example.com", "http://www.example.com"],
#        "methods": ["GET", "POST"],
#        "headers": ["Content-Type", "Authorization"]
#    }})
CORS(app)
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
prepared = instance.prepare(query="insert into movie.resource (movieId, resource, gid) values (?,?,?)")
prepared_query = instance.prepare("select * from movie.resource where movieId = ?")
prepared_query_for_download = instance.prepare("select * from movie.resource where movieId=? and resource=?")
prepared_delete = instance.prepare("delete from movie.resource where movieId = ? and resource = ?")


# add_source.consistency_level = ConsistencyLevel.LOCAL_ONE

@app.route('/movie/get_source', methods=['POST'])
@cross_origin()
def get_source():
    movieId = request.form["movieId"]

    if movieId is None:
        return "error"
    result = instance.execute(prepared_query, [movieId])
    result_list = []

    gids = []
    for (movieid, resource, gid) in result:
        if gid is None or len(gid) == 0:
            result_list.append({"movieId": movieid, "source": resource, "gid": gid})
            continue

        try:
            download = aria.get_download(gid)
        except:
            result_list.append({"movieId": movieid, "source": resource, "gid": ""})
            continue;
        result_list.append({"movieId": movieid, "source": resource, "gid": gid, "status": download.status})

    return json.dumps(result_list, ensure_ascii=False)


@app.route('/movie/add_source', methods=['POST'])
@cross_origin()
def add_source():
    print(request.method)
    movieId = request.form["movieId"]
    source = request.form["source"]
    print(movieId)
    print(source)
    if movieId is None or source is None:
        return "error"
    print(movieId)
    print(source)
    instance.execute(prepared.bind((movieId, source, "")))

    return "success"


@app.route('/movie/remove_source', methods=['POST'])
@cross_origin()
def remove_source():
    movieId = request.form["movieId"]
    source = request.form["source"]
    print(movieId)
    print(source)
    if movieId is None or source is None:
        return "error"
    instance.execute(prepared_delete.bind((movieId, source)))
    return "success"


@app.route('/movie/start', methods=['POST'])
@cross_origin()
def download():
    movieId = request.form["movieId"]
    source = request.form["source"]
    print([movieId, source])
    rows = instance.execute(prepared_query_for_download, [movieId, source])
    if rows.one()[2] is not None:
        return "exist!"
    if source.split(":") == "magnet":
        download = aria.add_magnet(source)
        instance.execute(prepared, [movieId, source, download.gid])
        return download.gid
    else:
        download = aria.add_uris([source])
        instance.execute(prepared, [movieId, source, download.gid])
        return download.gid


@app.route('/movie/pause', methods=['POST'])
@cross_origin()
def pause():
    gid = request.form["gid"]
    # instance.execute(prepared, [movieId, source, "paused"])
    download = aria.get_download(gid)
    aria.pause(download)


@app.route("/movie/batch_pause", methods=['POST'])
@cross_origin()
def batch_pause():
    movies = request.get_json()["downloads"]
    print("---------")
    gids = []
    for movie in movies:
        print(movie)
        # instance.execute(prepared, (movie["movieId"], movie["source"], "paused"))
        gids.append(movie["gid"])

    downloads = aria.get_downloads(gids)
    aria.pause(downloads)
    return "success"


@app.route('/movie/batch_resume', methods=['POST'])
@cross_origin()
def batch_resume():
    movies = request.get_json()["downloads"]
    gids = []
    for movie in movies:
        print(movie)
        # instance.execute(prepared, [movie["movieId"], movie["source"], "downloading"])
        gids.append(movie["gid"])
    downloads = aria.get_downloads(gids)
    aria.resume(downloads)
    return "success"


@app.route('/movie/resume', methods=['POST'])
@cross_origin()
def resume():
    gid = request.form["gid"]
    download = aria.get_downloads(gid)
    aria.resume(download)
    return "success"


@app.route('/movie/batch_stop', methods=['POST'])
@cross_origin()
def batch_stop():
    movies = request.get_json()["downloads"]
    gids = []
    for movie in movies:
        print(movie)
        # instance.execute(prepared, [movie["movieId"], movie["source"], "downloading"])
        gids.append(movie["gid"])
    downloads = aria.get_downloads(gids)
    aria.resume(downloads)


@app.route('/movie/batch_remove', methods=['POST'])
@cross_origin()
def batch_remove():
    movies = request.get_json()["downloads"]
    print(movies)
    gids = []
    for movie in movies:
        print(movie)
        # instance.execute(prepared, [movie["movieId"], movie["source"], "downloading"])
        gids.append(movie["gid"])
    downloads = aria.get_downloads(gids)
    aria.remove(downloads)
    return "success"


@app.route('/movie/remove', methods=['POST'])
@cross_origin()
def remove():
    movieId = request.form["movieId"]
    gid = request.form["gid"]
    source = request.form["source"]
    # instance.execute(prepared, [movieId, source, "canceled"])
    download = aria.get_download(gid)
    aria.remove([download])


@app.route('/movie/get_download_status', methods=['GET'])
@cross_origin()
def get_download_status():
    downloads = aria.get_downloads()
    result_list = []
    for download in downloads:
        download.update()
        result_list.append({"name": download.name, "speed": download.download_speed, "gid": download.gid,
                            "total_size": download.total_length, "complete_size": download.completed_length,
                            "status": download.status})
        print(download.name, download.download_speed, download.gid)
    return json.dumps(result_list, ensure_ascii=False)


if __name__ == '__main__':
    app.run(host="0.0.0.0")
