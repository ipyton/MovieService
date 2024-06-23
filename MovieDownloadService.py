#encoding=utf-8
import json
import sys
import codecs
from flask import Flask
from flask import request
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
import aria2p
from flask_cors import CORS, cross_origin
from cassandra.auth import PlainTextAuthProvider
import sched
import time
import concurrent.futures
import ffmpeg
from minio import Minio
from minio.error import S3Error
import threading
import os
minio_client = Minio(
    "localhost",  # MinIO server地址
    access_key="admin",  # 替换为你的 access key
    secret_key="admin123",  # 替换为你的 secret key
    secure=True  # 如果使用的是HTTP则设置为False
)
bucket_name = "my-bucket"  # 替换为你的桶名称
directory_path = "path/to/your/folder"  # 替换为你的文件夹路径
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
        secret="your_secret_token"
    )
)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
futures = []
# profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_ONE)

auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider, )
instance = cluster.connect("movie")
instance.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
prepared = instance.prepare(query="insert into movie.resource (movieId, resource, name, gid, status) values (?,?,?,?,?)")
prepared_query = instance.prepare("select * from movie.resource where movieId = ?")
prepared_query_for_download = instance.prepare("select * from movie.resource where movieId=? and resource=?")
prepared_delete = instance.prepare("delete from movie.resource where movieId = ? and resource = ?")
prepared_set_status = instance.prepare("update movie.resource set status = ? where movieId = ? and resource=?")
update_gid = instance.prepare("update movie.resource set gid = ? where movieId = ? and resource = ?")

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
    for (movieid, resource,name, gid,status) in result:
        if gid is None or len(gid) == 0:
            result_list.append({"movieId": movieid, "source": resource,  "name":name})
            continue
        try:
            download = aria.get_download(gid)
        except:
            result_list.append({"movieId": movieid, "source": resource, "gid": ""})
            continue
        result_list.append({"movieId": movieid, "source": resource, "gid": gid, "status": status})

    return json.dumps(result_list, ensure_ascii=False)


@app.route('/movie/add_source', methods=['POST'])
@cross_origin()
def add_source():
    print(request.method)
    movieId = request.form["movieId"]
    source = request.form["source"]
    name = request.form["name"]
    print(movieId)
    print(source)
    if movieId is None or source is None:
        return "error"
    print(movieId)
    print(source)
    instance.execute(prepared.bind((movieId, source, name,"" , "init")))

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

@app.route('/movie/get_files', methods=['POST'])
@cross_origin()
def get_files():
    gid = request.form["gid"]
    movieId = request.form["movieId"]
    resource = request.form["resource"]
    status = aria.get_download(gid)
    if status is None:
        return "404"
    elif status.followed_by_ids:
        gid = status.followed_by_ids[0]
        instance.execute(update_gid.bind((gid,movieId, resource)))
    else:
        return "getting meta"

    files = aria.get_files(gid)
    return files


@app.route('/movie/select', methods=['POST'])
@cross_origin()
def select_download():
    gid = request.form["gid"]
    select = request.form["place"]
    resource = request.form["resource"]
    movieId = request.form["movieId"]
    file_to_rename = aria.get_files(gid)[int(select) - 1]
    aria.client.change_options(gid, {"select-file": select,"index": file_to_rename.index,
    "path": movieId+ ":" + resource})
    aria.client.unpause(gid)
    instance.execute(prepared_query.bind(("downloading",movieId, resource)))
    return "success"

@app.route('/movie/start', methods=['POST'])
@cross_origin()
def download():
    movieId = request.form["movieId"]
    source = request.form["source"]
    name = request.form["name"]
    print([movieId, source])
    rows = instance.execute(prepared_query_for_download, [movieId, source])
    if rows.status != "finished":
        return "success"
    source= source.strip()
    if source.split(":")[0] == "magnet":
        download = aria.add_magnet(source,options={"pause": "true"})
        instance.execute(prepared, [movieId, source,name, download.gid,"downloading_meta"])
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

def upload(result):
    (download, output_path, input_path) = result
    for root, dirs, files in os.walk(output_path):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, directory_path)
            try:
                minio_client.fput_object(
                    bucket_name, object_name, file_path
                )
                print(f"File {file} uploaded successfully")
            except S3Error as e:
                print(f"Failed to upload {file}. Error: {e}")
    movieId = download.name.split(":")[0]
    instance.execute(prepared_set_status, ["finished", movieId])


def encode(download):
    if download is None:
        return None
    name_segment = download.name.split(":")
    output_path = "processed/" + download.name + "/" + "index.m3u8"
    input_path = "processed/" + download.name + "/" + "segment_%03d.ts"
    ffmpeg.input(download.directory).output(output_path,
                                            format="hls",
                                            hls_time=10,
                                            hls_list_size=0,
                                            hls_segment_filename=input_path
                                            ).run()
    return (download, output_path,input_path)

scheduler = sched.scheduler(time.time, time.sleep)

def check():
    downloads = aria.get_downloads()
    for download in downloads:
        download.update()
        futures.append(executor.submit(encode, (download)))

    for future in futures:
        if future.done():
            upload_executor.submit(upload,future.result())

    print("scan")
    scheduler.enter(3, 1, check, ())


def start_scheduler():
    scheduler.enter(5, 1, check)
    scheduler.run()



if __name__ == '__main__':
    # thread = threading.Thread(target=start_scheduler)
    # thread.start()
    app.run(host="0.0.0.0",port=5001)
