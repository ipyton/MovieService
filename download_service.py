#encoding=utf-8
import json

from flask import Flask, Blueprint
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
import os
import subprocess
import utils.FileManipulator as fileManipulator

minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)
bucket_name = "longvideos"
directory_path = "./processed"

download_bp = Blueprint('download', __name__, url_prefix='/')

# CORS(app,  resources={
#    r"/*": {
#        "origins": ["http://example.com", "http://www.example.com"],
#        "methods": ["GET", "POST"],
#        "headers": ["Content-Type", "Authorization"]
#    }})
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
handling_set = set()
auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider, )
instance = cluster.connect("movie")
instance.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
prepared = instance.prepare(query="insert into movie.resource (resource_id, type, season_id, episode,resource, name, gid, status,quality) values (?,?,?, ?,?,?,?,?,?)")
prepared_query = instance.prepare("select * from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ?")
prepared_query_for_download = instance.prepare("select * from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ? and resource = ?")
prepared_delete = instance.prepare("delete from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ? and resource = ?")
prepared_set_status = instance.prepare("update movie.resource set status = ? where resource_id  = ? and type = ? and season_id = ? and episode = ? and resource=? and quality = ?")
update_gid = instance.prepare("update movie.resource set gid = ? where resource_id = ? and type = ? and season_id = ? and episode = ? and resource = ? and quality = ?")
get_file_path = instance.prepare("select * from movie.playable where resource_id = ? and type = ? and season_id = ? and episode = ? and quality = ?")

delete_playable = instance.prepare("delete  from movie.playable where resource_id = ? and type = ? and season_id = ? and episode = ? and quality = ? and episode = ?")



# add_source.consistency_level = ConsistencyLevel.LOCAL_ONE

support_format = []
@download_bp.route('/movie/get_sources', methods=['POST'])
@cross_origin()
def get_sources():
    data = request.get_json()
    resource_id = data.get("resourceId")
    type = data.get("type")
    season_id = data.get("seasonId")
    episode = data.get("episode")
    print(resource_id is None)
    print(type is None,flush=True)
    if resource_id is None or type is None or season_id is None or episode is None:
        print("resourceId is null", flush=True)
        return "error", 400  # Return 400 explicitly if movieId is missing
    result = instance.execute(prepared_query, [resource_id,type,season_id,episode])

    result_list = []
    gids = []
    # for i in result:
    #     print(i, flush=True)
    for (resource_id, type, resource, name, gid, status) in result:
        print(resource_id, resource, name, gid, status, flush=True)
        if gid != None and len(gid):
            try:
                download = aria.get_download(gid)
            except:
                result_list.append({"resourceId": resource_id,"type":type,  "source": resource, "gid": "", "status": status})
                continue
        else:
            result_list.append({"resourceId": resource_id,"type":type,  "source": resource, "gid": gid, "status": status})

    print(result_list, flush=True)
    return json.dumps(result_list, ensure_ascii=False)

@download_bp.route('/movie/add_source', methods=['POST'])
@cross_origin()
def add_source():
    json = request.get_json()
    resource_id = json.get("resourceId")
    source = json.get("source")
    name = json.get("name")
    type = json.get("type")
    if resource_id is None or type is None or source is None or name is None :
        print("resourceId is null", flush=True)
        return "error", 400

    instance.execute(prepared.bind((resource_id.strip(),type, source.strip(), name.strip(), "" , "init")))

    return "success"


@download_bp.route('/movie/remove_source', methods=['POST'])
@cross_origin()
def remove_source():
    json = request.get_json()

    resourceId = json.get("resourceId")
    type = json.get("type")
    source = json.get("source")
    if resourceId is None or source is None or type is None :
        return "error"
    instance.execute(prepared_delete.bind((resourceId, type, source)))
    return "success"



@download_bp.route('/movie/get_files', methods=['POST'])
@cross_origin()
def get_files():
    gid = request.form["gid"]
    movieId = request.form["movieId"]
    resource = request.form["resource"]
    try:
        status = aria.get_download(gid)
    except Exception as e:
        return {"files":"invalid gid"}
    if status is None:
        return "404"
    elif status.followed_by_ids:
        gid = status.followed_by_ids[0]
        instance.execute(update_gid.bind((gid,movieId, resource)))
    else:
        print(status.status)
        result = {"status" :"getting", "total_size": status.total_length, "complete_size": status.completed_length}
        return json.dumps(result, ensure_ascii=False)

    files = aria.get_download(gid).files
    files = list(files)
    files = [{"file":file.index,"path":file.path.name,"size":file.length} for file in files]
    print(files,flush=True)
    return {"files":files, "gid":gid}





@download_bp.route('/movie/select', methods=['POST'])
@cross_origin()
def select_download():
    gid = request.form["gid"]
    select = request.form["place"]
    print(select, flush=True)
    resource = request.form["resource"]
    movieId = request.form["movieId"]
    file_to_rename = aria.get_download(gid).files[int(select) - 1]

    print("path" + "./cache/"+ movieId+ "/"+ resource)
    aria.client.change_option(gid, {"select-file": select,"index": file_to_rename.index,
    "dir": "./cache/"+ movieId+ "/"+ resource,"out":gid,"seed-time":0})
    aria.client.unpause(gid)
    instance.execute(prepared_set_status.bind(("downloading",movieId.strip(), resource.strip())))
    return "success"

@download_bp.route('/movie/start', methods=['POST'])
@cross_origin()
def download():
    movieId = request.form["movieId"]
    source = request.form["source"]
    name = request.form["name"]
    print([movieId, source])
    rows = instance.execute(prepared_query_for_download, [movieId, source])
    rows = list(rows)
    if len(rows) !=0 and rows[0].status == "finished":
        return "success"

    source= source.strip()
    if source.split(":")[0] == "magnet":
        print("path0" + "./cache/"+ movieId+ "/"+ source)
        if not os.path.exists("./cache/"+ movieId+ "/"+ source):
            os.makedirs("./cache/"+ movieId+ "/"+ source)
        download = aria.add_magnet(source,options={"dir":"./cache/"+ movieId+ "/"+ source, "pause-metadata": "true"})
        instance.execute(prepared, [movieId, source,name, download.gid,"downloading_meta"])
        return download.gid
    else:
        download = aria.add_uris([source])
        instance.execute(prepared, [movieId, source, download.gid])
        return download.gid


@download_bp.route('/movie/pause', methods=['POST'])
@cross_origin()
def pause():
    gid = request.form["gid"]
    download = aria.get_download(gid)
    aria.pause(download)


@download_bp.route("/movie/batch_pause", methods=['POST'])
@cross_origin()
def batch_pause():
    movies = request.get_json()["downloads"]
    gids = []
    for movie in movies:
        print(movie)
        gids.append(movie["gid"])

    downloads = aria.get_downloads(gids)
    aria.pause(downloads)
    return "success"


@download_bp.route('/movie/batch_resume', methods=['POST'])
@cross_origin()
def batch_resume():
    movies = request.get_json()["downloads"]
    gids = []
    for movie in movies:
        print(movie)
        gids.append(movie["gid"])
    downloads = aria.get_downloads(gids)
    aria.resume(downloads)
    return "success"



@download_bp.route('/movie/resume', methods=['POST'])
@cross_origin()
def resume():
    gid = request.form["gid"]
    download = aria.get_downloads(gid)
    aria.resume(download)
    return "success"


@download_bp.route('/movie/batch_stop', methods=['POST'])
@cross_origin()
def batch_stop():
    movies = request.get_json()["downloads"]
    gids = []
    for movie in movies:
        print(movie)
        gids.append(movie["gid"])
    downloads = aria.get_downloads(gids)
    aria.resume(downloads)


@download_bp.route('/movie/batch_remove', methods=['POST'])
@cross_origin()
def batch_remove():
    movies = request.get_json()["downloads"]
    print(movies)
    gids = []
    for movie in movies:
        print(movie)
        gids.append(movie["gid"])
    downloads = aria.get_downloads(gids)
    aria.remove(downloads)
    return "success"


@download_bp.route('/movie/remove', methods=['POST'])
@cross_origin()
def remove():
    movieId = request.form["movieId"]
    gid = request.form["gid"]
    source = request.form["source"]
    download = aria.get_download(gid)
    aria.remove([download])

@download_bp.route("/movie/delete_file")
@cross_origin()
def delete_file(resourceId, type, quality,episode):
    if resourceId is None or type is None or quality is None:
        return -1, "error"
    path = instance.execute(get_file_path, (resourceId, type, quality))
    results = path.all()
    if len(results) == 0:
        return -1, "error"
    if results[0].get("path") is None:
        return -1, "error"
    result = fileManipulator.delete_files_in_minio(results[0]["bucket"], results[0]["path"])
    instance.execute(delete_playable,type,quality, episode)
    if result is None or result is False:
        return -1, "error"
    return 0,"success"
    # delete_file(results[0]["bucket"],results[0]["path"])



@download_bp.route('/movie/get_download_status', methods=['GET'])
@cross_origin()
def get_download_status():
    downloads = aria.get_downloads()
    result_list = []
    for download in downloads:
        download.update()
        result_list.append({"name": download.name, "speed": download.download_speed, "gid": download.gid,
                            "total_size": download.total_length, "complete_size": download.completed_length,
                            "status": "complete" if download.total_length == download.completed_length else download.status})
        print(download.name, download.download_speed, download.gid)
    return json.dumps(result_list, ensure_ascii=False)

def upload(result):
    (download, idx, output_path) = result
    print(download, idx, output_path)
    for root, dirs, files in os.walk(output_path):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, root)
            print(root, flush=True)
            print(file_path,  flush=True)
            try:
                minio_client.fput_object(
                    bucket_name, str(file_path)[2:], file_path
                )
                print(f"File {file} uploaded successfully",flush=True)
            except S3Error as e:
                print(f"Failed to upload {file}. Error: {e}",flush=True)
            except Exception as e:
                print(e,flush=True)
    movieId = "/" + "/".join(str(download.dir).split("/")[1:-1])
    print(movieId)

    instance.execute(prepared_set_status, ["finished", movieId,str(download.dir).split("/")[-1]])
    print("success", flush=True)
    aria.remove([download],force=True)
    print("fi", flush=True)


def get_codec_name(video_path):
    command = [
        'ffprobe', '-v', 'error', '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name', '-of', 'default=nw=1:nk=1', video_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return result.stdout.strip()

# def encode(download):
#     if download is None:
#         return None
#
#     for file in download.files:
#         if file.selected and not file.is_metadata:
#             idx = file.index
#             print(file.path,flush=True)
#             output_path = "./processed"+str(download.dir)
#             if not os.path.exists(output_path):
#                 os.makedirs(output_path)
#         input_path = os.path.join(input_dir, filename)
#         if os.path.isfile(input_path):
#             file_base, _ = os.path.splitext(filename)
#             output_path = os.path.join(output_dir, f"{file_base}.m3u8")
#             segment_path = os.path.join(output_dir, f"{file_base}_segment_%03d.ts")
#
#             codec = get_codec_name(input_path)
#
#             if codec == 'h264':
#                 command = [
#                     'ffmpeg', '-i', input_path, '-c', 'copy', '-hls_time', '10', '-hls_list_size', '0',
#                     '-hls_segment_filename', segment_path, output_path
#                 ]
#             else:
#                 command = [
#                     'ffmpeg', '-i', input_path, '-c:v', 'h264_nvenc', '-c:a', 'aac', '-strict', '-2',
#                     '-hls_time', '10', '-hls_list_size', '0', '-hls_segment_filename', segment_path, output_path
#                 ]
#
#             subprocess.run(command, check=True)
#
#     print(f"All files have been processed and saved to {output_dir}.")

def encode(download):
    if download is None:
        return None
    print(download.gid,flush=True)
    idx = 0
    for file in download.files:
        if file.selected and not file.is_metadata:
            idx = file.index
            print("file path" + str(file.path),flush=True)
            output_path = "./processed/"+ "/".join(str(download.dir).split("/")[1:])
            print(output_path,flush=True)
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            meta = ffmpeg.probe("./" + str(file.path))
            for stream in meta["streams"]:
                if stream['codec_name'] == 'h264':
                    ffmpeg.input(os.path.join("./", file.path)).output(os.path.join(output_path, "index.m3u8"),
                                                            format="hls",
                                                            hls_time=10,
                                                            hls_list_size=0,
                                                            hls_segment_filename=os.path.join(output_path, "segment_%03d.ts"),
                                                            vcodec='copy',
                                                            acodec='copy'
                                                            ).run()
                    return (download, idx, output_path)
                else:
                    ffmpeg.input(os.path.join("./", file.path)).output(os.path.join(output_path, "index.m3u8"),
                                                            format="hls",
                                                            hls_time=10,
                                                            hls_list_size=0,
                                                            hls_segment_filename=os.path.join(output_path, "segment_%03d.ts"),
                                                            vcodec='libx264',
                                                            acodec='copy'
                                                            ).run()
                    return (download, idx, output_path)

scheduler = sched.scheduler(time.time, time.sleep)

def check():
    downloads = aria.get_downloads()
    for download in downloads:
        if download.name in handling_set:
            continue
        download.update()
        if (download.total_length == download.completed_length and download.is_metadata == False and (download.status=="active" or download.status=="complete")):
            futures.append(executor.submit(encode, (download)))
            handling_set.add(download.name)

    for future in futures:
        print(future,flush=True)
        if future.done() and future.exception() is None:
            result = future.result()
            futures.remove(future)
            upload_executor.submit(upload,result)
        if future.exception() is not None:
            print(future.exception(), flush=True)
    print("scan")

    scheduler.enter(3, 1, check, ())


def start_scheduler():
    scheduler.enter(5, 1, check)
    scheduler.run()




