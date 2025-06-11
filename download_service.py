# encoding=utf-8
import json
import logging
import traceback

from flask import Flask, Blueprint
from flask import request
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
import aria2p
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

# Create logger for this module
logger = logging.getLogger(__name__)

# Initialize services with error handling and logging
try:
    minio_client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="admin123",
        secure=False
    )
    logger.info("MinIO client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize MinIO client: {str(e)}")
    raise

bucket_name = "longvideos"
directory_path = "./processed"

download_bp = Blueprint('download', __name__, url_prefix='/')

try:
    aria = aria2p.API(
        aria2p.Client(
            host="http://localhost",
            port=6800,
            secret="your_secret_token"
        )
    )
    logger.info("Aria2 client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Aria2 client: {str(e)}")
    raise

executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
futures = []
handling_set = set()

# Database connection with logging
try:
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
    instance = cluster.connect("movie")
    instance.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    logger.info("Cassandra connection established successfully")
except Exception as e:
    logger.error(f"Failed to connect to Cassandra: {str(e)}")
    raise

# Prepare statements
try:
    prepared = instance.prepare(
        query="insert into movie.resource (resource_id, type, season_id, episode,resource, name, gid, status,quality) values (?,?,?, ?,?,?,?,?,?)")
    prepared_query = instance.prepare(
        "select * from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ?")
    prepared_query_for_download = instance.prepare(
        "select * from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ? and resource = ?")
    prepared_delete = instance.prepare(
        "delete from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ? and resource = ?")
    prepared_set_status = instance.prepare(
        "update movie.resource set status = ? where resource_id  = ? and type = ? and season_id = ? and episode = ? and resource=? and quality = ?")
    update_gid = instance.prepare(
        "update movie.resource set gid = ? where resource_id = ? and type = ? and season_id = ? and episode = ? and resource = ? and quality = ?")
    get_file_path = instance.prepare(
        "select * from movie.playable where resource_id = ? and type = ? and season_id = ? and episode = ? and quality = ?")
    delete_playable = instance.prepare(
        "delete  from movie.playable where resource_id = ? and type = ? and season_id = ? and episode = ? and quality = ? and episode = ?")
    logger.info("Database prepared statements created successfully")
except Exception as e:
    logger.error(f"Failed to prepare database statements: {str(e)}")
    raise

support_format = []


@download_bp.route("/hello")
def hello():
    logger.info("Health check endpoint accessed")
    return "Hello, World!", 200


@download_bp.route('/movie/get_sources', methods=['POST'])
def get_sources():
    logger.info("Get sources request received")

    try:
        data = request.get_json()
        resource_id = data.get("resourceId")
        type = data.get("type")
        season_id = data.get("seasonId")
        episode = data.get("episode")

        logger.debug(
            f"Request parameters: resource_id={resource_id}, type={type}, season_id={season_id}, episode={episode}")

        if resource_id is None or type is None or season_id is None or episode is None:
            logger.warning("Missing required parameters in get_sources request")
            return "error", 400

        logger.info(f"Querying sources for resource_id={resource_id}, type={type}")
        result = instance.execute(prepared_query, [resource_id, type, season_id, episode])

        result_list = []
        for row in result:
            resource_id, type, resource, name, gid, status = row[:6]  # Handle different row structures
            logger.debug(f"Processing source: {resource_id}, {resource}, {name}, {gid}, {status}")

            if gid and len(gid):
                try:
                    download = aria.get_download(gid)
                    result_list.append(
                        {"resourceId": resource_id, "type": type, "source": resource, "gid": gid, "status": status})
                except Exception as e:
                    logger.warning(f"Failed to get download for gid {gid}: {str(e)}")
                    result_list.append(
                        {"resourceId": resource_id, "type": type, "source": resource, "gid": "", "status": status})
            else:
                result_list.append(
                    {"resourceId": resource_id, "type": type, "source": resource, "gid": gid, "status": status})

        logger.info(f"Returning {len(result_list)} sources")
        return json.dumps(result_list, ensure_ascii=False)

    except Exception as e:
        logger.error(f"Error in get_sources: {str(e)}")
        logger.error(traceback.format_exc())
        return "Internal server error", 500


@download_bp.route('/movie/add_source', methods=['POST'])
def add_source():
    logger.info("Add source request received")

    try:
        data = request.get_json()
        resource_id = data.get("resourceId")
        source = data.get("source")
        name = data.get("name")
        type = data.get("type")

        logger.debug(f"Adding source: resource_id={resource_id}, type={type}, source={source}, name={name}")

        if resource_id is None or type is None or source is None or name is None:
            logger.warning("Missing required parameters in add_source request")
            return "error", 400

        instance.execute(prepared.bind((resource_id.strip(), type, source.strip(), name.strip(), "", "init")))
        logger.info(f"Source added successfully for resource_id={resource_id}")
        return "success"

    except Exception as e:
        logger.error(f"Error in add_source: {str(e)}")
        logger.error(traceback.format_exc())
        return "Internal server error", 500


@download_bp.route('/movie/remove_source', methods=['POST'])
def remove_source():
    logger.info("Remove source request received")

    try:
        data = request.get_json()
        resourceId = data.get("resourceId")
        type = data.get("type")
        source = data.get("source")

        logger.debug(f"Removing source: resourceId={resourceId}, type={type}, source={source}")

        if resourceId is None or source is None or type is None:
            logger.warning("Missing required parameters in remove_source request")
            return "error", 400

        instance.execute(prepared_delete.bind((resourceId, type, source)))
        logger.info(f"Source removed successfully for resourceId={resourceId}")
        return "success"

    except Exception as e:
        logger.error(f"Error in remove_source: {str(e)}")
        logger.error(traceback.format_exc())
        return "Internal server error", 500


@download_bp.route('/movie/get_files', methods=['POST'])
def get_files():
    logger.info("Get files request received")

    try:
        gid = request.form["gid"]
        movieId = request.form["movieId"]
        resource = request.form["resource"]

        logger.debug(f"Getting files for gid={gid}, movieId={movieId}")

        try:
            status = aria.get_download(gid)
        except Exception as e:
            logger.warning(f"Invalid gid {gid}: {str(e)}")
            return {"files": "invalid gid"}

        if status is None:
            logger.warning(f"No download found for gid {gid}")
            return "404"
        elif status.followed_by_ids:
            new_gid = status.followed_by_ids[0]
            logger.info(f"Download gid changed from {gid} to {new_gid}")
            instance.execute(update_gid.bind((new_gid, movieId, resource)))
            gid = new_gid
        else:
            result = {"status": "getting", "total_size": status.total_length, "complete_size": status.completed_length}
            logger.debug(f"Download in progress: {result}")
            return json.dumps(result, ensure_ascii=False)

        files = aria.get_download(gid).files
        files = list(files)
        files = [{"file": file.index, "path": file.path.name, "size": file.length} for file in files]

        logger.info(f"Returning {len(files)} files for gid {gid}")
        return {"files": files, "gid": gid}

    except KeyError as e:
        logger.error(f"Missing required form parameter: {str(e)}")
        return "Missing required parameter", 400
    except Exception as e:
        logger.error(f"Error in get_files: {str(e)}")
        logger.error(traceback.format_exc())
        return "Internal server error", 500


@download_bp.route('/movie/get_download_status', methods=['GET'])
def get_download_status():
    logger.info("Get download status request received")

    try:
        downloads = aria.get_downloads()
        result_list = []

        for download in downloads:
            download.update()
            status = "complete" if download.total_length == download.completed_length else download.status
            result_list.append({
                "name": download.name,
                "speed": download.download_speed,
                "gid": download.gid,
                "total_size": download.total_length,
                "complete_size": download.completed_length,
                "status": status
            })
            logger.debug(f"Download status: {download.name}, {status}, {download.download_speed}")

        logger.info(f"Returning status for {len(result_list)} downloads")
        return json.dumps(result_list, ensure_ascii=False)

    except Exception as e:
        logger.error(f"Error in get_download_status: {str(e)}")
        logger.error(traceback.format_exc())
        return "Internal server error", 500


def upload(result):
    """Upload processed files to MinIO with comprehensive logging"""
    logger.info("Starting file upload process")

    try:
        (download, idx, output_path) = result
        logger.info(f"Uploading files from {output_path}")

        uploaded_files = 0
        for root, dirs, files in os.walk(output_path):
            for file in files:
                file_path = os.path.join(root, file)
                object_name = str(file_path)[2:]  # Remove "./" prefix

                try:
                    minio_client.fput_object(bucket_name, object_name, file_path)
                    uploaded_files += 1
                    logger.debug(f"File uploaded successfully: {file}")
                except S3Error as e:
                    logger.error(f"S3 error uploading {file}: {str(e)}")
                except Exception as e:
                    logger.error(f"Unexpected error uploading {file}: {str(e)}")

        movieId = "/" + "/".join(str(download.dir).split("/")[1:-1])
        resource_name = str(download.dir).split("/")[-1]

        # Update status in database
        instance.execute(prepared_set_status, ["finished", movieId, resource_name])
        logger.info(f"Upload completed: {uploaded_files} files uploaded for {movieId}")

        # Clean up
        aria.remove([download], force=True)
        logger.info(f"Download {download.gid} removed from aria2")

    except Exception as e:
        logger.error(f"Error in upload process: {str(e)}")
        logger.error(traceback.format_exc())


def encode(download):
    """Encode video files to HLS format with logging"""
    if download is None:
        logger.warning("Encode called with None download")
        return None

    logger.info(f"Starting encoding for download {download.gid}")

    try:
        idx = 0
        for file in download.files:
            if file.selected and not file.is_metadata:
                idx = file.index
                logger.info(f"Encoding file: {file.path}")

                output_path = "./processed/" + "/".join(str(download.dir).split("/")[1:])
                logger.debug(f"Output path: {output_path}")

                if not os.path.exists(output_path):
                    os.makedirs(output_path)
                    logger.debug(f"Created output directory: {output_path}")

                try:
                    meta = ffmpeg.probe("./" + str(file.path))

                    # Check codec and encode accordingly
                    for stream in meta["streams"]:
                        if stream.get('codec_name') == 'h264':
                            logger.info("Using copy codec for h264 stream")
                            ffmpeg.input(os.path.join("./", file.path)).output(
                                os.path.join(output_path, "index.m3u8"),
                                format="hls",
                                hls_time=10,
                                hls_list_size=0,
                                hls_segment_filename=os.path.join(output_path, "segment_%03d.ts"),
                                vcodec='copy',
                                acodec='copy'
                            ).run()
                        else:
                            logger.info("Using libx264 codec for non-h264 stream")
                            ffmpeg.input(os.path.join("./", file.path)).output(
                                os.path.join(output_path, "index.m3u8"),
                                format="hls",
                                hls_time=10,
                                hls_list_size=0,
                                hls_segment_filename=os.path.join(output_path, "segment_%03d.ts"),
                                vcodec='libx264',
                                acodec='copy'
                            ).run()

                        logger.info(f"Encoding completed for {file.path}")
                        return (download, idx, output_path)

                except Exception as e:
                    logger.error(f"FFmpeg encoding error: {str(e)}")
                    raise

    except Exception as e:
        logger.error(f"Error in encode process: {str(e)}")
        logger.error(traceback.format_exc())
        return None


scheduler = sched.scheduler(time.time, time.sleep)


def check():
    """Check for completed downloads and process them"""
    try:
        logger.debug("Checking for completed downloads")
        downloads = aria.get_downloads()

        for download in downloads:
            if download.name in handling_set:
                continue

            download.update()
            if (download.total_length == download.completed_length and
                    download.is_metadata == False and
                    (download.status == "active" or download.status == "complete")):
                logger.info(f"Processing completed download: {download.name}")
                futures.append(executor.submit(encode, download))
                handling_set.add(download.name)

        # Process completed encoding tasks
        completed_futures = []
        for future in futures:
            if future.done():
                if future.exception() is None:
                    result = future.result()
                    if result:
                        logger.info("Submitting upload task")
                        upload_executor.submit(upload, result)
                    completed_futures.append(future)
                else:
                    logger.error(f"Encoding task failed: {future.exception()}")
                    completed_futures.append(future)

        # Remove completed futures
        for future in completed_futures:
            futures.remove(future)

    except Exception as e:
        logger.error(f"Error in check function: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Schedule next check
        scheduler.enter(3, 1, check, ())


def start_scheduler():
    """Start the download checking scheduler"""
    logger.info("Starting download scheduler")
    scheduler.enter(5, 1, check)
    scheduler.run()