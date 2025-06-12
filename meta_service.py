from flask import Blueprint
import logging
import time
from confluent_kafka import Producer

from flask import request
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import requests
from bs4 import BeautifulSoup

import re
import MovieParser
import json

# Configure logger for this module
logger = logging.getLogger(__name__)

meta_bp = Blueprint('meta', __name__, url_prefix='/')

# Database connection setup with logging
logger.info("Initializing Cassandra connection...")
auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")

cluster = Cluster(['127.0.0.1'], port=9042,  # Ensure correct port
                  auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))

try:
    instance = cluster.connect()
    logger.info("Successfully connected to Cassandra cluster")
except Exception as e:
    logger.error(f"Failed to connect to Cassandra cluster: {str(e)}")
    raise

# Prepare statements with logging
logger.info("Preparing Cassandra statements...")
try:
    insert_meta = instance.prepare("insert into movie.meta (resource_id,poster, score, introduction, movie_name, tags,"
                                   " actor_list, release_year, level, picture_list, maker_list, genre_list,type,language,total_season) "
                                   " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    get_video_meta = instance.prepare("select * from movie.meta where resource_id = ? and type = ? and language = ?")
    getStared = instance.prepare("select * from movie.movieGallery where user_id = ? and resource_id=? and type = ?")
    get_play_information = instance.prepare(
        "select * from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ? and resource=?")
    get_playlist = instance.prepare(
        "select * from movie.playable where resource_Id = ? and type = ? and season_id = ? ;")
    insert_first_season_meta = instance.prepare(
        "insert into movie.season_meta (resource_id, type, season_id, total_episode) values(?, ?, ?, ?);")
    logger.info("Successfully prepared all Cassandra statements")
except Exception as e:
    logger.error(f"Failed to prepare Cassandra statements: {str(e)}")
    raise

# Kafka configuration with logging
KAFKA_BROKER = "localhost:9092"
KAFKA_GROUP_ID = "flask-consumer-group"

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'
}

# Global variable to store messages
messages = []

logger.info(f"Initializing Kafka producer with broker: {KAFKA_BROKER}")
try:
    # 创建 Producer 实例
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER
    })
    logger.info("Successfully initialized Kafka producer")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {str(e)}")
    raise


def delivery_report(err, msg):
    """回调函数，用于处理消息发送结果"""
    if err is not None:
        logger.error(f"Failed sending Kafka message: {err}")
    else:
        logger.info(f"Successfully sent Kafka message to topic: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def get_url_base():
    return "https://www.themoviedb.org"


def get_search_url(keyword, language):
    url = get_url_base() + "/search?query=" + keyword + "&language=" + language
    logger.debug(f"Generated search URL: {url}")
    return url


def get_detail_url(type, id, language):
    url = get_url_base() + "/" + type + "/" + id + "?language=" + language
    logger.debug(f"Generated detail URL: {url}")
    return url


def save_to_database(metadata):
    logger.info("save_to_database function called (not implemented)")
    pass


def resolveMeta(file, job):
    logger.debug("Starting HTML parsing with BeautifulSoup")
    try:
        soup = BeautifulSoup(file, "html.parser")
        result = job(soup)
        logger.debug("Successfully parsed HTML and executed job")
        return result
    except Exception as e:
        logger.error(f"Error parsing HTML: {str(e)}")
        raise


def requestDispatcher(method, request_url, header=None):
    logger.info(f"Making {method.upper()} request to: {request_url}")
    start_time = time.time()

    if header is None:
        header = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Cache-Control": "max-age=0",
            "If-None-Match": 'W/"5c0e0ef9ede84708f885069358e5a72b"',
            "Priority": 'u=0, i',
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": '?0',
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }

    try:
        result = requests.request(method, request_url, headers=header)
        elapsed_time = time.time() - start_time
        logger.info(f"Request completed in {elapsed_time:.3f}s - Status: {result.status_code}")

        if result.status_code != 200:
            logger.warning(f"Request returned non-200 status code: {result.status_code}")

        return result
    except requests.exceptions.RequestException as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Request failed after {elapsed_time:.3f}s: {str(e)}")
        return None
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Unexpected error during request after {elapsed_time:.3f}s: {str(e)}")
        return None


def get_meta_with_params(req_type, req_id, language):
    logger.info(f"Getting metadata for type: {req_type}, id: {req_id}, language: {language}")

    try:
        # Check database first
        logger.debug("Checking database for existing metadata...")
        result = instance.execute(get_video_meta.bind((req_id, req_type, language)))
        result_list = list(result)
        logger.debug(f"Database query returned {len(result_list)} results")

        if len(result_list) != 0:
            logger.info("Found existing metadata in database")
            parsed = MovieParser.parseMovie(result_list[0])

            # Check if starred
            user_id = request.args.get("userId")
            if user_id:
                logger.debug(f"Checking starred status for user: {user_id}")
                star_result = list(instance.execute(getStared.bind((user_id, req_type, req_id))))
                if len(star_result) != 0:
                    parsed["stared"] = True
                    logger.debug("Movie is starred by user")

            logger.info("Successfully retrieved metadata from database")
            return json.dumps(parsed, ensure_ascii=False)

        # Fetch from external source
        logger.info("No metadata found in database, fetching from external source")
        result = requestDispatcher("get", get_detail_url(req_type, req_id, language))

        if result is None:
            logger.error("Failed to fetch metadata from external source")
            return json.dumps([], ensure_ascii=False)

        def handler(soup_result):
            logger.debug("Starting metadata parsing from HTML")
            return_result = {}

            try:
                body = soup_result.body.div.main

                # Parse poster
                poster = body.find("img", {"class": "poster w-full"})
                if poster is not None:
                    poster = poster["src"]
                    logger.debug(f"Found poster: {poster}")

                # Parse score
                score = body.find("div", {"class": "user_score_chart"})
                if score is not None:
                    score = score["data-percent"]
                    logger.debug(f"Found score: {score}")

                # Parse introduction
                introduction = body.find("div", {"class": "overview"})
                if introduction is not None:
                    introduction = introduction.p.text
                    logger.debug("Found introduction")

                # Parse tag
                tag = body.find("h3", {"class": "tagline"})
                if tag is not None:
                    tag = tag.text
                    logger.debug(f"Found tag: {tag}")

                # Parse level
                level = body.find("span", {"class": "certification"})
                if level is not None:
                    level = level.text
                    logger.debug(f"Found level: {level}")

                # Parse movie name
                movie_name = None
                if body.find("section", {"class": "images inner"}) is not None:
                    movie_name = body.find("section", {"class": "images inner"}).section

                # Parse release year
                release_year = body.find("span", {"class": "tag release_date"})
                if release_year is not None:
                    release_year = release_year.text
                    logger.debug(f"Found release year: {release_year}")

                if movie_name is not None and movie_name.a is not None:
                    movie_name = movie_name.a.text
                    logger.debug(f"Found movie name: {movie_name}")

                # Parse actors
                actress = body.find("ol", {"class": "people scroller"})
                actressList = []

                if actress is not None:
                    actresses = actress.find_all("li", {"class": "card"})
                    logger.debug(f"Found {len(actresses)} actors")

                    for actress in actresses:
                        actorDetail = None
                        name = None
                        avatar = None
                        if actress.a is not None:
                            actorDetail = actress.a["href"]
                        if actress.img is not None:
                            avatar = actress.img["src"]
                            name = actress.img["alt"]
                        character = actress.find("p", {"class": "character"})
                        if character is not None:
                            character = character.text

                        actressList.append(
                            json.dumps({"actorDetailPage": actorDetail, "character": character, "avatar": avatar,
                                        "name": name},
                                       ensure_ascii=False))

                # Parse pictures
                pictures = body.find_all("div", {
                    "class": "backdrop glyphicons_v2 picture grey no_image_holder no_border no_border_radius"})
                picturesList = []
                for picture in pictures:
                    if picture.img is not None:
                        picturesList.append(picture.img["src"])
                logger.debug(f"Found {len(picturesList)} pictures")

                # Parse makers
                makers = body.find_all("li", {"class": "profile"})
                makersDict = {}
                if makers is not None:
                    for maker in makers:
                        info = maker.find_all("p")
                        if len(info) == 2:
                            makersDict[info[0].a.text] = info[1].text
                logger.debug(f"Found {len(makersDict)} makers")

                # Parse genres
                genresList = []
                genre = body.find("span", {"class": "genres"})
                if genre is not None:
                    genres = genre.find_all("a")
                    if genres is not None:
                        for genre in genres:
                            if genre.text:
                                genresList.append(genre.text)
                logger.debug(f"Found {len(genresList)} genres")

                # Build result
                return_result["poster"] = poster
                return_result["score"] = score
                return_result["introduction"] = introduction
                return_result["tag"] = tag
                return_result["movie_name"] = movie_name
                return_result["release_year"] = release_year
                return_result["level"] = level
                return_result["actressList"] = actressList
                return_result["pictureList"] = picturesList
                return_result["makerList"] = makersDict
                return_result["genre_list"] = genresList
                return_result["type"] = req_type
                return_result["language"] = language

                total_season = 1
                if req_type == "movie":
                    total_season = 0
                return_result["total_season"] = total_season

                # Save to database
                logger.info("Saving metadata to database")
                try:
                    instance.execute(insert_meta, (req_id, poster, score, introduction, movie_name, tag, actressList,
                                                   release_year, level, picturesList, makersDict, genresList, req_type,
                                                   language, total_season))

                    if req_type == "tv":
                        instance.execute(insert_first_season_meta, (req_id, req_type, 1, 1))
                        logger.debug("Inserted first season metadata for TV show")

                    logger.info("Successfully saved metadata to database")
                except Exception as e:
                    logger.error(f"Failed to save metadata to database: {str(e)}")
                    # Continue execution even if database save fails

                logger.info("Successfully parsed and processed metadata")
                return json.dumps(return_result, ensure_ascii=False)

            except Exception as e:
                logger.error(f"Error parsing metadata from HTML: {str(e)}")
                raise

        return resolveMeta(result.text, handler)

    except Exception as e:
        logger.error(f"Unexpected error in get_meta_with_params: {str(e)}")
        return json.dumps({"error": "Internal server error"}, ensure_ascii=False)


@meta_bp.route('/movie/get_meta', methods=['GET'])
def get_meta():  # get name of a movie.
    logger.info(f"GET /movie/get_meta - Request received from {request.remote_addr}")
    start_time = time.time()

    try:
        language = request.args.get("Accept-Language")
        req_type = request.args.get("type")
        req_id = request.args.get("id")
        user_id = request.args.get("userId")

        logger.info(f"Request parameters - type: {req_type}, id: {req_id}, language: {language}, userId: {user_id}")

        if language is None:
            language = "en-US"
            logger.debug("Language not provided, defaulting to en-US")

        if req_type is None or req_id is None:
            logger.warning("Missing required parameters (type or id)")
            return "parameters are not sufficient", 400

        if language != "en-US":
            language = "en-US"
            logger.debug("Language forced to en-US")

        result = get_meta_with_params(req_type, req_id, language)

        elapsed_time = time.time() - start_time
        logger.info(f"GET /movie/get_meta completed successfully in {elapsed_time:.3f}s")

        return result

    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"GET /movie/get_meta failed after {elapsed_time:.3f}s: {str(e)}")
        return "Internal server error", 500


@meta_bp.route("/movie/search", methods=['GET'])
def searchMovies():
    logger.info(f"GET /movie/search - Request received from {request.remote_addr}")
    start_time = time.time()

    try:
        keyword = request.args.get("keyword")
        page_number = request.args.get("page_number")
        accept_language = request.args.get("Accept-Language")

        logger.info(f"Search parameters - keyword: {keyword}, page: {page_number}, language: {accept_language}")

        if accept_language is None:
            accept_language = "en-US"
            logger.debug("Language not provided, defaulting to en-US")

        if not keyword:
            logger.warning("Search keyword not provided")
            return json.dumps({"error": "Keyword is required"}, ensure_ascii=False), 400

        result = requestDispatcher("get", get_search_url(keyword, accept_language))
        if result is None:
            logger.error("Failed to fetch search results")
            return json.dumps([], ensure_ascii=True)

        def handler(soup_result):
            logger.debug("Starting search results parsing")
            return_result = []

            try:
                search_container = soup_result.body.div.main.section.div.div.div.next_sibling.next_sibling.section.div.div
                movies_processed = 0

                for movie in search_container.children:
                    if (movie is None) or (len(str(movie).strip()) == 0) or (movie.div is None):
                        continue

                    try:
                        attributes = []
                        for detail in movie.div.children:
                            if (detail is None) or (len(str(detail).strip()) == 0):
                                continue
                            else:
                                attributes.append(detail)

                        if len(attributes) < 2:
                            continue

                        img_address = attributes[0].find("img")
                        detail_address = attributes[1].find("a", {"class": "result"})
                        translated_name = attributes[1].find("h2")
                        original_name = attributes[1].find("span", {"class": "title"})
                        release_date = attributes[1].find("span", {"class": "release_date"})
                        introduction = attributes[1].find("p")

                        # Extract values
                        release_date = release_date.text if release_date is not None else None
                        introduction = introduction.text if introduction is not None else None
                        detail_address = detail_address["href"] if detail_address is not None else None
                        img_address = img_address["src"] if img_address is not None else None
                        original_name = original_name.text if original_name is not None else None
                        translated_name = translated_name.next if translated_name is not None else None

                        if detail_address:
                            match = re.search(r"/([^/]+)/([^/?]+)", detail_address)
                            if match:
                                type = match.group(1)
                                id = match.group(2)

                                return_result.append({
                                    "image_address": img_address,
                                    "translated_name": translated_name,
                                    "original_name": original_name,
                                    "release_date": release_date,
                                    "introduction": introduction,
                                    "detail_address": detail_address,
                                    "type": type,
                                    "resource_id": id
                                })
                                movies_processed += 1
                            else:
                                logger.warning(f"Could not parse detail address: {detail_address}")

                    except Exception as e:
                        logger.warning(f"Error parsing individual movie result: {str(e)}")
                        continue

                logger.info(f"Successfully processed {movies_processed} search results")
                return json.dumps(return_result, ensure_ascii=False)

            except Exception as e:
                logger.error(f"Error parsing search results: {str(e)}")
                return json.dumps([], ensure_ascii=False)

        parsed_result = resolveMeta(result.text, handler)
        elapsed_time = time.time() - start_time
        logger.info(f"GET /movie/search completed successfully in {elapsed_time:.3f}s")

        return parsed_result

    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"GET /movie/search failed after {elapsed_time:.3f}s: {str(e)}")
        return json.dumps({"error": "Internal server error"}, ensure_ascii=False), 500


@meta_bp.route("/movie/get_play_information", methods=["GET"])
def get_play_information():
    logger.info(f"GET /movie/get_play_information - Request received from {request.remote_addr}")
    start_time = time.time()

    try:
        resourceId = request.args.get("resourceId")
        type = request.args.get("type")
        season_id = request.args.get("seasonId")

        logger.info(f"Play information parameters - resourceId: {resourceId}, type: {type}, seasonId: {season_id}")

        if resourceId is None or type is None or season_id is None:
            logger.warning("Missing required parameters for play information")
            return "parameters are not sufficient", 400

        logger.debug("Querying database for playlist information")
        rs = instance.execute(get_playlist, (resourceId, type, season_id))
        result = []

        for (resource_id, type, quality, bucket, path) in rs:
            result.append({
                "resource_id": resource_id,
                "type": type,
                "season_id": season_id,
                "quality": quality,
                "bucket": bucket,
                "path": path
            })

        elapsed_time = time.time() - start_time
        logger.info(
            f"GET /movie/get_play_information completed successfully in {elapsed_time:.3f}s - Returned {len(result)} items")

        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"GET /movie/get_play_information failed after {elapsed_time:.3f}s: {str(e)}")
        return json.dumps({"error": "Internal server error"}, ensure_ascii=False), 500