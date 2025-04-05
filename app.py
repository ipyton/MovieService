# -*- coding: utf-8 -*-
import json
import logging
import time
import traceback

from confluent_kafka import KafkaException, Consumer, Producer

from flask import Flask
from flask import request
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import requests
from bs4 import BeautifulSoup
from flask_cors import CORS, cross_origin
import sys
import io

import re
import MovieParser
import os
import json


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
os.environ['FLASK_ENV'] = 'development'
os.environ['FLASK_DEBUG'] = '1'


auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")

cluster = Cluster(['127.0.0.1'],port=9042,  # Ensure correct port
    auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))


app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*","methods": "*", "headers":"*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

instance = cluster.connect()

insert_meta = instance.prepare("insert into movie.meta (resource_id,poster, score, introduction, movie_name, tags,"
                               " actor_list, release_year, level, picture_list, maker_list, genre_list,type,language,total_season) "
                               " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
get_video_meta = instance.prepare("select * from movie.meta where resource_id = ? and type = ? and language = ?")
getStared = instance.prepare("select * from movie.movieGallery where user_id = ? and resource_id=? and type = ?")
get_play_information = instance.prepare("select * from movie.resource where resource_id = ? and type = ? and season_id = ? and episode = ? and resource=?")
get_playlist = instance.prepare("select * from movie.playable where resource_Id = ? and type = ? and season_id = ? ;")

insert_first_season_meta = instance.prepare("insert into movie.season_meta (resource_id, type, season_id, total_episode) values(?, ?, ?, ?);")
KAFKA_BROKER = "localhost:9092"
KAFKA_GROUP_ID = "flask-consumer-group"

conf ={
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'
}

# Global variable to store messages
messages = []


# 创建 Producer 实例
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER
})

def delivery_report(err, msg):
    """回调函数，用于处理消息发送结果"""
    if err is not None:
        print(f"failed sending message: {err}",flush=True)
    else:
        print(f"successfully sent message: {msg.topic()} [{msg.partition()}] @ {msg.offset()}",flush = True)





def get_url_base():
    return "https://www.themoviedb.org"


def get_search_url(keyword, language):
    return get_url_base() + "/search?query=" + keyword + "&language=" + language


def get_detail_url(type, id, language):
    return get_url_base() + "/" + type + "/" + id + "?language=" + language


def save_to_database(metadata):
    pass


def resolveMeta(file, job):
    soup = BeautifulSoup(file, "html.parser")
    return job(soup)


def requestDispatcher(method, request_url, header=None):
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

    result=requests.request(method, request_url, headers=header)

    return result

def get_meta_with_params(type, id, language):
    result = instance.execute(get_video_meta.bind((id, type, language)))
    result_list = list(result)
    print(result_list, flush=True)
    if len(result_list) != 0:
        parsed = MovieParser.parseMovie(result_list[0])
        result = list(instance.execute(getStared.bind((request.args.get("userId"), type, id))))
        if len(result) != 0:
            parsed["stared"] = True
        return json.dumps(parsed, ensure_ascii=False)
    print("visit outside", flush=True)
    result = requestDispatcher("get", get_detail_url(type, id, language))
    print(get_detail_url(type, id, language),flush=True)
    if result is None:
        return json.dumps([], ensure_ascii=False)

    def handler(result):
        return_result = {}
        body = result.body.div.main
        poster = body.find("img", {"class": "poster w-full"})
        if poster is not None:
            poster = poster["src"]
        score = body.find("div", {"class": "user_score_chart"})
        if score is not None:
            score = score["data-percent"]
        introduction = body.find("div", {"class": "overview"})
        if introduction is not None:
            introduction = introduction.p.text
        tag = body.find("h3", {"class": "tagline"})
        if tag is not None:
            tag = tag.text

        level = body.find("span", {"class": "certification"})
        if level is not None:
            level = level.text

        movie_name = None
        if body.find("section", {"class": "images inner"}) is not None:
            movie_name = body.find("section", {"class": "images inner"}).section

        release_year = body.find("span", {"class": "tag release_date"})

        if release_year is not None:
            release_year = release_year.text

        if movie_name is not None and movie_name.a is not None:
            movie_name = movie_name.a.text

        actress = body.find("ol", {"class": "people scroller"})

        actressList = []

        if actress is not None:
            actresses = actress.find_all("li", {"class": "card"})
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
                    json.dumps({"actorDetailPage": actorDetail, "character": character, "avatar": avatar, "name": name},
                               ensure_ascii=False))
        # print(actressList)
        pictures = body.find_all("div", {
            "class": "backdrop glyphicons_v2 picture grey no_image_holder no_border no_border_radius"})
        picturesList = []
        for picture in pictures:
            if picture.img is not None:
                picturesList.append(picture.img["src"])

        makers = body.find_all("li", {"class": "profile"})
        makersDict = {}
        if makers is not None:
            for maker in makers:
                info = maker.find_all("p")
                if len(info) == 2:
                    makersDict[info[0].a.text] = info[1].text
        genresList = []
        genre = body.find("span", {"class": "genres"})
        if genre is not None:
            genres = genre.find_all("a")
            if genres is not None:
                for genre in genres:
                    if genre.text:
                        genresList.append(genre.text)
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
        return_result["type"] = type
        return_result["language"] = language
        total_season = 1
        if type == "movie":
            total_season = 0
        return_result["total_season"] = total_season
        # insert_meta = instance.prepare("insert into movie.meta (movieId,poster, score, introduction, movie_name, tags, actress_list, release_year, level, picture_list, maker_list, genre_list)  values(?,?,?,?,?,?,?,?,?,?,?,?)")
        instance.execute(insert_meta, (id, poster, score, introduction, movie_name, tag, actressList,
                                       release_year, level, picturesList, makersDict, genresList, type, language, total_season))
        if (type == "tv"):
            instance.execute(insert_first_season_meta, (id, type, 1, 1))
        return json.dumps(return_result, ensure_ascii=False)

    return resolveMeta(result.text, handler)


@app.route('/movie/get_meta', methods=['GET'])
@cross_origin()
def get_meta():  # get name of a movie.
    # print(get_detail_url(request.args.get("detail_address")))
    language = request.args.get("Accept-Language")
    print(language, flush=True)
    if language is None:
        language = "en-US"
    type = request.args.get("type")
    id = request.args.get("id")
    if type is None or id is None:
        print("get meta error", flush=True)
        return "parameters are not sufficient"

    result = get_meta_with_params(type, id, language)
    if language != "en-US":
        get_meta_with_params(type, id, "en-US")
    return result


@app.route("/movie/search", methods=['GET'])
@cross_origin()
def searchMovies():
    keyword = request.args.get("keyword")
    page_number = request.args.get("page_number")
    accept_language = request.args.get("Accept-Language")
    if accept_language is None:
        accept_language = "en-US"

    result = requestDispatcher("get", get_search_url(keyword, accept_language))
    if result is None:
        return json.dumps([], ensure_ascii=True)
    def handler(result):
        return_result = []
        for movie in result.body.div.main.section.div.div.div.next_sibling.next_sibling.section.div.div.children:
            if (movie is None) or (len(str(movie).strip()) == 0) or (movie.div is None):
                continue

            attributes = []
            for detail in movie.div.children:
                if (detail is None) or (len(str(detail).strip()) == 0):
                    continue
                else:
                    attributes.append(detail)
            img_address = attributes[0].find("img")
            detail_address = attributes[1].find("a", {"class": "result"})
            translated_name = attributes[1].find("h2")

            original_name = attributes[1].find("span", {"class": "title"})
            release_date = attributes[1].find("span", {"class": "release_date"})
            introduction = attributes[1].find("p")
            if release_date is not None:
                release_date = release_date.text
            if introduction is not None:
                introduction = introduction.text
            if detail_address is not None:
                detail_address = detail_address["href"]
            if img_address is not None:
                img_address = img_address["src"]
            if original_name is not None:
                original_name = original_name.text
            if translated_name is not None:
                translated_name = translated_name.next
            # if img_address is not None:
            #     response = requests.get(img_address)
            #     print(response)

            # print(movie)
            match = re.search(r"/([^/]+)/([^/?]+)", detail_address)

            if match:
                type = match.group(1)  # 'person'
                id = match.group(2)  # '3264284-gongfu-wang'
                print("Category:", type)
                print("Identifier:", id)
            else:
                return "url resolving error"
            return_result.append(
                {"image_address": img_address, "translated_name": translated_name, "original_name": original_name,
                 "release_date": release_date, "introduction": introduction, "detail_address": detail_address,
                 "type": type, "resource_id":id})
        return json.dumps(return_result, ensure_ascii=False)

    return resolveMeta(result.text, handler)

@app.route("/movie/get_play_information", methods=["GET"])
@cross_origin()
def get_play_information():
    resourceId = request.args.get("resourceId")
    type = request.args.get("type")
    season_id = request.args.get("seasonId")
    if resourceId is None or type is None or season_id is None:
        print("get play information error", flush=True)
        return -1, "parameters are not sufficient"
    print(resourceId, flush=True)
    print(type, flush=True)
    rs = instance.execute(get_playlist, (resourceId, type, season_id))
    result = []
    for (resource_id, type, quality, bucket, path) in rs:
        result.append({"resource_id": resource_id, "type": type,"season_id":season_id ,"quality": quality,"bucket": bucket, "path": path})

    return json.dumps(result, ensure_ascii=False)


@app.route('/movie/get_suggestions')
@cross_origin()
def get_suggestions():
    print(request.args.get("keyword"))
    return "this is suggestion method"

if __name__ == "__main__":
    try:
        logging.info("Starting Kafka consumers")
        app = Flask(__name__)

        logging.info("Starting Flask application")
        app.run(host="0.0.0.0", port=5000)

    except Exception as e:
        logging.critical(f"Main process error: {e}")
        traceback.print_exc()
        print("Main process error", flush=True)
            # logging.critical(traceback.format_exc())