import json

from flask import Flask
from flask import request
from cassandra.cluster import Cluster
import requests
from bs4 import BeautifulSoup
from flask_cors import CORS, cross_origin

cluster = Cluster(['192.168.0.1', '192.168.0.2'])

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'


instance = cluster.connect()

def get_url_base():
    return "https://www.themoviedb.org"


def get_search_url(keyword, language):
    return get_url_base() + "/search?query=" + keyword + "&language=" + language


def get_detail_url(url):
    return get_url_base() + url


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
    return requests.request(method, request_url, headers=header)


@app.route('/movie/get_meta', methods=['GET'])
@cross_origin()
def get_meta():  # get name of a movie.
    # print(request.args.get("detail_address"))
    # print(get_detail_url(request.args.get("detail_address")))
    result = requestDispatcher("get", get_detail_url(request.args.get("detail_address")))

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

        movie_name = body.find("h2", {"class": "2"})
        print(movie_name)
        release_year = body.find("span",{"class":"tag release_date"})

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
                    {"actorDetailPage": actorDetail, "character": character, "avatar": avatar, "name": name})

        pictures = body.find_all("div", {
            "class": "backdrop glyphicons_v2 picture grey no_image_holder no_border no_border_radius"})
        picturesList = []
        for picture in pictures:
            if picture.img is not None:
                picturesList.append(picture.img["src"])

        makers = body.find_all("li", {"class": "profile"})
        makersList = []
        if makers is not None:
            for maker in makers:
                info = maker.find_all("p")
                if len(info) == 2:
                    makersList.append({"name": info[0].a.text, "role":info[1].text})

        genresList = []
        genre = body.find("span", {"class" : "genres"})
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
        return_result["makerList"] = makersList
        return_result["genre_list"] = genresList
        instance.execute("insert into movie.meta (movieId,poster, score, introduction, movie_name, tags, actress_list, release_year, level, picture_list, maker_list, genre_list)"
                         " values(?,?,?,?,?,?,?,?,?,?,?,?)",request.args.get("detail_address") , poster, score, introduction, movie_name, tag, actressList, release_year, level, picturesList, makersList, genresList)

        return json.dumps(return_result, ensure_ascii=False)

    return resolveMeta(result.text, handler)


@app.route("/movie/search", methods=['GET'])
@cross_origin()
def searchMovies():
    keyword = request.args.get("keyword")
    page_number = request.args.get("page_number")
    print(get_search_url(keyword, "zh-CN"))
    result = requestDispatcher("get", get_search_url(keyword, "zh-CN"))

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
            return_result.append(
                {"image_address": img_address, "translated_name": translated_name, "original_name": original_name,
                 "release_date": release_date, "introduction": introduction, "detail_address": detail_address})
        return json.dumps(return_result, ensure_ascii=False)

    return resolveMeta(result.text, handler)


@app.route('/movie/get_suggestions')
def get_suggestions():
    print(request.args.get("keyword"))
    return "this is suggestion method"


if __name__ == '__main__':
    app.run()
