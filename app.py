import json

from flask import Flask
from flask import request
from cassandra.cluster import Cluster
import requests
from bs4 import BeautifulSoup

cluster = Cluster(['192.168.0.1', '192.168.0.2'])

app = Flask(__name__)
# cluster.connect()

def get_url_base():
    return "https://www.themoviedb.org"

def get_search_url(keyword, language):
    return get_url_base() +"/search?query="+ keyword +"&language=" + language

def save_to_database(metadata):
    pass


def resolveMeta(file, job):
    soup = BeautifulSoup(file, "html.parser")
    return job(soup)

def requestDispatcher(method,request_url,header=None):
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


@app.route('/movie/get_meta')
def get_meta():  # get name of a movie.
    print(request.args.get("movie_id"))
    return 'Hello World!'


@app.route("/movie/search")
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
            detail_address = attributes[1].find("a", {"class":"result"})
            translated_name = attributes[1].div.div.a.h2.next_element
            original_name = attributes[1].find("span", {"class":"title"})
            release_date = attributes[1].find("span",{ "class":"release_date"})
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
            # if img_address is not None:
            #     response = requests.get(img_address)
            #     print(response)

            # print(movie)
            return_result.append({"image_address": img_address, "translated_name": translated_name, "original_name":original_name, "release_date" : release_date,"introduction":introduction, "detail_address":detail_address})
        return json.dumps(return_result, ensure_ascii=False)
    return resolveMeta(result.text, handler)



@app.route('/movie/get_suggestions')
def get_suggestions():
    print(request.args.get("keyword"))
    return "this is suggestion method"


if __name__ == '__main__':
    app.run()
