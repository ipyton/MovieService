import logging
import time
import re
import json
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
import requests
from bs4 import BeautifulSoup

from app.models.movie import MetaBase, MetaResponse, SearchResponse, SearchResult
from app.db.cassandra import get_cassandra_instance, prepare_statements
from app.services.kafka_service import send_message

logger = logging.getLogger(__name__)

router = APIRouter()


def get_url_base():
    return "https://www.themoviedb.org"


def get_search_url(keyword, language):
    url = get_url_base() + "/search?query=" + keyword + "&language=" + language
    logger.debug(f"Generated search URL: {url}")
    return url


def get_detail_url(type_val, id_val, language):
    url = get_url_base() + "/" + type_val + "/" + id_val + "?language=" + language
    logger.debug(f"Generated detail URL: {url}")
    return url


def request_dispatcher(method, request_url, header=None):
    """Make HTTP requests with comprehensive logging"""
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


def resolve_meta(file, job):
    """Parse HTML with BeautifulSoup and execute job"""
    logger.debug("Starting HTML parsing with BeautifulSoup")
    try:
        soup = BeautifulSoup(file, "html.parser")
        result = job(soup)
        logger.debug("Successfully parsed HTML and executed job")
        return result
    except Exception as e:
        logger.error(f"Error parsing HTML: {str(e)}")
        raise


@router.get("/get_meta")
async def get_meta(
    resource_id: str = Query(...),
    type_val: str = Query(...),
    language: str = Query("en-US"),
    user_id: Optional[str] = Query(None)
):
    """Get metadata for a movie or TV show"""
    logger.info(f"Getting metadata for type: {type_val}, id: {resource_id}, language: {language}")

    try:
        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        # Check database first
        logger.debug("Checking database for existing metadata...")
        result = instance.execute(statements["get_video_meta"], [resource_id, type_val, language])
        result_list = list(result)
        logger.debug(f"Database query returned {len(result_list)} results")

        if len(result_list) != 0:
            logger.info("Found existing metadata in database")
            row = result_list[0]
            
            # Parse the row into a response
            parsed = {
                "resource_id": row.resource_id,
                "type": row.type,
                "language": row.language,
                "poster": row.poster,
                "score": row.score,
                "introduction": row.introduction,
                "movie_name": row.movie_name,
                "tags": row.tags,
                "actor_list": row.actor_list if hasattr(row, 'actor_list') else [],
                "release_year": row.release_year,
                "level": row.level,
                "picture_list": row.picture_list if hasattr(row, 'picture_list') else [],
                "maker_list": row.maker_list if hasattr(row, 'maker_list') else [],
                "genre_list": row.genre_list if hasattr(row, 'genre_list') else [],
                "total_season": row.total_season if hasattr(row, 'total_season') else 1,
                "stared": False
            }

            # Check if starred
            if user_id:
                logger.debug(f"Checking starred status for user: {user_id}")
                star_result = list(instance.execute(statements["get_stared"], [user_id, resource_id, type_val]))
                if len(star_result) != 0:
                    parsed["stared"] = True
                    logger.debug("Movie is starred by user")

            logger.info("Successfully retrieved metadata from database")
            return parsed

        # Fetch from external source
        logger.info("No metadata found in database, fetching from external source")
        result = request_dispatcher("get", get_detail_url(type_val, resource_id, language))

        if result is None:
            logger.error("Failed to fetch metadata from external source")
            return []

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
                    return_result["poster"] = poster
                else:
                    return_result["poster"] = ""

                # Parse score
                score = body.find("div", {"class": "user_score_chart"})
                if score is not None:
                    score = score["data-percent"]
                    logger.debug(f"Found score: {score}")
                    return_result["score"] = score
                else:
                    return_result["score"] = ""

                # Parse introduction
                introduction = body.find("div", {"class": "overview"})
                if introduction is not None:
                    introduction = introduction.p.text
                    logger.debug("Found introduction")
                    return_result["introduction"] = introduction
                else:
                    return_result["introduction"] = ""

                # Parse tag
                tag = body.find("h3", {"class": "tagline"})
                if tag is not None:
                    tag = tag.text
                    logger.debug(f"Found tag: {tag}")
                    return_result["tags"] = tag
                else:
                    return_result["tags"] = ""

                # Parse level
                level = body.find("span", {"class": "certification"})
                if level is not None:
                    level = level.text
                    logger.debug(f"Found level: {level}")
                    return_result["level"] = level
                else:
                    return_result["level"] = ""

                # Parse movie name
                movie_name = None
                if body.find("section", {"class": "images inner"}) is not None:
                    movie_name = body.find("section", {"class": "images inner"}).section

                # Parse release year
                release_year = body.find("span", {"class": "tag release_date"})
                if release_year is not None:
                    release_year = release_year.text
                    logger.debug(f"Found release year: {release_year}")
                    return_result["release_year"] = release_year
                else:
                    return_result["release_year"] = ""

                if movie_name is not None and movie_name.a is not None:
                    movie_name = movie_name.a.text
                    logger.debug(f"Found movie name: {movie_name}")
                    return_result["movie_name"] = movie_name
                else:
                    return_result["movie_name"] = ""

                # Parse actors
                actress = body.find("ol", {"class": "people scroller"})
                actress_list = []
                if actress is not None:
                    for li in actress.find_all("li", {"class": "card"}):
                        p = li.p
                        if p is not None:
                            actress_list.append({
                                "name": p.a.text if p.a is not None else "",
                                "role": p.find("p", {"class": "character"}).text if p.find("p", {"class": "character"}) is not None else ""
                            })
                    logger.debug(f"Found {len(actress_list)} actors")
                    return_result["actor_list"] = actress_list
                else:
                    return_result["actor_list"] = []

                # Parse pictures
                pictures = body.find("div", {"class": "image_content"})
                picture_list = []
                if pictures is not None:
                    for img in pictures.find_all("img"):
                        if img.has_attr("data-src"):
                            picture_list.append(img["data-src"])
                    logger.debug(f"Found {len(picture_list)} pictures")
                    return_result["picture_list"] = picture_list
                else:
                    return_result["picture_list"] = []

                # Parse makers
                makers = body.find("ol", {"class": "people no_image"})
                maker_list = []
                if makers is not None:
                    for li in makers.find_all("li"):
                        p = li.p
                        if p is not None:
                            maker_list.append({
                                "name": p.a.text if p.a is not None else "",
                                "role": p.find("p", {"class": "character"}).text if p.find("p", {"class": "character"}) is not None else ""
                            })
                    logger.debug(f"Found {len(maker_list)} makers")
                    return_result["maker_list"] = maker_list
                else:
                    return_result["maker_list"] = []

                # Parse genres
                genres = body.find("span", {"class": "genres"})
                genre_list = []
                if genres is not None:
                    for a in genres.find_all("a"):
                        genre_list.append(a.text)
                    logger.debug(f"Found {len(genre_list)} genres")
                    return_result["genre_list"] = genre_list
                else:
                    return_result["genre_list"] = []

                # Set additional fields
                return_result["resource_id"] = resource_id
                return_result["type"] = type_val
                return_result["language"] = language
                return_result["total_season"] = 1  # Default value
                return_result["stared"] = False

                logger.info("Successfully parsed metadata from HTML")
                return return_result
            except Exception as e:
                logger.error(f"Error parsing metadata: {str(e)}", exc_info=True)
                return {}

        # Parse the HTML and get the metadata
        meta_data = resolve_meta(result.text, handler)
        
        if not meta_data:
            logger.warning("Failed to parse metadata from HTML")
            return []

        # Insert metadata into database
        try:
            instance.execute(statements["insert_meta"], (
                meta_data.get("resource_id", ""),
                meta_data.get("poster", ""),
                meta_data.get("score", ""),
                meta_data.get("introduction", ""),
                meta_data.get("movie_name", ""),
                meta_data.get("tags", ""),
                meta_data.get("actor_list", []),
                meta_data.get("release_year", ""),
                meta_data.get("level", ""),
                meta_data.get("picture_list", []),
                meta_data.get("maker_list", []),
                meta_data.get("genre_list", []),
                meta_data.get("type", ""),
                meta_data.get("language", ""),
                meta_data.get("total_season", 1)
            ))
            logger.info("Metadata inserted into database")
        except Exception as e:
            logger.error(f"Failed to insert metadata into database: {str(e)}", exc_info=True)

        return meta_data

    except Exception as e:
        logger.error(f"Error in get_meta: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/search")
async def search_movies(
    keyword: str = Query(...),
    language: str = Query("en-US")
):
    """Search for movies or TV shows"""
    logger.info(f"Searching for movies with keyword: {keyword}, language: {language}")

    try:
        # Make request to external source
        result = request_dispatcher("get", get_search_url(keyword, language))

        if result is None:
            logger.error("Failed to fetch search results from external source")
            return {"results": []}

        def handler(soup_result):
            logger.debug("Starting search results parsing from HTML")
            return_results = []

            try:
                search_results = soup_result.find("div", {"class": "search_results"})
                if search_results is None:
                    logger.warning("No search results found")
                    return return_results

                results = search_results.find_all("div", {"class": "card"})
                logger.debug(f"Found {len(results)} search results")

                for result in results:
                    try:
                        # Parse result details
                        result_data = {}
                        
                        # Parse ID and type
                        if result.a is not None and result.a.has_attr("href"):
                            href = result.a["href"]
                            match = re.search(r"/([^/]+)/([^/]+)", href)
                            if match:
                                result_data["type"] = match.group(1)
                                result_data["id"] = match.group(2)
                            else:
                                continue
                        else:
                            continue

                        # Parse name
                        name = result.find("h2")
                        if name is not None:
                            result_data["name"] = name.text.strip()
                        else:
                            result_data["name"] = ""

                        # Parse year
                        year = result.find("span", {"class": "release_date"})
                        if year is not None:
                            result_data["year"] = year.text.strip()
                        else:
                            result_data["year"] = ""

                        # Parse poster
                        poster = result.find("img", {"class": "poster"})
                        if poster is not None and poster.has_attr("src"):
                            result_data["poster"] = poster["src"]
                        else:
                            result_data["poster"] = ""

                        return_results.append(result_data)
                    except Exception as e:
                        logger.error(f"Error parsing search result: {str(e)}")
                        continue

                logger.info(f"Successfully parsed {len(return_results)} search results")
                return return_results
            except Exception as e:
                logger.error(f"Error parsing search results: {str(e)}", exc_info=True)
                return []

        # Parse the HTML and get the search results
        search_results = resolve_meta(result.text, handler)
        
        return {"results": search_results}

    except Exception as e:
        logger.error(f"Error in search_movies: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_play_information")
async def get_play_information(
    resource_id: str = Query(...),
    type_val: str = Query(...),
    season_id: str = Query(...),
    episode: str = Query(...),
    resource: str = Query(...)
):
    """Get play information for a resource"""
    logger.info("Get play information request received")

    try:
        logger.debug(f"Getting play information for resource_id={resource_id}, type={type_val}, season_id={season_id}, episode={episode}, resource={resource}")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        # Query database for play information
        result = instance.execute(statements["select_resource_for_download"], [resource_id, type_val, season_id, episode, resource])
        rows = list(result)

        if not rows:
            logger.warning(f"No play information found for resource_id={resource_id}")
            raise HTTPException(status_code=404, detail="Play information not found")

        row = rows[0]
        resource_id, type_val, season_id, episode, resource, name, gid, status, quality = row

        # Return play information
        return {
            "resourceId": resource_id,
            "type": type_val,
            "seasonId": season_id,
            "episode": episode,
            "resource": resource,
            "name": name,
            "gid": gid,
            "status": status,
            "quality": quality
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_play_information: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_playlist")
async def get_playlist(
    resource_id: str = Query(...),
    type_val: str = Query(...),
    season_id: str = Query(...)
):
    """Get playlist for a resource"""
    logger.info("Get playlist request received")

    try:
        logger.debug(f"Getting playlist for resource_id={resource_id}, type={type_val}, season_id={season_id}")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        # Query database for playlist
        result = instance.execute(statements["get_playlist"], [resource_id, type_val, season_id])
        rows = list(result)

        if not rows:
            logger.warning(f"No playlist found for resource_id={resource_id}")
            return []

        playlist = []
        for row in rows:
            playlist.append({
                "resourceId": row.resource_id,
                "type": row.type,
                "seasonId": row.season_id,
                "episode": row.episode,
                "quality": row.quality,
                "url": row.url,
                "size": row.size
            })

        logger.info(f"Returning playlist with {len(playlist)} items")
        return playlist

    except Exception as e:
        logger.error(f"Error in get_playlist: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
