import logging
import json
import concurrent.futures
import os
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Form, Query
from fastapi.responses import JSONResponse

from app.models.movie import (
    ResourceSourceRequest, ResourceSourceCreate, ResourceSourceRemove,
    DownloadStatus, FilesResponse, FileInfo
)
from app.db.cassandra import get_cassandra_instance, prepare_statements
from app.services.aria2_service import get_aria2_client, get_download, get_download_files, get_download_status
from app.services.minio_service import upload_file

logger = logging.getLogger(__name__)

router = APIRouter()

# Thread pools for background tasks
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
futures = []
handling_set = set()

# Directory for processed files
directory_path = "./processed"


@router.get("/hello")
async def hello():
    """Health check endpoint"""
    logger.info("Health check endpoint accessed")
    return "Hello, World!"


@router.post("/get_sources")
async def get_sources(request: ResourceSourceRequest):
    """Get sources for a resource"""
    logger.info("Get sources request received")

    try:
        resource_id = request.resourceId
        type_val = request.type
        season_id = request.seasonId
        episode = request.episode

        logger.debug(
            f"Request parameters: resource_id={resource_id}, type={type_val}, season_id={season_id}, episode={episode}")

        if not resource_id or not type_val or not season_id or not episode:
            logger.warning("Missing required parameters in get_sources request")
            raise HTTPException(status_code=400, detail="Missing required parameters")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        logger.info(f"Querying sources for resource_id={resource_id}, type={type_val}")
        result = instance.execute(statements["select_resource"], [resource_id, type_val, season_id, episode])

        result_list = []
        for row in result:
            # Handle different row structures
            resource_id, type_val, season_id, episode, resource, name, gid, status, quality = row

            logger.debug(f"Processing source: {resource_id}, {resource}, {name}, {gid}, {status}")

            if gid and len(gid):
                try:
                    download = get_download(gid)
                    if download:
                        result_list.append({
                            "resourceId": resource_id,
                            "type": type_val,
                            "source": resource,
                            "gid": gid,
                            "status": status,
                            "quality": quality
                        })
                    else:
                        result_list.append({
                            "resourceId": resource_id,
                            "type": type_val,
                            "source": resource,
                            "gid": "",
                            "status": status,
                            "quality": quality
                        })
                except Exception as e:
                    logger.warning(f"Failed to get download for gid {gid}: {str(e)}")
                    result_list.append({
                        "resourceId": resource_id,
                        "type": type_val,
                        "source": resource,
                        "gid": "",
                        "status": status,
                        "quality": quality
                    })
            else:
                result_list.append({
                    "resourceId": resource_id,
                    "type": type_val,
                    "source": resource,
                    "gid": gid,
                    "status": status,
                    "quality": quality
                })

        logger.info(f"Returning {len(result_list)} sources")
        return result_list

    except Exception as e:
        logger.error(f"Error in get_sources: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/add_source")
async def add_source(request: ResourceSourceCreate):
    """Add a source for a resource"""
    logger.info("Add source request received")

    try:
        resource_id = request.resourceId
        source = request.source
        name = request.name
        type_val = request.type

        logger.debug(f"Adding source: resource_id={resource_id}, type={type_val}, source={source}, name={name}")

        if not resource_id or not type_val or not source or not name:
            logger.warning("Missing required parameters in add_source request")
            raise HTTPException(status_code=400, detail="Missing required parameters")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        # Add source to database
        instance.execute(statements["insert_resource"], (resource_id.strip(), type_val, "", "", source.strip(), name.strip(), "", "init", ""))
        logger.info(f"Source added successfully for resource_id={resource_id}")
        
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Error in add_source: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/remove_source")
async def remove_source(request: ResourceSourceRemove):
    """Remove a source for a resource"""
    logger.info("Remove source request received")

    try:
        resource_id = request.resourceId
        type_val = request.type
        source = request.source

        logger.debug(f"Removing source: resourceId={resource_id}, type={type_val}, source={source}")

        if not resource_id or not source or not type_val:
            logger.warning("Missing required parameters in remove_source request")
            raise HTTPException(status_code=400, detail="Missing required parameters")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        # Remove source from database
        instance.execute(statements["delete_resource"], (resource_id, type_val, "", "", source))
        logger.info(f"Source removed successfully for resourceId={resource_id}")
        
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Error in remove_source: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/get_files")
async def get_files(
    gid: str = Form(...),
    movieId: str = Form(...),
    resource: str = Form(...)
):
    """Get files for a download"""
    logger.info("Get files request received")

    try:
        logger.debug(f"Getting files for gid={gid}, movieId={movieId}")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        try:
            status = get_download(gid)
            if status is None:
                logger.warning(f"No download found for gid {gid}")
                return JSONResponse(content="404", status_code=404)
            elif status.followed_by_ids:
                new_gid = status.followed_by_ids[0]
                logger.info(f"Download gid changed from {gid} to {new_gid}")
                instance.execute(statements["update_resource_gid"], (new_gid, movieId, "movie", "", "", resource, ""))
                gid = new_gid
            else:
                result = {
                    "status": "getting",
                    "total_size": status.total_length,
                    "complete_size": status.completed_length
                }
                logger.debug(f"Download in progress: {result}")
                return result
        except Exception as e:
            logger.warning(f"Invalid gid {gid}: {str(e)}")
            return {"files": "invalid gid"}

        # Get files
        files = get_download_files(gid)
        if not files:
            return {"files": [], "gid": gid}

        logger.info(f"Returning {len(files)} files for gid {gid}")
        return {"files": files, "gid": gid}

    except Exception as e:
        logger.error(f"Error in get_files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_download_status")
async def get_download_status_endpoint(gid: str = Query(...)):
    """Get download status"""
    logger.info("Get download status request received")

    try:
        logger.debug(f"Getting status for gid={gid}")

        status = get_download_status(gid)
        if status["status"] == "error":
            logger.warning(f"Error getting download status: {status['message']}")
            raise HTTPException(status_code=404, detail=status["message"])

        logger.info(f"Download status: {status['status']}")
        return status

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_download_status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/start_download")
async def start_download(
    resource_id: str = Form(...),
    type_val: str = Form(...),
    season_id: str = Form(...),
    episode: str = Form(...),
    resource: str = Form(...)
):
    """Start a download"""
    logger.info("Start download request received")

    try:
        logger.debug(f"Starting download for resource_id={resource_id}, type={type_val}, resource={resource}")

        # Get database instance and statements
        instance = get_cassandra_instance()
        statements = prepare_statements(instance)

        # Check if download already exists
        result = instance.execute(statements["select_resource_for_download"], [resource_id, type_val, season_id, episode, resource])
        rows = list(result)

        if not rows:
            logger.warning(f"No resource found for resource_id={resource_id}, type={type_val}, resource={resource}")
            raise HTTPException(status_code=404, detail="Resource not found")

        row = rows[0]
        resource_id, type_val, season_id, episode, resource, name, gid, status, quality = row

        # Add download to aria2
        aria_client = get_aria2_client()
        download = aria_client.add_uris([resource])
        new_gid = download.gid

        # Update database with new GID
        instance.execute(statements["update_resource_gid"], (new_gid, resource_id, type_val, season_id, episode, resource, quality))
        instance.execute(statements["update_resource_status"], ("downloading", resource_id, type_val, season_id, episode, resource, quality))

        logger.info(f"Download started with GID: {new_gid}")
        return {"status": "success", "gid": new_gid}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in start_download: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
