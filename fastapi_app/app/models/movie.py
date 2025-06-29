from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ResourceBase(BaseModel):
    resource_id: str
    type: str
    season_id: Optional[str] = None
    episode: Optional[str] = None


class ResourceCreate(ResourceBase):
    resource: str
    name: str
    quality: Optional[str] = None


class ResourceResponse(ResourceBase):
    resource: str
    name: str
    gid: Optional[str] = ""
    status: Optional[str] = "init"
    quality: Optional[str] = None


class ResourceSourceRequest(BaseModel):
    resourceId: str
    type: str
    seasonId: Optional[str] = ""
    episode: Optional[str] = ""


class ResourceSourceCreate(BaseModel):
    resourceId: str
    type: str
    source: str
    name: str


class ResourceSourceRemove(BaseModel):
    resourceId: str
    type: str
    source: str


class DownloadStatus(BaseModel):
    status: str
    total_size: int
    complete_size: int


class FileInfo(BaseModel):
    file: int
    path: str
    size: int


class FilesResponse(BaseModel):
    files: List[FileInfo]
    gid: str


class PlayInformation(BaseModel):
    resource_id: str
    type: str
    season_id: str
    episode: str
    resource: str


class MetaBase(BaseModel):
    resource_id: str
    type: str
    language: str


class MetaCreate(MetaBase):
    poster: Optional[str] = None
    score: Optional[str] = None
    introduction: Optional[str] = None
    movie_name: Optional[str] = None
    tags: Optional[str] = None
    actor_list: Optional[List[Dict[str, Any]]] = None
    release_year: Optional[str] = None
    level: Optional[str] = None
    picture_list: Optional[List[str]] = None
    maker_list: Optional[List[Dict[str, Any]]] = None
    genre_list: Optional[List[str]] = None
    total_season: Optional[int] = 1


class MetaResponse(MetaBase):
    poster: Optional[str] = None
    score: Optional[str] = None
    introduction: Optional[str] = None
    movie_name: Optional[str] = None
    tags: Optional[str] = None
    actor_list: Optional[List[Dict[str, Any]]] = None
    release_year: Optional[str] = None
    level: Optional[str] = None
    picture_list: Optional[List[str]] = None
    maker_list: Optional[List[Dict[str, Any]]] = None
    genre_list: Optional[List[str]] = None
    total_season: Optional[int] = 1
    stared: Optional[bool] = False


class SearchResult(BaseModel):
    id: str
    name: str
    year: Optional[str] = None
    poster: Optional[str] = None
    type: str


class SearchResponse(BaseModel):
    results: List[SearchResult] 