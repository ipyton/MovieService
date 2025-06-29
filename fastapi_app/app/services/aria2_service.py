import logging
import aria2p
from typing import Optional, Dict, Any, List

from app.core.config import settings

logger = logging.getLogger(__name__)

# Global aria2 client
aria2_client = None


def get_aria2_client():
    """Get or create Aria2 client"""
    global aria2_client
    
    if aria2_client is not None:
        return aria2_client
    
    try:
        aria2_client = aria2p.API(
            aria2p.Client(
                host=settings.ARIA2_HOST,
                port=settings.ARIA2_PORT,
                secret=settings.ARIA2_SECRET
            )
        )
        logger.info("Aria2 client initialized successfully")
        return aria2_client
    except Exception as e:
        logger.error(f"Failed to initialize Aria2 client: {str(e)}")
        raise


def get_download(gid: str) -> Optional[aria2p.Download]:
    """Get download by GID"""
    try:
        client = get_aria2_client()
        return client.get_download(gid)
    except Exception as e:
        logger.warning(f"Failed to get download for gid {gid}: {str(e)}")
        return None


def add_download(url: str, options: Dict[str, Any] = None) -> Optional[str]:
    """Add a new download"""
    try:
        client = get_aria2_client()
        download = client.add_uris([url], options=options)
        logger.info(f"Added download with GID: {download.gid}")
        return download.gid
    except Exception as e:
        logger.error(f"Failed to add download for URL {url}: {str(e)}")
        return None


def get_download_files(gid: str) -> List[Dict[str, Any]]:
    """Get files for a download"""
    try:
        download = get_download(gid)
        if not download:
            return []
        
        files = download.files
        return [{"file": file.index, "path": file.path.name, "size": file.length} for file in files]
    except Exception as e:
        logger.error(f"Failed to get files for download {gid}: {str(e)}")
        return []


def get_download_status(gid: str) -> Dict[str, Any]:
    """Get download status"""
    try:
        download = get_download(gid)
        if not download:
            return {"status": "error", "message": "Download not found"}
        
        return {
            "status": download.status,
            "total_size": download.total_length,
            "complete_size": download.completed_length,
            "download_speed": download.download_speed,
            "progress": download.progress,
            "eta": download.eta
        }
    except Exception as e:
        logger.error(f"Failed to get status for download {gid}: {str(e)}")
        return {"status": "error", "message": str(e)} 