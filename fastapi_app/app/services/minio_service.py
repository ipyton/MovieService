import logging
import os
from typing import Optional, BinaryIO, Dict, Any

from minio import Minio
from minio.error import S3Error

from app.core.config import settings

logger = logging.getLogger(__name__)

# Global MinIO client
minio_client = None


def get_minio_client():
    """Get or create MinIO client"""
    global minio_client
    
    if minio_client is not None:
        return minio_client
    
    try:
        minio_client = Minio(
            settings.MINIO_HOST,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        logger.info("MinIO client initialized successfully")
        
        # Ensure bucket exists
        if not minio_client.bucket_exists(settings.MINIO_BUCKET):
            minio_client.make_bucket(settings.MINIO_BUCKET)
            logger.info(f"Created bucket: {settings.MINIO_BUCKET}")
            
        return minio_client
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {str(e)}")
        raise


def upload_file(file_path: str, object_name: Optional[str] = None) -> Dict[str, Any]:
    """Upload a file to MinIO"""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return {"success": False, "error": "File not found"}
    
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    try:
        client = get_minio_client()
        file_size = os.path.getsize(file_path)
        
        # Upload the file
        client.fput_object(
            settings.MINIO_BUCKET,
            object_name,
            file_path
        )
        
        logger.info(f"Successfully uploaded {file_path} to {object_name}")
        return {
            "success": True,
            "bucket": settings.MINIO_BUCKET,
            "object_name": object_name,
            "size": file_size
        }
    except S3Error as e:
        logger.error(f"S3 error uploading file {file_path}: {str(e)}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Error uploading file {file_path}: {str(e)}")
        return {"success": False, "error": str(e)}


def download_file(object_name: str, file_path: str) -> Dict[str, Any]:
    """Download a file from MinIO"""
    try:
        client = get_minio_client()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Download the file
        client.fget_object(
            settings.MINIO_BUCKET,
            object_name,
            file_path
        )
        
        logger.info(f"Successfully downloaded {object_name} to {file_path}")
        return {
            "success": True,
            "bucket": settings.MINIO_BUCKET,
            "object_name": object_name,
            "file_path": file_path
        }
    except S3Error as e:
        logger.error(f"S3 error downloading file {object_name}: {str(e)}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Error downloading file {object_name}: {str(e)}")
        return {"success": False, "error": str(e)}


def get_presigned_url(object_name: str, expires: int = 3600) -> Dict[str, Any]:
    """Get a presigned URL for an object"""
    try:
        client = get_minio_client()
        
        # Generate presigned URL
        url = client.presigned_get_object(
            settings.MINIO_BUCKET,
            object_name,
            expires=expires
        )
        
        logger.info(f"Generated presigned URL for {object_name}")
        return {
            "success": True,
            "url": url,
            "expires_in": expires
        }
    except S3Error as e:
        logger.error(f"S3 error generating presigned URL for {object_name}: {str(e)}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Error generating presigned URL for {object_name}: {str(e)}")
        return {"success": False, "error": str(e)} 