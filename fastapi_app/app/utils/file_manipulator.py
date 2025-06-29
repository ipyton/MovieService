import os
import logging
import shutil
import subprocess
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)


def ensure_directory_exists(directory_path: str) -> bool:
    """Ensure that a directory exists, creating it if necessary"""
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Created directory: {directory_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create directory {directory_path}: {str(e)}")
        return False


def delete_file(file_path: str) -> bool:
    """Delete a file if it exists"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted file: {file_path}")
            return True
        else:
            logger.warning(f"File not found for deletion: {file_path}")
            return False
    except Exception as e:
        logger.error(f"Failed to delete file {file_path}: {str(e)}")
        return False


def move_file(source_path: str, destination_path: str) -> bool:
    """Move a file from source to destination"""
    try:
        # Ensure destination directory exists
        destination_dir = os.path.dirname(destination_path)
        ensure_directory_exists(destination_dir)
        
        # Move the file
        shutil.move(source_path, destination_path)
        logger.info(f"Moved file from {source_path} to {destination_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to move file from {source_path} to {destination_path}: {str(e)}")
        return False


def copy_file(source_path: str, destination_path: str) -> bool:
    """Copy a file from source to destination"""
    try:
        # Ensure destination directory exists
        destination_dir = os.path.dirname(destination_path)
        ensure_directory_exists(destination_dir)
        
        # Copy the file
        shutil.copy2(source_path, destination_path)
        logger.info(f"Copied file from {source_path} to {destination_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to copy file from {source_path} to {destination_path}: {str(e)}")
        return False


def get_file_info(file_path: str) -> Dict[str, Any]:
    """Get information about a file"""
    try:
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return {"exists": False}
        
        stat_info = os.stat(file_path)
        return {
            "exists": True,
            "size": stat_info.st_size,
            "created": stat_info.st_ctime,
            "modified": stat_info.st_mtime,
            "accessed": stat_info.st_atime,
            "is_directory": os.path.isdir(file_path),
            "is_file": os.path.isfile(file_path)
        }
    except Exception as e:
        logger.error(f"Failed to get file info for {file_path}: {str(e)}")
        return {"exists": False, "error": str(e)}


def list_files(directory_path: str, pattern: Optional[str] = None) -> List[str]:
    """List files in a directory, optionally filtered by a pattern"""
    try:
        if not os.path.exists(directory_path):
            logger.warning(f"Directory not found: {directory_path}")
            return []
        
        if pattern:
            import fnmatch
            files = [f for f in os.listdir(directory_path) if fnmatch.fnmatch(f, pattern)]
        else:
            files = os.listdir(directory_path)
        
        return [os.path.join(directory_path, f) for f in files]
    except Exception as e:
        logger.error(f"Failed to list files in {directory_path}: {str(e)}")
        return []


def run_ffmpeg_command(command: List[str]) -> Tuple[bool, str]:
    """Run an FFmpeg command and return success status and output"""
    try:
        logger.info(f"Running FFmpeg command: {' '.join(command)}")
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg command failed with code {result.returncode}: {result.stderr}")
            return False, result.stderr
        
        logger.info("FFmpeg command completed successfully")
        return True, result.stdout
    except Exception as e:
        logger.error(f"Failed to run FFmpeg command: {str(e)}")
        return False, str(e) 