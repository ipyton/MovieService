import logging
from typing import Optional

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider

from app.core.config import settings

logger = logging.getLogger(__name__)

# Global database session
instance = None


def get_cassandra_instance():
    """Get or create Cassandra instance"""
    global instance
    
    if instance is not None:
        return instance
    
    logger.info("Initializing Cassandra connection...")
    try:
        auth_provider = PlainTextAuthProvider(
            username=settings.CASSANDRA_USERNAME,
            password=settings.CASSANDRA_PASSWORD
        )
        
        cluster = Cluster(
            settings.CASSANDRA_HOSTS,
            port=settings.CASSANDRA_PORT,
            auth_provider=auth_provider
        )
        
        # Connect to keyspace
        instance = cluster.connect(settings.CASSANDRA_KEYSPACE)
        instance.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
        
        logger.info("Successfully connected to Cassandra cluster")
        return instance
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra cluster: {str(e)}")
        raise


# Prepare statements
def prepare_statements(instance):
    """Prepare all statements for the application"""
    logger.info("Preparing Cassandra statements...")
    
    statements = {}
    
    try:
        # Resource statements
        statements["insert_resource"] = instance.prepare(
            "INSERT INTO movie.resource (resource_id, type, season_id, episode, resource, name, gid, status, quality) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        
        statements["select_resource"] = instance.prepare(
            "SELECT * FROM movie.resource WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ?"
        )
        
        statements["select_resource_for_download"] = instance.prepare(
            "SELECT * FROM movie.resource WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ? AND resource = ?"
        )
        
        statements["delete_resource"] = instance.prepare(
            "DELETE FROM movie.resource WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ? AND resource = ?"
        )
        
        statements["update_resource_status"] = instance.prepare(
            "UPDATE movie.resource SET status = ? WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ? AND resource = ? AND quality = ?"
        )
        
        statements["update_resource_gid"] = instance.prepare(
            "UPDATE movie.resource SET gid = ? WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ? AND resource = ? AND quality = ?"
        )
        
        # Playable statements
        statements["select_file_path"] = instance.prepare(
            "SELECT * FROM movie.playable WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ? AND quality = ?"
        )
        
        statements["delete_playable"] = instance.prepare(
            "DELETE FROM movie.playable WHERE resource_id = ? AND type = ? AND season_id = ? AND episode = ? AND quality = ? AND episode = ?"
        )
        
        # Meta statements
        statements["insert_meta"] = instance.prepare(
            "INSERT INTO movie.meta (resource_id, poster, score, introduction, movie_name, tags, actor_list, release_year, level, picture_list, maker_list, genre_list, type, language, total_season) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        
        statements["get_video_meta"] = instance.prepare(
            "SELECT * FROM movie.meta WHERE resource_id = ? AND type = ? AND language = ?"
        )
        
        statements["get_stared"] = instance.prepare(
            "SELECT * FROM movie.movieGallery WHERE user_id = ? AND resource_id = ? AND type = ?"
        )
        
        statements["get_playlist"] = instance.prepare(
            "SELECT * FROM movie.playable WHERE resource_id = ? AND type = ? AND season_id = ?"
        )
        
        statements["insert_first_season_meta"] = instance.prepare(
            "INSERT INTO movie.season_meta (resource_id, type, season_id, total_episode) VALUES (?, ?, ?, ?)"
        )
        
        logger.info("Successfully prepared all Cassandra statements")
        return statements
    except Exception as e:
        logger.error(f"Failed to prepare Cassandra statements: {str(e)}")
        raise 