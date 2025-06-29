import os
from typing import List, Optional, Dict, Any

from pydantic_settings import BaseSettings
from pydantic import validator

class Settings(BaseSettings):
    PROJECT_NAME: str = "Movie Service"
    PROJECT_DESCRIPTION: str = "Movie Service API with FastAPI"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"
    
    # CORS
    CORS_ORIGINS: List[str] = ["*"]
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s %(levelname)s %(name)s %(message)s"
    
    # Database
    CASSANDRA_HOSTS: List[str] = ["127.0.0.1"]
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "movie"
    CASSANDRA_USERNAME: str = "cassandra"
    CASSANDRA_PASSWORD: str = "cassandra"
    
    # MinIO
    MINIO_HOST: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "admin"
    MINIO_SECRET_KEY: str = "admin123"
    MINIO_SECURE: bool = False
    MINIO_BUCKET: str = "longvideos"
    
    # Aria2
    ARIA2_HOST: str = "http://localhost"
    ARIA2_PORT: int = 6800
    ARIA2_SECRET: str = "your_secret_token"
    
    # Kafka
    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_GROUP_ID: str = "fastapi-consumer-group"
    
    # Auth Service
    AUTH_SERVICE_URL: str = "http://localhost:8080/auth/hasPermission"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings() 