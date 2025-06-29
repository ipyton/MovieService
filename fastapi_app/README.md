# Movie Service - FastAPI Migration

This is a migration of the Movie Service from Flask to FastAPI.

## Features

- RESTful API for movie metadata and download management
- Integration with external movie database services
- Download management with Aria2
- File storage with MinIO
- Messaging with Kafka
- Database storage with Cassandra

## Requirements

- Python 3.8+
- Cassandra database
- MinIO server
- Aria2 daemon
- Kafka broker

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd MovieService/fastapi_app
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

The application uses environment variables for configuration. You can set these in a `.env` file in the root directory:

```
CASSANDRA_HOSTS=127.0.0.1
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=movie
CASSANDRA_USERNAME=cassandra
CASSANDRA_PASSWORD=cassandra

MINIO_HOST=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=admin123
MINIO_SECURE=False
MINIO_BUCKET=longvideos

ARIA2_HOST=http://localhost
ARIA2_PORT=6800
ARIA2_SECRET=your_secret_token

KAFKA_BROKER=localhost:9092
KAFKA_GROUP_ID=fastapi-consumer-group

AUTH_SERVICE_URL=http://localhost:8080/auth/hasPermission
```

## Running the Application

### Development Mode

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Production Mode

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API Documentation

Once the application is running, you can access the API documentation at:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Project Structure

```
app/
├── api/
│   ├── endpoints/
│   │   ├── download.py
│   │   └── meta.py
│   └── api.py
├── core/
│   ├── config.py
│   └── logging.py
├── db/
│   └── cassandra.py
├── models/
│   └── movie.py
├── services/
│   ├── aria2_service.py
│   ├── kafka_service.py
│   └── minio_service.py
├── utils/
└── main.py
```

## Differences from Flask Version

- Uses FastAPI instead of Flask for improved performance and built-in validation
- Leverages Pydantic models for request/response validation
- Async endpoint handlers for improved concurrency
- Built-in OpenAPI documentation
- Dependency injection system for services and database connections
- Improved error handling with more detailed responses 