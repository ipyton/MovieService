#!/bin/bash

# Change to the directory containing this script
cd "$(dirname "$0")"

# Set PYTHONPATH to include the parent directory
export PYTHONPATH=$PYTHONPATH:$(dirname $(pwd))

# Run the FastAPI application with uvicorn
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 