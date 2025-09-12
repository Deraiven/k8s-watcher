#!/bin/bash
# Start script for namespace-watcher

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Please run ./setup.sh first."
    exit 1
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo ".env file not found. Creating from template..."
    cp .env.example .env
    echo "Please edit .env file with your credentials before running."
    exit 1
fi

# Activate virtual environment
echo "Starting Namespace Watcher..."
source venv/bin/activate

# Run the application
python run_local.py