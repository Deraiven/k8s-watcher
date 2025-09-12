#!/bin/bash
# Setup script for namespace-watcher

echo "=== Namespace Watcher Setup ==="

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Copy environment file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cp .env.example .env
    echo "Please edit .env file with your credentials before running the application"
else
    echo ".env file already exists"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To run the application:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Edit .env file with your credentials: vim .env"
echo "3. Run the application: python run_local.py"
echo ""
echo "To deactivate the virtual environment: deactivate"