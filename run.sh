#!/bin/bash

# Set the working directory to the script's location
cd "$(dirname "$0")"

# Activate virtual environment
source venv/bin/activate

# Run the Python script
python main.py
