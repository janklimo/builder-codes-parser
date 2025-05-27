# JSON Stream Parser

A simple Python project that demonstrates the usage of `json-stream` package to parse JSON files.

## Setup

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

The project includes a sample `data.json` file and a `main.py` script that demonstrates how to use `json-stream` to parse JSON data.

To run the example:
```bash
python main.py
```

## Features

- Uses `json-stream` for efficient JSON parsing
- Demonstrates both random access and streaming access to JSON data
- Includes error handling for common scenarios
- Uses persistent mode to allow random access to the data

## Project Structure

- `main.py`: Main script for parsing JSON
- `data.json`: Sample JSON data file
- `requirements.txt`: Project dependencies 
