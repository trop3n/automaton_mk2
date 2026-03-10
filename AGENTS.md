# Agent Development Guide

This document provides essential information for agentic coding agents (such as Claude, GPT-4, etc.) operating in this repository.

## Project Overview

This is a Python-based Vimeo video classification and management system. It automatically classifies, renames, and organizes Vimeo videos based on scheduled events, time windows, and video metadata.

### Main Scripts
- `automaton.py` - Original video classification script
- `automaton_v2.py` - Improved version with better time extraction logic
- `automaton_scheduler.py` - Vimeo Live Event scheduler and tracker
- `query_video.py` - Utility to query Vimeo API for video metadata

## Build/Lint/Test Commands

### Running the Application

```bash
# Activate virtual environment
source .venv/bin/activate

# Run main classification script (v1)
python3 automaton.py

# Run main classification script (v2 - recommended)
python3 automaton_v2.py

# Run scheduler (see examples below)
python3 automaton_scheduler.py --help
python3 automaton_scheduler.py list-types
python3 automaton_scheduler.py list
python3 automaton_scheduler.py create --type "Test Service A" --date 2024-12-07 --time 09:30 --dry-run
python3 automaton_scheduler.py match-videos --hours 72

# Query specific video metadata
python3 query_video.py VIDEO_ID
python3 query_video.py 1137434285 1137326065
```

### Running Tests

**Note**: This project currently has no automated tests. When adding new functionality, consider creating a `tests/` directory with pytest tests.

```bash
# When tests are added, run them with:
pytest tests/
pytest tests/test_specific_file.py
pytest tests/test_specific_file.py::test_function_name
```

### Linting and Formatting

**Note**: This project currently has no linting or formatting tools configured. Consider adding:

```bash
# Recommended tools to add (not currently installed):
pip install black flake8 mypy ruff

# Format code
black .

# Check for linting issues
flake8 .
ruff check .

# Type checking
mypy .
```

### Dependency Management

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies (if requirements.txt exists)
pip install -r requirements.txt

# Or install individually
pip install vimeo pytz python-dotenv aiohttp
```

## Code Style Guidelines

### Imports

Group imports in the following order, separated by blank lines:
1. Standard library imports (alphabetically)
2. Third-party imports (alphabetically)
3. Local application imports (if any)

```python
# Standard library
import os
import re
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Third-party
import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient
```

### Formatting

- **Indentation**: 4 spaces (no tabs)
- **Line Length**: Aim for 80-100 characters maximum
- **Quotes**: Use double quotes (`"`) for strings consistently
- **Blank Lines**: 
  - Two blank lines before top-level function/class definitions
  - One blank line between logical sections within functions
  - One blank line after import block

### Naming Conventions

- **Constants**: UPPER_CASE with underscores
  ```python
  VIMEO_ACCESS_TOKEN = "..."
  TIMEZONE = "America/Chicago"
  LOOKBACK_HOURS = 48
  ```

- **Functions**: snake_case with descriptive names
  ```python
  def get_vimeo_client(token, key, secret):
  def is_time_in_window(dt, start_tuple, end_tuple):
  def extract_time_from_title(title):
  ```

- **Variables**: snake_case, descriptive names preferred
  ```python
  scheduled_dt = ...
  video_title_lower = ...
  all_recent_videos = []
  ```

- **Classes**: PascalCase (though this project uses primarily functions)
  ```python
  class VideoClassifier:
  ```

### Documentation

- **Module Docstrings**: Use triple-quoted strings at the top of files
  ```python
  """
  Vimeo Live Event Scheduler and Tracker
  
  This script manages Vimeo Live Events with embedded classification metadata.
  
  Usage:
      python3 automaton_scheduler.py create --type "Test Service A" --date 2024-12-07
  """
  ```

- **Function Docstrings**: Use triple-quoted strings immediately after function definition
  ```python
  def get_best_timestamp(video_data, local_tz):
      """
      Get the most reliable timestamp from video data.
      
      Args:
          video_data: Video metadata dict
          local_tz: Local timezone object
      
      Returns:
          tuple: (datetime in local timezone, source field name)
      """
  ```

- **Inline Comments**: Use sparingly, only when code isn't self-explanatory
  ```python
  # Monday is 0, Sunday is 6
  day_of_week = reference_time.weekday()
  ```

### Type Hints

While not currently used in this codebase, type hints are encouraged for new code:

```python
from typing import Dict, List, Tuple, Optional

def classify_video(video_data: Dict) -> Dict[str, any]:
    """Classify a video and return classification info."""
    
def get_best_timestamp(video_data: Dict, local_tz) -> Tuple[datetime, str]:
    """Get the most reliable timestamp from video data."""
```

### Error Handling

- Use try-except blocks for API calls and file operations
- Provide clear, actionable error messages
- Use `sys.exit(1)` for fatal errors
- Always check HTTP status codes for API responses

```python
try:
    response = client.get("/me/videos", params={...})
    response.raise_for_status()
    videos = response.json().get("data", [])
except Exception as e:
    print(f"An error occurred while fetching videos: {e}")
    sys.exit(1)
```

### Configuration Management

- Store configuration constants at the top of the file
- Load secrets from environment variables using `.env` file
- Use descriptive constant names

```python
# --- Configuration ---
VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

TIMEZONE = "America/Chicago"
LOOKBACK_HOURS = 48
```

### Code Structure

Follow this general structure for Python scripts:

1. **Shebang and module docstring** (if executable)
2. **Imports**
3. **Load environment variables** (`load_dotenv()`)
4. **Configuration constants**
5. **Helper functions** (logically ordered)
6. **Main function**
7. **Entry point guard** (`if __name__ == "__main__":`)

### API Client Usage

When using the Vimeo API:

```python
# Initialize client
client = get_vimeo_client(token, key, secret)

# Test connection
user_response = client.get("/me")
if user_response.status_code != 200:
    print(f"Failed to connect: {user_response.status_code}")
    return

# Make API calls with proper error handling
try:
    response = client.get("/me/videos", params={...})
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"Error: {e}")
```

### Working with Dates and Timezones

This project extensively uses datetime operations with timezone awareness:

```python
import pytz
from datetime import datetime, timedelta

# Always use timezone-aware datetimes
local_tz = pytz.timezone("America/Chicago")
now_utc = datetime.now(pytz.utc)
now_local = now_utc.astimezone(local_tz)

# Parse ISO format timestamps
dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
```

## Environment Variables

Required environment variables (store in `.env` file):

```
VIMEO_ACCESS_TOKEN=your_token_here
VIMEO_CLIENT_ID=your_client_id_here
VIMEO_CLIENT_SECRET=your_client_secret_here
```

## Project-Specific Notes

### Video Classification Logic

The classification system uses:
1. **Time windows** - Videos are classified based on upload/modified time
2. **Title keywords** - Keywords like "worship", "contemporary", "root", etc.
3. **Day of week** - Different service types on different days
4. **Scheduled events** - Metadata embedded in event descriptions

### Folder Structure

- Videos are moved to specific folders based on classification
- Some folders are excluded from processing (`EXCLUDED_FOLDER_IDS`)
- Only videos in the Team Library root are processed

### Best Practices for This Project

1. **Test API calls with dry-run mode** before making actual changes
2. **Use DEBUG_MODE** to inspect video metadata without processing
3. **Check video playability** before processing (skip phantom live events)
4. **Validate timestamps** - handle timezone conversions carefully
5. **Preserve existing titles** when already correct
6. **Log operations clearly** with descriptive print statements

## Additional Resources

- Vimeo API Documentation: https://developer.vimeo.com/api/reference
- Python Vimeo Client: https://github.com/vimeo/vimeo.py
- Python-dotenv: https://github.com/theskumar/python-dotenv
