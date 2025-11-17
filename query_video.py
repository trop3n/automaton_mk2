#!/usr/bin/env python3
"""
Simple script to query Vimeo API and get full metadata for specific videos.
Usage: python3 query_video.py VIDEO_ID [VIDEO_ID ...]
Example: python3 query_video.py 1137434285 1137326065
"""

import os
import sys
import json
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables
load_dotenv()

VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

def query_video(client, video_id):
    """Query full metadata for a video ID."""
    try:
        # Construct the URI
        video_uri = f"/videos/{video_id}" if not video_id.startswith('/') else video_id

        print(f"\n{'='*80}")
        print(f"Querying video: {video_id}")
        print('='*80)

        # Fetch the video with all available fields
        response = client.get(video_uri)

        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))
        else:
            print(f"ERROR: Status {response.status_code}")
            print(response.text)

    except Exception as e:
        print(f"ERROR querying video {video_id}: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 query_video.py VIDEO_ID [VIDEO_ID ...]")
        print("\nExamples:")
        print("  python3 query_video.py 1137434285")
        print("  python3 query_video.py 1137434285 1137326065 1137436717")
        sys.exit(1)

    # Initialize Vimeo client
    client = VimeoClient(
        token=VIMEO_ACCESS_TOKEN,
        key=VIMEO_CLIENT_ID,
        secret=VIMEO_CLIENT_SECRET
    )

    # Test connection
    try:
        user_response = client.get("/me")
        if user_response.status_code == 200:
            print(f"Connected to Vimeo as: {user_response.json().get('name')}")
        else:
            print("ERROR: Failed to connect to Vimeo API")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Could not connect to Vimeo: {e}")
        sys.exit(1)

    # Query each video ID provided
    video_ids = sys.argv[1:]
    for video_id in video_ids:
        query_video(client, video_id)

if __name__ == "__main__":
    main()
