from vimeo import VimeoClient
from dotenv import load_dotenv
import os
import json
load_dotenv()
client = VimeoClient(
    token=os.environ.get("VIMEO_ACCESS_TOKEN"),
    key=os.environ.get("VIMEO_CLIENT_ID"),
    secret=os.environ.get("VIMEO_CLIENT_SECRET")
)
# Try querying all videos filtered by type=live
print("=== ALL LIVE VIDEOS (including events) ===\n")
response = client.get("/me/videos", params={
    "per_page": 50,
    "filter": "live",
    "fields": "uri,name,type,created_time,modified_time,description"
})
if response.status_code == 200:
    data = response.json()
    print(f"Total: {len(data.get('data', []))} items\n")
    for video in data.get("data", []):
        print(f"URI: {video.get('uri')}")
        print(f"Name: {video.get('name')}")
        print(f"Type: {video.get('type')}")
        print(f"Created: {video.get('created_time')}")
        print(f"Modified: {video.get('modified_time')}")
        print(f"Description: {video.get('description', 'N/A')[:100]}")
        print("-" * 60)
else:
    print(f"Error {response.status_code}: {response.text}")
# Also try without filter to see what's there
print("\n\n=== RECENT VIDEOS (all types) ===\n")
response = client.get("/me/videos", params={
    "per_page": 20,
    "sort": "modified_time",
    "direction": "desc",
    "fields": "uri,name,type,created_time,modified_time,description"
})
if response.status_code == 200:
    data = response.json()
    for video in data.get("data", []):
        if video.get('type') == 'live':
            print(f"URI: {video.get('uri')}")
            print(f"Name: {video.get('name')}")
            print(f"Type: {video.get('type')}")
            print(f"Created: {video.get('created_time')}")
            print(f"Modified: {video.get('modified_time')}")
            print(f"Description: {video.get('description', 'N/A')[:100]}")
            print("-" * 60)
else:
    print(f"Error {response.status_code}: {response.text}")
