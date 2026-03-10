import os
import re
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables from a .env file
load_dotenv()

# --- Configuration ---
VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

# Timezone for upload date calculations
TIMEZONE = "America/Chicago"

# Time window to check for recent videos (in hours)
LOOKBACK_HOURS = 48

# --- Service Time Options ---
SATURDAY_SERVICE_TIME = "5:30 PM"

# The Root Class time windows
ROOT_CLASS_WINDOWS = {
    "Monday": {
        "start": (19, 0),  # 7:00 PM
        "end": (21, 0),  # 9:00 PM
    },
    "Sunday": {
        "start": (10, 15),  # 10:15 AM
        "end": (11, 0),  # 11:00 AM
    },
}

# --- Folder Configuration ---
EXCLUDED_FOLDER_IDS = ["11103430", "182762", "8219992"]

DESTINATION_FOLDERS = {
    "Worship Services": "15749517",
    "Weddings and Memorials": "2478125",
    "Scott's Classes": "15680946",
    "The Root Class": "10606776",
}


def get_vimeo_client(token, key, secret):
    """Initializes and returns the Vimeo client."""
    return VimeoClient(token=token, key=key, secret=secret)


def is_time_in_window(dt, start_tuple, end_tuple):
    """Check if a datetime falls within a time window."""
    start_hour, start_minute = start_tuple
    end_hour, end_minute = end_tuple

    time_minutes = dt.hour * 60 + dt.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute

    return start_minutes <= time_minutes <= end_minutes


def extract_time_from_title(title):
    """
    Extract service time from title if present.
    Returns formatted time string like "9:30 AM" or "11:00 AM", or None if not found.
    """
    # Pattern to match times like "9:30", "11:00", "9:30 AM", "11:00 AM", "0930", "1100"
    time_patterns = [
        r'(\d{1,2}):(\d{2})\s*(AM|PM)?',  # Matches "9:30", "11:00", "9:30 AM"
        r'(\d{2})(\d{2})\s*(AM|PM)?',      # Matches "0930", "1100"
    ]

    for pattern in time_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            if ':' in match.group(0):
                # Format: "9:30" or "9:30 AM"
                hour = int(match.group(1))
                minute = match.group(2)
            else:
                # Format: "0930" or "1100"
                hour = int(match.group(1))
                minute = match.group(2)

            # Determine AM/PM
            if match.lastindex >= 3 and match.group(3):
                period = match.group(3).upper()
            else:
                # Default to AM for morning times
                period = "AM" if hour < 12 else "PM"

            # Convert to 12-hour format if needed
            if hour == 0:
                hour = 12
            elif hour > 12:
                hour = hour - 12
                period = "PM"

            return f"{hour}:{minute} {period}"

    return None


def get_best_timestamp(video_data, local_tz):
    """
    Get the most reliable timestamp from video data.
    Priority: release_time > modified_time > created_time
    """
    # Try release_time first
    if video_data.get("release_time"):
        try:
            dt = datetime.fromisoformat(
                video_data["release_time"].replace("Z", "+00:00")
            ).astimezone(local_tz)
            return (dt, "release_time")
        except:
            pass

    # Use modified_time
    if video_data.get("modified_time"):
        dt = datetime.fromisoformat(
            video_data["modified_time"].replace("Z", "+00:00")
        ).astimezone(local_tz)
        return (dt, "modified_time")

    # Fallback to created_time
    dt = datetime.fromisoformat(
        video_data["created_time"].replace("Z", "+00:00")
    ).astimezone(local_tz)
    return (dt, "created_time")


def get_recent_videos(client, lookback_hours):
    """Fetches all videos recently modified."""
    print(f"Fetching all videos modified in the last {lookback_hours} hours...")

    now_utc = datetime.now(pytz.utc)
    start_time_utc = now_utc - timedelta(hours=lookback_hours)

    all_recent_videos = []

    try:
        response = client.get(
            "/me/videos",
            params={
                "per_page": 100,
                "sort": "modified_time",
                "direction": "desc",
                "fields": "uri,name,created_time,modified_time,release_time,duration,parent_folder,is_playable",
            },
        )
        response.raise_for_status()

        videos = response.json().get("data", [])

        for video in videos:
            modified_time_str = video.get("modified_time")
            if not modified_time_str:
                continue

            modified_time_utc = datetime.fromisoformat(
                modified_time_str.replace("Z", "+00:00")
            )
            if modified_time_utc >= start_time_utc:
                all_recent_videos.append(video)
            else:
                break

    except Exception as e:
        print(f"An error occurred while fetching videos: {e}")

    print(f"Found {len(all_recent_videos)} recently modified videos to check.")
    return all_recent_videos


def classify_video(video_data):
    """
    Classify a video and return classification info.

    Returns a dict with:
    - classification_type: "sunday_worship", "saturday_worship", "root_class", "scott_class", "wedding_memorial", "unclassified"
    - service_type: "Traditional" or "Contemporary" (for worship services)
    - service_date: date object
    - title_suffix: The title suffix to use
    - folder: The destination folder
    """
    local_tz = pytz.timezone(TIMEZONE)
    current_title = video_data.get("name", "")

    # Get timestamp
    reference_time, _ = get_best_timestamp(video_data, local_tz)

    # Prepare title for categorization
    original_title = re.sub(r"^\d{4}-\d{2}-\d{2} - ", "", current_title)
    video_title_lower = original_title.lower()

    # Determine service date
    service_date = reference_time.date()
    day_of_week = reference_time.weekday()  # Monday=0, Sunday=6

    # Special case: Saturday services that finish after midnight
    if day_of_week == 6 and reference_time.hour < 6:  # Sunday before 6 AM
        if "worship" in video_title_lower or "traditional" in video_title_lower:
            adjusted_time = reference_time - timedelta(days=1)
            day_of_week = adjusted_time.weekday()
            service_date = adjusted_time.date()
            reference_time = adjusted_time

    result = {
        "video_data": video_data,
        "service_date": service_date,
        "day_of_week": day_of_week,
        "reference_time": reference_time,
        "original_title": original_title,
        "classification_type": "unclassified",
    }

    # Check for The Root Class
    if "capture - piro hall" in video_title_lower or "the root class" in video_title_lower or "root" in video_title_lower:
        # Monday Root Class
        if day_of_week == 0:
            window = ROOT_CLASS_WINDOWS["Monday"]
            if is_time_in_window(reference_time, window["start"], window["end"]):
                result["classification_type"] = "root_class"
                result["title_suffix"] = "The Root Class"
                result["folder"] = "The Root Class"
                return result

        # Sunday Root Class
        elif day_of_week == 6:
            window = ROOT_CLASS_WINDOWS["Sunday"]
            if is_time_in_window(reference_time, window["start"], window["end"]):
                result["classification_type"] = "root_class"
                result["title_suffix"] = "0930 - The Root Class"
                result["folder"] = "The Root Class"
                return result

    # Check for Worship Services
    elif "worship" in video_title_lower or "contemporary" in video_title_lower or "traditional" in video_title_lower:
        service_type = "Contemporary" if "contemporary" in video_title_lower else "Traditional"
        result["service_type"] = service_type

        # Saturday Service
        if day_of_week == 5:
            result["classification_type"] = "saturday_worship"
            result["title_suffix"] = f"Worship Service - Traditional {SATURDAY_SERVICE_TIME}"
            result["folder"] = "Worship Services"
            return result

        # Sunday Services - Extract time or use defaults
        elif day_of_week == 6:
            result["classification_type"] = "sunday_worship"
            result["folder"] = "Worship Services"

            # Try to extract time from title
            extracted_time = extract_time_from_title(current_title)

            if extracted_time:
                # Time found in title - use it
                service_time = extracted_time
            else:
                # No time in title - apply defaults
                # Contemporary → 9:30 AM (Smith Worship Center)
                # Traditional → 11:00 AM (Hasley Chapel)
                service_time = "9:30 AM" if service_type == "Contemporary" else "11:00 AM"

            result["title_suffix"] = f"Worship Service - {service_type} {service_time}"
            return result

        # Worship on non-service day
        else:
            result["classification_type"] = "wedding_memorial"
            result["title_suffix"] = "Memorial or Wedding Service"
            result["folder"] = "Weddings and Memorials"
            return result

    # Check for explicit Memorial/Wedding
    elif "memorial" in video_title_lower or "wedding" in video_title_lower:
        result["classification_type"] = "wedding_memorial"
        result["title_suffix"] = "Memorial or Wedding Service"
        result["folder"] = "Weddings and Memorials"
        return result

    # Check for Scott's Classes
    elif "scott" in video_title_lower or ("class" in video_title_lower and "root" not in video_title_lower):
        result["classification_type"] = "scott_class"
        result["title_suffix"] = original_title
        result["folder"] = "Scott's Classes"
        return result

    return result


def apply_classification(client, classification):
    """Apply the classification to a video (rename and move)."""
    video_data = classification["video_data"]
    service_date = classification["service_date"]

    # Get title suffix
    title_suffix = classification.get("title_suffix")
    if not title_suffix:
        return {"title_updated": False, "moved": False}

    # Build new title
    correct_date_str = service_date.strftime("%Y-%m-%d")
    new_title = f"{correct_date_str} - {title_suffix}"
    current_title = video_data.get("name", "")

    stats = {"title_updated": False, "moved": False}

    # Rename if needed
    if current_title != new_title:
        print(f"  - Updating title to: '{new_title}'")
        try:
            client.patch(video_data["uri"], data={"name": new_title})
            print("    ✓ Successfully updated title")
            stats["title_updated"] = True
        except Exception as e:
            print(f"    ✗ Error updating title: {e}")
            return stats
    else:
        print(f"  - Title already correct: '{new_title}'")

    # Move to folder
    folder_name = classification.get("folder")
    if folder_name:
        folder_id = DESTINATION_FOLDERS.get(folder_name)
        if folder_id:
            print(f"  - Moving to '{folder_name}' folder...")
            try:
                user_response = client.get("/me")
                user_uri = user_response.json()["uri"]
                project_uri = f"{user_uri}/projects/{folder_id}"
                video_uri_id = video_data["uri"].split("/")[-1]

                move_response = client.put(f"{project_uri}/videos/{video_uri_id}")
                if move_response.status_code == 204:
                    print("    ✓ Successfully moved video")
                    stats["moved"] = True
                else:
                    print(f"    ✗ Error moving: {move_response.status_code}")
            except Exception as e:
                print(f"    ✗ Error moving video: {e}")

    return stats


def main():
    """Main function."""
    print("=" * 60)
    print("VIMEO VIDEO CLASSIFICATION SCRIPT (AUTOMATED)")
    print("=" * 60)

    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        print("ERROR: Vimeo credentials not configured in .env file")
        return

    client = get_vimeo_client(VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET)

    # Test connection
    user_response = client.get("/me")
    if user_response.status_code != 200:
        print(f"Failed to connect to Vimeo API: {user_response.status_code}")
        return
    print(f"✓ Connected as: {user_response.json().get('name')}\n")

    # Get recent videos
    videos_to_check = get_recent_videos(client, LOOKBACK_HOURS)

    # Track videos and stats
    videos_to_process = []
    skipped_count = 0

    print("\n" + "=" * 60)
    print("CLASSIFYING VIDEOS")
    print("=" * 60)

    for video in videos_to_check:
        print(f"\n[{video['name']}]")

        # Rule 1: Only playable videos
        if not video.get("is_playable"):
            print("  ⊘ Skipping: Not playable (phantom live event)")
            skipped_count += 1
            continue

        # Get parent folder
        parent_folder = video.get("parent_folder")
        parent_folder_id = None
        if parent_folder:
            parent_folder_id = parent_folder["uri"].split("/")[-1]

        # Rule 2: Skip excluded folders
        if parent_folder_id and parent_folder_id in EXCLUDED_FOLDER_IDS:
            print(f"  ⊘ Skipping: In excluded folder '{parent_folder.get('name')}'")
            skipped_count += 1
            continue

        # Rule 3: Only Team Library root
        if parent_folder is not None:
            if parent_folder_id in DESTINATION_FOLDERS.values():
                print(f"  ⊘ Skipping: Already in destination folder '{parent_folder.get('name')}'")
            else:
                print(f"  ⊘ Skipping: Not in Team Library root")
            skipped_count += 1
            continue

        # Classify the video
        classification = classify_video(video)

        if classification["classification_type"] == "unclassified":
            print("  ⊘ No classification rule matched")
            skipped_count += 1
            continue

        print(f"  → {classification.get('title_suffix', 'Classified')}")
        videos_to_process.append(classification)

    # Process all classified videos
    print("\n" + "=" * 60)
    print("PROCESSING CLASSIFIED VIDEOS")
    print("=" * 60)

    videos_updated = 0
    videos_moved = 0

    for classification in videos_to_process:
        video_name = classification["video_data"]["name"]
        print(f"\n[{video_name}]")
        stats = apply_classification(client, classification)
        if stats["title_updated"]:
            videos_updated += 1
        if stats["moved"]:
            videos_moved += 1

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Videos Scanned: {len(videos_to_check)}")
    print(f"Skipped: {skipped_count}")
    print(f"Classified and Processed: {len(videos_to_process)}")
    print(f"  - Titles Updated: {videos_updated}")
    print(f"  - Videos Moved: {videos_moved}")
    print("=" * 60)
    print("\nSUNDAY SERVICE TIME LOGIC:")
    print("  - If time found in title → Uses that time")
    print("  - Contemporary (no time) → Defaults to 9:30 AM")
    print("  - Traditional (no time) → Defaults to 11:00 AM")
    print("\nDone!")


if __name__ == "__main__":
    main()
