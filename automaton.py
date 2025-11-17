import os
import re
import json
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables from a .env file
load_dotenv()

# --- Configuration ---
# Reads all necessary credentials from your .env file.
VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

# Timezone for upload date calculations (e.g., 'America/Chicago' for CDT)
TIMEZONE = "America/Chicago"

# Time window to check for recent videos (in hours)
LOOKBACK_HOURS = 72  # Increased to 72 hours for debugging

# DEBUG MODE: When True, will show metadata for already-processed videos without moving them
DEBUG_MODE = True

# --- Classification Time Windows Configuration ---
# These windows are based on when videos finish processing (modified_time)
# Format: (start_hour, start_minute, end_hour, end_minute)

SATURDAY_SERVICE_WINDOW = {
    "Traditional 5:30 PM": {
        "start": (18, 15),  # 6:15 PM
        "end": (21, 0),  # 9:00 PM
        "service_date": 0,  # Same day (Saturday)
    }
}

SUNDAY_SERVICE_WINDOWS = {
    "9:30 AM": {
        "start": (10, 15),  # 10:15 AM
        "end": (11, 0),  # 11:00 AM
        "service_date": 0,  # Same day (Sunday)
    },
    "11:00 AM": {
        "start": (11, 45),  # 11:45 AM
        "end": (13, 30),  # 1:30 PM
        "service_date": 0,  # Same day (Sunday)
    },
}

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
# List of folder IDs to EXCLUDE from processing. This rule is absolute.
EXCLUDED_FOLDER_IDS = ["11103430", "182762", "8219992"]

# Destination folders for categorization
DESTINATION_FOLDERS = {
    "Worship Services": "15749517",
    "Weddings and Memorials": "2478125",
    "Scott's Classes": "15680946",
    "The Root Class": "10606776",
}


def get_vimeo_client(token, key, secret):
    """Initializes and returns the Vimeo client using token, key, and secret."""
    client = VimeoClient(token=token, key=key, secret=secret)
    return client


def print_video_debug_info(video_data, local_tz, client=None):
    """
    Prints comprehensive debug information about a video's metadata.
    """
    print("\n" + "=" * 60)
    print("DEBUG: VIDEO METADATA")
    print("=" * 60)

    # Basic info
    print(f"Title: {video_data.get('name', 'N/A')}")
    print(f"URI: {video_data.get('uri', 'N/A')}")
    print(f"Duration: {video_data.get('duration', 0)} seconds")
    print(f"Is Playable: {video_data.get('is_playable', False)}")

    # Timestamps
    print("\n--- TIMESTAMPS ---")
    for field in ["created_time", "modified_time", "release_time"]:
        if video_data.get(field):
            try:
                dt_utc = datetime.fromisoformat(
                    video_data[field].replace("Z", "+00:00")
                )
                dt_local = dt_utc.astimezone(local_tz)
                print(f"{field:20s}: {video_data[field]} (UTC)")
                print(
                    f"{'':20s}  -> {dt_local.strftime('%Y-%m-%d %I:%M:%S %p %Z')} (Local)"
                )
            except:
                print(f"{field:20s}: {video_data[field]} (parse error)")
        else:
            print(f"{field:20s}: NOT PRESENT")

    # Live event data
    print("\n--- LIVE EVENT DATA ---")
    live_data = video_data.get("live")
    if live_data:
        print(json.dumps(live_data, indent=2))
    else:
        print("No live event data present")

    # Try to fetch full video details for additional fields
    if client:
        try:
            print("\n--- FETCHING FULL VIDEO DETAILS ---")
            response = client.get(video_data['uri'])
            if response.status_code == 200:
                full_data = response.json()

                # Check for any live-related fields
                if 'live' in full_data and full_data['live']:
                    print("Full live event data from detailed fetch:")
                    print(json.dumps(full_data['live'], indent=2))

                # Check for content rating or tags that might indicate time
                if 'tags' in full_data:
                    print(f"\nTags: {full_data.get('tags', [])}")

                if 'description' in full_data:
                    print(f"\nDescription: {full_data.get('description', 'N/A')}")

        except Exception as e:
            print(f"Could not fetch full video details: {e}")

    print("=" * 60 + "\n")

    return live_data


def is_time_in_window(dt, start_tuple, end_tuple):
    """
    Check if a datetime falls within a time window.

    Args:
        dt: datetime object to check
        start_tuple: (hour, minute) for window start
        end_tuple: (hour, minute) for window end

    Returns:
        bool: True if time falls within window
    """
    start_hour, start_minute = start_tuple
    end_hour, end_minute = end_tuple

    time_minutes = dt.hour * 60 + dt.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute

    return start_minutes <= time_minutes <= end_minutes


def get_best_timestamp(video_data, local_tz):
    """
    Get the most reliable timestamp from video data for classification.
    Tries multiple fields in order of reliability:
    1. release_time (if available)
    2. modified_time (most reliable for processed videos)
    3. created_time (fallback)

    Returns:
        tuple: (datetime in local timezone, source field name)
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

    # Use modified_time (most reliable for our use case)
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
    """Fetches all videos recently modified to find candidates for processing."""
    print(f"Fetching all videos modified in the last {lookback_hours} hours...")

    # Calculate the start time for the lookback window
    now_utc = datetime.now(pytz.utc)
    start_time_utc = now_utc - timedelta(hours=lookback_hours)

    all_recent_videos = []

    try:
        # Sort by modified_time to find recently finished archives
        response = client.get(
            "/me/videos",
            params={
                "per_page": 100,
                "sort": "modified_time",
                "direction": "desc",
                "fields": "uri,name,created_time,modified_time,release_time,duration,parent_folder,is_playable,live.status,live.streaming_start_time,live.time,live.scheduled_start_time,live.ended_time,live.archived_time",
            },
        )
        response.raise_for_status()

        videos = response.json().get("data", [])

        # DEBUG: Show what fields are being returned for the first video
        if videos:
            print("\n" + "=" * 60)
            print("DEBUG: Sample of fields returned by API (first video)")
            print("=" * 60)
            print(f"Available fields: {list(videos[0].keys())}")
            print("=" * 60 + "\n")

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
                # Since the list is sorted, we can stop once we're outside the window.
                break

    except Exception as e:
        print(f"An error occurred while fetching videos: {e}")

    print(f"Found {len(all_recent_videos)} recently modified videos to check.")
    return all_recent_videos


def process_video(client, video_data, debug_only=False):
    """
    Determines the correct title and category, then renames and moves the video if necessary.
    Uses time-window based classification on modified_time for accuracy.
    Returns a dictionary with the results of the operations.

    Args:
        client: Vimeo client instance
        video_data: Video metadata dict
        debug_only: If True, only show debug info without renaming/moving
    """
    stats = {"title_updated": False, "moved": False}
    current_title = video_data.get("name", "")
    local_tz = pytz.timezone(TIMEZONE)

    # --- DEBUG: Print all video metadata ---
    live_data = print_video_debug_info(video_data, local_tz, client)

    # If debug_only mode, skip all processing after showing metadata
    if debug_only:
        print("  - DEBUG MODE: Skipping rename/move operations")
        return stats

    # --- 1. Get Best Timestamp for Classification ---
    reference_time, time_source = get_best_timestamp(video_data, local_tz)
    print(
        f"  - Using {time_source} for classification: {reference_time.strftime('%Y-%m-%d %I:%M %p')}"
    )

    # Extract duration if available
    duration_seconds = video_data.get("duration", 0)
    duration_minutes = duration_seconds / 60 if duration_seconds else 0
    if duration_minutes > 0:
        print(f"  - Video duration: {duration_minutes:.1f} minutes")

    # --- 2. Prepare Title for Categorization ---
    original_title_for_categorization = re.sub(
        r"^\d{4}-\d{2}-\d{2} - ", "", current_title
    )
    video_title_lower = original_title_for_categorization.lower()

    # --- 3. Determine Service Date ---
    # For most services, the service date is the same as the reference time date
    service_date = reference_time.date()
    day_of_week = reference_time.weekday()  # Monday is 0, Sunday is 6

    # Special case: Saturday services that finish after midnight
    # If we detect a Sunday early morning timestamp (before 6 AM) with worship keywords,
    # it might be a Saturday service that went past midnight
    if day_of_week == 6 and reference_time.hour < 6:  # Sunday before 6 AM
        if "worship" in video_title_lower or "traditional" in video_title_lower:
            print(
                "  - Early Sunday morning timestamp detected - checking if this is a late Saturday service"
            )
            # Adjust to previous day (Saturday) for classification
            adjusted_time = reference_time - timedelta(days=1)
            day_of_week = adjusted_time.weekday()
            service_date = adjusted_time.date()
            reference_time = adjusted_time
            print(f"  - Adjusted to Saturday for classification purposes")

    # --- 4. Display Classification Context ---
    print(
        f"  - Day of week: {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][day_of_week]}"
    )
    print(f"  - Service date: {service_date.strftime('%Y-%m-%d')}")
    print(f"  - Title keywords: {video_title_lower[:50]}...")

    category_folder_name = None
    final_title_suffix = None

    # --- 5. Classification Logic Using Time Windows ---

    # Check for The Root Class first (more specific)
    if (
        "capture - piro hall" in video_title_lower
        or "the root class" in video_title_lower
        or "root" in video_title_lower
    ):
        print("  - Detected 'Root Class' keywords in title")

        # Monday Root Class
        if day_of_week == 0:  # Monday
            window = ROOT_CLASS_WINDOWS["Monday"]
            if is_time_in_window(reference_time, window["start"], window["end"]):
                category_folder_name = "The Root Class"
                final_title_suffix = "The Root Class"
                print(f"    → Classified as Monday Root Class")
            else:
                print(f"    → Outside Monday Root Class window")

        # Sunday Root Class (overlaps with 9:30 worship, so needs title check)
        elif day_of_week == 6:  # Sunday
            window = ROOT_CLASS_WINDOWS["Sunday"]
            if is_time_in_window(reference_time, window["start"], window["end"]):
                category_folder_name = "The Root Class"
                final_title_suffix = "0930 - The Root Class"
                print(f"    → Classified as Sunday Root Class (9:30 AM)")
            else:
                print(f"    → Outside Sunday Root Class window")

    # Check for Worship Services
    elif (
        "worship" in video_title_lower
        or "contemporary" in video_title_lower
        or "traditional" in video_title_lower
    ):
        print("  - Detected 'Worship Service' keywords in title")
        service_type = (
            "Contemporary" if "contemporary" in video_title_lower else "Traditional"
        )
        print(f"    → Service type: {service_type}")

        # Saturday Service
        if day_of_week == 5:  # Saturday
            for service_name, window in SATURDAY_SERVICE_WINDOW.items():
                if is_time_in_window(reference_time, window["start"], window["end"]):
                    category_folder_name = "Worship Services"
                    final_title_suffix = f"Worship Service - {service_name}"
                    print(f"    → Classified as Saturday {service_name}")
                    break

        # Sunday Services
        elif day_of_week == 6:  # Sunday
            # Check each Sunday service window
            for service_time, window in SUNDAY_SERVICE_WINDOWS.items():
                if is_time_in_window(reference_time, window["start"], window["end"]):
                    category_folder_name = "Worship Services"
                    final_title_suffix = (
                        f"Worship Service - {service_type} {service_time}"
                    )
                    print(f"    → Classified as Sunday {service_type} {service_time}")
                    break

            # If no window matched, use fallback logic
            if not category_folder_name:
                print(
                    f"    → No Sunday window matched for time {reference_time.strftime('%I:%M %p')}"
                )
                print(f"    → Applying fallback: treating as wedding/memorial")
                category_folder_name = "Weddings and Memorials"
                final_title_suffix = "Memorial or Wedding Service"

        # Worship service on a non-service day (probably a wedding/memorial)
        else:
            print(
                "  - 'Worship' title found on a non-service day. Categorizing as 'Weddings and Memorials'."
            )
            category_folder_name = "Weddings and Memorials"
            final_title_suffix = "Memorial or Wedding Service"

    # Check for explicit Memorial/Wedding keywords
    elif "memorial" in video_title_lower or "wedding" in video_title_lower:
        print("  - Detected 'Memorial' or 'Wedding' keywords")
        category_folder_name = "Weddings and Memorials"
        final_title_suffix = "Memorial or Wedding Service"

    # Check for Scott's Classes
    elif "scott" in video_title_lower or (
        "class" in video_title_lower and "root" not in video_title_lower
    ):
        print("  - Detected 'Scott's Class' keywords")
        category_folder_name = "Scott's Classes"
        final_title_suffix = (
            original_title_for_categorization  # Use the original title for classes
        )

    # --- 6. Rename and Move ---
    if category_folder_name and final_title_suffix:
        # Determine the correct service date for the title
        correct_date_str = service_date.strftime("%Y-%m-%d")
        new_title = f"{correct_date_str} - {final_title_suffix}"

        print(f"  - Proposed title: '{new_title}'")

        # Rename if the current title is not exactly correct
        if current_title != new_title:
            print(f"  - Updating title to: '{new_title}'")
            try:
                client.patch(video_data["uri"], data={"name": new_title})
                print("    - Successfully updated title.")
                stats["title_updated"] = True
            except Exception as e:
                print(f"    - An error occurred while updating title: {e}")
                return stats
        else:
            print("  - Skipping rename: Title is already correct.")

        # Move to the correct folder
        folder_id = DESTINATION_FOLDERS.get(category_folder_name)
        if folder_id:
            print(
                f"  - Moving to folder for '{category_folder_name}' (ID: {folder_id})."
            )
            try:
                user_response = client.get("/me")
                user_uri = user_response.json()["uri"]
                project_uri = f"{user_uri}/projects/{folder_id}"
                video_uri_id = video_data["uri"].split("/")[-1]

                move_response = client.put(f"{project_uri}/videos/{video_uri_id}")
                if move_response.status_code == 204:
                    print("    - Successfully moved video.")
                    stats["moved"] = True
                else:
                    print(
                        f"    - Error moving video: {move_response.status_code} - {move_response.text}"
                    )
            except Exception as e:
                print(f"    - An error occurred while moving video: {e}")
    else:
        print("  - No categorization rule matched. Video will not be moved.")

    return stats


def main():
    """Main function to run the Vimeo video management script."""
    print("--- Starting Vimeo Automation Script ---")

    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        print("ERROR: Vimeo credentials are not fully configured.")
        print(
            "Please ensure VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, and VIMEO_CLIENT_SECRET are in your .env file."
        )
        return

    client = get_vimeo_client(VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET)

    user_response = client.get("/me")
    if user_response.status_code != 200:
        print(
            f"Failed to connect to Vimeo API. Status: {user_response.status_code}, Response: {user_response.json()}"
        )
        return
    print(f"Successfully connected to Vimeo as: {user_response.json().get('name')}")

    videos_to_check = get_recent_videos(client, LOOKBACK_HOURS)

    # --- Initialize Counters ---
    scanned_count = len(videos_to_check)
    processed_count = 0
    updated_count = 0
    moved_count = 0

    if not videos_to_check:
        print("No new videos found to process.")
    else:
        for video in videos_to_check:
            print("\n" + "-" * 20)
            print(f"Checking video: {video['name']} ({video['uri']})")

            # Rule 1: Only process playable videos.
            if not video.get("is_playable"):
                print(
                    "  - Skipping: Video is not playable (likely a phantom live event object)."
                )
                continue

            # Get parent folder info for exclusion checks
            parent_folder = video.get("parent_folder")
            parent_folder_id = None
            if parent_folder:
                parent_folder_id = parent_folder["uri"].split("/")[-1]

            # Rule 2: Skip if the video is in an excluded folder.
            if parent_folder_id and parent_folder_id in EXCLUDED_FOLDER_IDS:
                if DEBUG_MODE:
                    print(
                        f"  - DEBUG MODE: Checking excluded folder video '{parent_folder.get('name')}' (will not process)."
                    )
                    # Show metadata for excluded videos in debug mode
                    live_data = print_video_debug_info(video, pytz.timezone(TIMEZONE), client)
                    continue
                else:
                    print(
                        f"  - Skipping: Video is in an excluded folder ('{parent_folder.get('name')}')."
                    )
                    continue

            # Rule 3: Only process videos in the Team Library (root).
            if parent_folder is not None:
                if parent_folder_id in DESTINATION_FOLDERS.values():
                    if DEBUG_MODE:
                        print(
                            f"  - DEBUG MODE: Checking already-processed video in '{parent_folder.get('name')}' (will not move/rename)."
                        )
                        # Process for debug but don't actually move/rename
                        processed_count += 1
                        stats = process_video(client, video, debug_only=True)
                        continue
                    else:
                        print(
                            f"  - Skipping: Video is already in a destination folder ('{parent_folder.get('name')}')."
                        )
                        continue
                else:
                    print(
                        f"  - Skipping: Video is not in the Team Library root (it's in '{parent_folder.get('name')}')."
                    )
                    continue

            # If the video passes all checks, process it.
            print("  - Video is valid for processing.")
            processed_count += 1
            stats = process_video(client, video)
            if stats["title_updated"]:
                updated_count += 1
            if stats["moved"]:
                moved_count += 1

    # --- Print Final Summary ---
    print("\n" + "=" * 30)
    print("--- Processing Summary ---")
    print(f"Videos Scanned: {scanned_count}")
    print(f"Videos Processed: {processed_count}")
    print(f"Titles Updated: {updated_count}")
    print(f"Videos Moved: {moved_count}")
    print("=" * 30)
    print("\n--- Script Finished ---")


if __name__ == "__main__":
    main()
