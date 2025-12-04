#!/usr/bin/env python3
"""
Vimeo Live Event Scheduler and Tracker

This script manages Vimeo Live Events with embedded classification metadata,
providing reliable video classification through pre-configured event data.

Features:
- Create Vimeo Live Events with classification metadata in title/description
- Track all scheduled events in a JSON file
- List upcoming and past events
- Query archived videos and match them to scheduled events
- Safe test mode with non-production nomenclature

Usage:
    python3 automaton_scheduler.py create --type "Test Service A" --date 2024-12-07 --time 09:30
    python3 automaton_scheduler.py list
    python3 automaton_scheduler.py match-videos
    python3 automaton_scheduler.py --help
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables
load_dotenv()

# --- Configuration ---
VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

TIMEZONE = "America/Chicago"
SCHEDULE_FILE = Path(__file__).parent / "schedule_tracker.json"

# --- Test Event Types ---
# Using generic test nomenclature to avoid confusion with production
TEST_EVENT_TYPES = {
    "Test Service A": {
        "description": "Test service type A for classification testing",
        "folder_destination": "Worship Services",
        "typical_duration_minutes": 60,
    },
    "Test Service B": {
        "description": "Test service type B for classification testing",
        "folder_destination": "Worship Services",
        "typical_duration_minutes": 90,
    },
    "Test Class Alpha": {
        "description": "Test class type Alpha for classification testing",
        "folder_destination": "Scott's Classes",
        "typical_duration_minutes": 45,
    },
    "Test Class Beta": {
        "description": "Test class type Beta for classification testing",
        "folder_destination": "The Root Class",
        "typical_duration_minutes": 60,
    },
    "Test Special Event": {
        "description": "Test special event for classification testing",
        "folder_destination": "Weddings and Memorials",
        "typical_duration_minutes": 120,
    },
}

# Destination folder IDs (same as automaton.py)
DESTINATION_FOLDERS = {
    "Worship Services": "15749517",
    "Weddings and Memorials": "2478125",
    "Scott's Classes": "15680946",
    "The Root Class": "10606776",
}


def get_vimeo_client():
    """Initialize and return the Vimeo client."""
    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        print("ERROR: Vimeo credentials not configured in .env file")
        sys.exit(1)

    return VimeoClient(
        token=VIMEO_ACCESS_TOKEN,
        key=VIMEO_CLIENT_ID,
        secret=VIMEO_CLIENT_SECRET
    )


def load_schedule():
    """Load the schedule tracker JSON file."""
    if SCHEDULE_FILE.exists():
        with open(SCHEDULE_FILE, 'r') as f:
            return json.load(f)
    return {
        "events": [],
        "last_updated": None,
        "metadata": {
            "version": "1.0",
            "created": datetime.now(pytz.timezone(TIMEZONE)).isoformat()
        }
    }


def save_schedule(schedule_data):
    """Save the schedule tracker to JSON file."""
    schedule_data["last_updated"] = datetime.now(pytz.timezone(TIMEZONE)).isoformat()
    with open(SCHEDULE_FILE, 'w') as f:
        json.dump(schedule_data, f, indent=2)
    print(f"Schedule saved to: {SCHEDULE_FILE}")


def create_classification_metadata(event_type, scheduled_date, scheduled_time):
    """
    Create structured classification metadata for embedding in video description.
    This metadata can be parsed by the classification script.
    """
    event_config = TEST_EVENT_TYPES.get(event_type, {})

    metadata = {
        "classification": {
            "event_type": event_type,
            "scheduled_date": scheduled_date,
            "scheduled_time": scheduled_time,
            "folder_destination": event_config.get("folder_destination", "Unknown"),
            "expected_duration_minutes": event_config.get("typical_duration_minutes", 60),
        },
        "generated_by": "automaton_scheduler",
        "version": "1.0"
    }

    # Create human-readable + machine-parseable description
    description_lines = [
        f"Scheduled Event: {event_type}",
        f"Date: {scheduled_date}",
        f"Time: {scheduled_time}",
        "",
        "--- CLASSIFICATION METADATA (DO NOT EDIT) ---",
        f"AUTOMATON_METADATA:{json.dumps(metadata)}"
    ]

    return "\n".join(description_lines), metadata


def create_event_title(event_type, scheduled_date, scheduled_time):
    """
    Create a structured title for the live event.
    Format: YYYY-MM-DD - HHMM - Event Type
    """
    time_formatted = scheduled_time.replace(":", "")
    return f"{scheduled_date} - {time_formatted} - {event_type}"


def cmd_create_event(args):
    """Create a new Vimeo Live Event with classification metadata."""
    local_tz = pytz.timezone(TIMEZONE)

    # Validate event type
    if args.type not in TEST_EVENT_TYPES:
        print(f"ERROR: Unknown event type '{args.type}'")
        print(f"Available types: {', '.join(TEST_EVENT_TYPES.keys())}")
        sys.exit(1)

    # Parse and validate date/time
    try:
        scheduled_dt = datetime.strptime(f"{args.date} {args.time}", "%Y-%m-%d %H:%M")
        scheduled_dt = local_tz.localize(scheduled_dt)
    except ValueError as e:
        print(f"ERROR: Invalid date/time format: {e}")
        print("Expected: --date YYYY-MM-DD --time HH:MM")
        sys.exit(1)

    # Check if event is in the future
    now = datetime.now(local_tz)
    if scheduled_dt < now and not args.force:
        print(f"WARNING: Scheduled time {scheduled_dt} is in the past!")
        print("Use --force to create anyway (for testing)")
        sys.exit(1)

    # Create title and description
    title = create_event_title(args.type, args.date, args.time)
    description, metadata = create_classification_metadata(args.type, args.date, args.time)

    print("\n" + "=" * 60)
    print("CREATING VIMEO LIVE EVENT")
    print("=" * 60)
    print(f"Event Type: {args.type}")
    print(f"Scheduled:  {scheduled_dt.strftime('%Y-%m-%d %I:%M %p %Z')}")
    print(f"Title:      {title}")
    print(f"Folder:     {TEST_EVENT_TYPES[args.type]['folder_destination']}")
    print("=" * 60)

    if args.dry_run:
        print("\n[DRY RUN] Would create event with:")
        print(f"  Title: {title}")
        print(f"  Description:\n{description}")
        print("\n[DRY RUN] No changes made to Vimeo or schedule file.")
        return

    # Connect to Vimeo
    client = get_vimeo_client()

    # Verify connection
    user_response = client.get("/me")
    if user_response.status_code != 200:
        print(f"ERROR: Failed to connect to Vimeo API: {user_response.status_code}")
        sys.exit(1)
    print(f"Connected as: {user_response.json().get('name')}")

    # Create the live event
    print("\nCreating live event...")

    try:
        # Note: The exact API call depends on your Vimeo plan
        # Enterprise plans use /me/live_events
        # Other plans may use different endpoints

        event_data = {
            "title": title,
            "description": description,
            "time_zone": TIMEZONE,
            # Privacy settings - adjust as needed
            "privacy": {"view": "unlisted"},  # Safe for testing
            # Schedule the event
            "schedule": {
                "scheduled_time": scheduled_dt.isoformat(),
            }
        }

        response = client.post("/me/live_events", data=event_data)

        if response.status_code in [200, 201]:
            event_response = response.json()
            event_uri = event_response.get("uri", "unknown")
            event_id = event_uri.split("/")[-1] if event_uri else "unknown"

            print(f"SUCCESS: Live event created!")
            print(f"  Event URI: {event_uri}")
            print(f"  Event ID:  {event_id}")

            # Get RTMP details if available
            if "stream_key" in event_response:
                print(f"  Stream Key: {event_response['stream_key']}")
            if "rtmp_link" in event_response:
                print(f"  RTMP URL: {event_response['rtmp_link']}")

            # Save to schedule tracker
            schedule = load_schedule()
            schedule["events"].append({
                "id": event_id,
                "uri": event_uri,
                "event_type": args.type,
                "title": title,
                "scheduled_date": args.date,
                "scheduled_time": args.time,
                "scheduled_datetime_iso": scheduled_dt.isoformat(),
                "folder_destination": TEST_EVENT_TYPES[args.type]["folder_destination"],
                "status": "scheduled",
                "created_at": datetime.now(local_tz).isoformat(),
                "archived_video_id": None,  # Will be filled when video is archived
                "classification_complete": False,
                "metadata": metadata
            })
            save_schedule(schedule)

        else:
            print(f"ERROR: Failed to create live event")
            print(f"  Status: {response.status_code}")
            print(f"  Response: {response.text}")

            # Check if it's a permissions issue
            if response.status_code == 403:
                print("\nNote: Creating live events may require Vimeo Enterprise or specific API scopes.")
                print("Alternative: You can manually create the event in Vimeo and use 'register' command to track it.")

    except Exception as e:
        print(f"ERROR: Exception while creating event: {e}")
        sys.exit(1)


def cmd_register_event(args):
    """
    Register an existing Vimeo Live Event that was created manually.
    This allows tracking events created through the Vimeo web interface.
    """
    local_tz = pytz.timezone(TIMEZONE)

    # Validate event type
    if args.type not in TEST_EVENT_TYPES:
        print(f"ERROR: Unknown event type '{args.type}'")
        print(f"Available types: {', '.join(TEST_EVENT_TYPES.keys())}")
        sys.exit(1)

    # Parse date/time
    try:
        scheduled_dt = datetime.strptime(f"{args.date} {args.time}", "%Y-%m-%d %H:%M")
        scheduled_dt = local_tz.localize(scheduled_dt)
    except ValueError as e:
        print(f"ERROR: Invalid date/time format: {e}")
        sys.exit(1)

    title = create_event_title(args.type, args.date, args.time)
    _, metadata = create_classification_metadata(args.type, args.date, args.time)

    print("\n" + "=" * 60)
    print("REGISTERING EXISTING EVENT")
    print("=" * 60)
    print(f"Event ID:   {args.event_id}")
    print(f"Event Type: {args.type}")
    print(f"Scheduled:  {scheduled_dt.strftime('%Y-%m-%d %I:%M %p %Z')}")
    print("=" * 60)

    if args.dry_run:
        print("\n[DRY RUN] Would register event. No changes made.")
        return

    # Optionally verify the event exists in Vimeo
    if not args.skip_verify:
        client = get_vimeo_client()
        response = client.get(f"/videos/{args.event_id}")
        if response.status_code == 200:
            video_data = response.json()
            print(f"Verified: Found video '{video_data.get('name')}'")
        else:
            print(f"WARNING: Could not verify event ID {args.event_id} (status {response.status_code})")
            if not args.force:
                print("Use --force to register anyway")
                sys.exit(1)

    # Save to schedule tracker
    schedule = load_schedule()

    # Check for duplicate
    for event in schedule["events"]:
        if event.get("id") == args.event_id:
            print(f"WARNING: Event {args.event_id} already registered")
            if not args.force:
                print("Use --force to update existing entry")
                sys.exit(1)
            # Remove existing entry
            schedule["events"] = [e for e in schedule["events"] if e.get("id") != args.event_id]

    schedule["events"].append({
        "id": args.event_id,
        "uri": f"/videos/{args.event_id}",
        "event_type": args.type,
        "title": title,
        "scheduled_date": args.date,
        "scheduled_time": args.time,
        "scheduled_datetime_iso": scheduled_dt.isoformat(),
        "folder_destination": TEST_EVENT_TYPES[args.type]["folder_destination"],
        "status": "registered",
        "created_at": datetime.now(local_tz).isoformat(),
        "archived_video_id": args.event_id,  # For manually registered events, this is the same
        "classification_complete": False,
        "metadata": metadata,
        "manually_registered": True
    })
    save_schedule(schedule)
    print("Event registered successfully!")


def cmd_list_events(args):
    """List all tracked events."""
    schedule = load_schedule()
    events = schedule.get("events", [])

    if not events:
        print("No events in schedule tracker.")
        print(f"Schedule file: {SCHEDULE_FILE}")
        return

    local_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(local_tz)

    # Sort events by scheduled datetime
    events_sorted = sorted(
        events,
        key=lambda e: e.get("scheduled_datetime_iso", ""),
        reverse=True
    )

    # Filter by status if requested
    if args.status:
        events_sorted = [e for e in events_sorted if e.get("status") == args.status]

    # Filter upcoming only
    if args.upcoming:
        events_sorted = [
            e for e in events_sorted
            if datetime.fromisoformat(e["scheduled_datetime_iso"]) > now
        ]

    print("\n" + "=" * 80)
    print(f"SCHEDULED EVENTS ({len(events_sorted)} events)")
    print("=" * 80)

    for event in events_sorted:
        scheduled_dt = datetime.fromisoformat(event["scheduled_datetime_iso"])
        is_past = scheduled_dt < now
        status_icon = "[PAST]" if is_past else "[UPCOMING]"
        classified_icon = "[CLASSIFIED]" if event.get("classification_complete") else ""

        print(f"\n{status_icon} {classified_icon}")
        print(f"  ID:        {event.get('id', 'N/A')}")
        print(f"  Type:      {event.get('event_type', 'N/A')}")
        print(f"  Title:     {event.get('title', 'N/A')}")
        print(f"  Scheduled: {scheduled_dt.strftime('%Y-%m-%d %I:%M %p %Z')}")
        print(f"  Folder:    {event.get('folder_destination', 'N/A')}")
        print(f"  Status:    {event.get('status', 'N/A')}")
        if event.get("archived_video_id"):
            print(f"  Video ID:  {event.get('archived_video_id')}")

    print("\n" + "=" * 80)
    print(f"Schedule file: {SCHEDULE_FILE}")
    print(f"Last updated: {schedule.get('last_updated', 'Never')}")


def cmd_list_types(args):
    """List available event types."""
    print("\n" + "=" * 60)
    print("AVAILABLE EVENT TYPES (Test Nomenclature)")
    print("=" * 60)

    for type_name, config in TEST_EVENT_TYPES.items():
        print(f"\n  {type_name}")
        print(f"    Description: {config['description']}")
        print(f"    Folder: {config['folder_destination']}")
        print(f"    Typical Duration: {config['typical_duration_minutes']} minutes")

    print("\n" + "=" * 60)


def cmd_match_videos(args):
    """
    Attempt to match recent Vimeo videos to scheduled events.
    This helps identify which archived videos correspond to which events.
    """
    local_tz = pytz.timezone(TIMEZONE)
    schedule = load_schedule()
    events = schedule.get("events", [])

    if not events:
        print("No events in schedule to match against.")
        return

    client = get_vimeo_client()

    # Verify connection
    user_response = client.get("/me")
    if user_response.status_code != 200:
        print(f"ERROR: Failed to connect to Vimeo API")
        sys.exit(1)
    print(f"Connected as: {user_response.json().get('name')}")

    # Fetch recent videos
    lookback_hours = args.hours or 72
    print(f"\nFetching videos from the last {lookback_hours} hours...")

    now_utc = datetime.now(pytz.utc)
    start_time_utc = now_utc - timedelta(hours=lookback_hours)

    response = client.get(
        "/me/videos",
        params={
            "per_page": 50,
            "sort": "modified_time",
            "direction": "desc",
            "fields": "uri,name,created_time,modified_time,duration,description,live"
        }
    )

    if response.status_code != 200:
        print(f"ERROR: Failed to fetch videos: {response.status_code}")
        return

    videos = response.json().get("data", [])
    print(f"Found {len(videos)} recent videos")

    print("\n" + "=" * 80)
    print("MATCHING VIDEOS TO SCHEDULED EVENTS")
    print("=" * 80)

    matched_count = 0

    for video in videos:
        video_id = video["uri"].split("/")[-1]
        video_name = video.get("name", "Unknown")
        description = video.get("description", "") or ""

        # Check if description contains our metadata marker
        if "AUTOMATON_METADATA:" in description:
            try:
                metadata_str = description.split("AUTOMATON_METADATA:")[1].strip()
                # Handle potential extra content after JSON
                if "\n" in metadata_str:
                    metadata_str = metadata_str.split("\n")[0]
                embedded_metadata = json.loads(metadata_str)

                print(f"\n[MATCH FOUND] Video: {video_name}")
                print(f"  Video ID: {video_id}")
                print(f"  Embedded Type: {embedded_metadata.get('classification', {}).get('event_type')}")
                print(f"  Embedded Date: {embedded_metadata.get('classification', {}).get('scheduled_date')}")

                # Update schedule tracker
                for event in events:
                    if (event.get("scheduled_date") == embedded_metadata.get("classification", {}).get("scheduled_date") and
                        event.get("scheduled_time") == embedded_metadata.get("classification", {}).get("scheduled_time") and
                        event.get("event_type") == embedded_metadata.get("classification", {}).get("event_type")):

                        event["archived_video_id"] = video_id
                        event["status"] = "archived"
                        print(f"  -> Linked to scheduled event!")
                        matched_count += 1
                        break

            except (json.JSONDecodeError, IndexError) as e:
                print(f"  Warning: Could not parse embedded metadata: {e}")

        # Also try matching by title pattern
        else:
            for event in events:
                if event.get("title") and event["title"] in video_name:
                    print(f"\n[TITLE MATCH] Video: {video_name}")
                    print(f"  Video ID: {video_id}")
                    print(f"  Matched Event: {event.get('event_type')}")

                    if not event.get("archived_video_id"):
                        event["archived_video_id"] = video_id
                        event["status"] = "archived"
                        matched_count += 1
                    break

    if matched_count > 0:
        save_schedule(schedule)
        print(f"\n{matched_count} videos matched and linked to scheduled events.")
    else:
        print("\nNo new matches found.")


def cmd_classify(args):
    """
    Classify a video using the schedule tracker data.
    This is the classification logic that uses scheduled event data instead of time windows.
    """
    schedule = load_schedule()
    events = schedule.get("events", [])

    # Find the event for this video
    matching_event = None
    for event in events:
        if event.get("archived_video_id") == args.video_id or event.get("id") == args.video_id:
            matching_event = event
            break

    if not matching_event:
        print(f"No scheduled event found for video ID: {args.video_id}")
        print("You may need to run 'match-videos' first, or 'register' this event manually.")
        return

    print("\n" + "=" * 60)
    print("CLASSIFICATION RESULT")
    print("=" * 60)
    print(f"Video ID:     {args.video_id}")
    print(f"Event Type:   {matching_event.get('event_type')}")
    print(f"Service Date: {matching_event.get('scheduled_date')}")
    print(f"Service Time: {matching_event.get('scheduled_time')}")
    print(f"Destination:  {matching_event.get('folder_destination')}")

    # Generate the correct title
    event_type = matching_event.get("event_type", "Unknown")
    date = matching_event.get("scheduled_date", "0000-00-00")
    time = matching_event.get("scheduled_time", "00:00").replace(":", "")

    correct_title = f"{date} - {time} - {event_type}"
    print(f"Correct Title: {correct_title}")

    if args.apply:
        client = get_vimeo_client()

        # Get current video info
        response = client.get(f"/videos/{args.video_id}")
        if response.status_code != 200:
            print(f"ERROR: Could not fetch video: {response.status_code}")
            return

        video_data = response.json()
        current_title = video_data.get("name", "")

        if current_title != correct_title:
            print(f"\nRenaming: '{current_title}' -> '{correct_title}'")
            rename_response = client.patch(f"/videos/{args.video_id}", data={"name": correct_title})
            if rename_response.status_code in [200, 204]:
                print("Title updated successfully!")
            else:
                print(f"ERROR: Failed to rename: {rename_response.status_code}")
        else:
            print("\nTitle already correct.")

        # Move to folder
        folder_name = matching_event.get("folder_destination")
        folder_id = DESTINATION_FOLDERS.get(folder_name)

        if folder_id:
            print(f"\nMoving to folder: {folder_name} (ID: {folder_id})")
            user_response = client.get("/me")
            user_uri = user_response.json()["uri"]
            project_uri = f"{user_uri}/projects/{folder_id}"

            move_response = client.put(f"{project_uri}/videos/{args.video_id}")
            if move_response.status_code == 204:
                print("Video moved successfully!")

                # Update schedule tracker
                matching_event["classification_complete"] = True
                matching_event["status"] = "classified"
                save_schedule(schedule)
            else:
                print(f"ERROR: Failed to move video: {move_response.status_code}")
    else:
        print("\nUse --apply to actually rename and move the video.")


def main():
    parser = argparse.ArgumentParser(
        description="Vimeo Live Event Scheduler and Tracker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available test event types
  python3 automaton_scheduler.py list-types

  # Create a new test event (dry run)
  python3 automaton_scheduler.py create --type "Test Service A" --date 2024-12-07 --time 09:30 --dry-run

  # Register an existing Vimeo event
  python3 automaton_scheduler.py register --event-id 123456789 --type "Test Service A" --date 2024-12-07 --time 09:30

  # List all scheduled events
  python3 automaton_scheduler.py list

  # Match recent videos to scheduled events
  python3 automaton_scheduler.py match-videos

  # Classify a specific video
  python3 automaton_scheduler.py classify --video-id 123456789 --apply
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Create event command
    create_parser = subparsers.add_parser("create", help="Create a new Vimeo Live Event")
    create_parser.add_argument("--type", required=True, help="Event type (use 'list-types' to see options)")
    create_parser.add_argument("--date", required=True, help="Event date (YYYY-MM-DD)")
    create_parser.add_argument("--time", required=True, help="Event time (HH:MM)")
    create_parser.add_argument("--dry-run", action="store_true", help="Preview without creating")
    create_parser.add_argument("--force", action="store_true", help="Force creation even if date is in past")

    # Register existing event command
    register_parser = subparsers.add_parser("register", help="Register an existing Vimeo event")
    register_parser.add_argument("--event-id", required=True, help="Vimeo video/event ID")
    register_parser.add_argument("--type", required=True, help="Event type")
    register_parser.add_argument("--date", required=True, help="Event date (YYYY-MM-DD)")
    register_parser.add_argument("--time", required=True, help="Event time (HH:MM)")
    register_parser.add_argument("--dry-run", action="store_true", help="Preview without registering")
    register_parser.add_argument("--force", action="store_true", help="Force registration even if duplicate")
    register_parser.add_argument("--skip-verify", action="store_true", help="Skip verification of event ID")

    # List events command
    list_parser = subparsers.add_parser("list", help="List tracked events")
    list_parser.add_argument("--status", help="Filter by status (scheduled, archived, classified)")
    list_parser.add_argument("--upcoming", action="store_true", help="Show only upcoming events")

    # List types command
    subparsers.add_parser("list-types", help="List available event types")

    # Match videos command
    match_parser = subparsers.add_parser("match-videos", help="Match recent videos to scheduled events")
    match_parser.add_argument("--hours", type=int, default=72, help="Hours to look back (default: 72)")

    # Classify command
    classify_parser = subparsers.add_parser("classify", help="Classify a video using schedule data")
    classify_parser.add_argument("--video-id", required=True, help="Video ID to classify")
    classify_parser.add_argument("--apply", action="store_true", help="Actually rename and move the video")

    args = parser.parse_args()

    if args.command == "create":
        cmd_create_event(args)
    elif args.command == "register":
        cmd_register_event(args)
    elif args.command == "list":
        cmd_list_events(args)
    elif args.command == "list-types":
        cmd_list_types(args)
    elif args.command == "match-videos":
        cmd_match_videos(args)
    elif args.command == "classify":
        cmd_classify(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
