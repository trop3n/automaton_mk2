#!/usr/bin/env python3
"""
Manual Review Tool for Vimeo Video Classification

Provides a CLI interface for manually reviewing and classifying videos
that couldn't be automatically classified with high confidence.

Usage:
    python3 review_videos.py list
    python3 review_videos.py show VIDEO_ID
    python3 review_videos.py classify VIDEO_ID --event EVENT_ID --service SERVICE_INDEX
    python3 review_videos.py skip VIDEO_ID --reason "Not a weekend service"
    python3 review_videos.py stats
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient


load_dotenv()


VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

CONFIG_FILE = Path(__file__).parent / "live_events_config.json"
REVIEW_QUEUE_FILE = Path(__file__).parent / "review_queue.json"


def load_config() -> Dict:
    """Load configuration."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)


def load_review_queue() -> Dict:
    """Load review queue."""
    if REVIEW_QUEUE_FILE.exists():
        with open(REVIEW_QUEUE_FILE, 'r') as f:
            return json.load(f)
    return {"videos": []}


def save_review_queue(queue: Dict):
    """Save review queue."""
    with open(REVIEW_QUEUE_FILE, 'w') as f:
        json.dump(queue, f, indent=2)


def get_vimeo_client() -> VimeoClient:
    """Get Vimeo client."""
    return VimeoClient(
        token=VIMEO_ACCESS_TOKEN,
        key=VIMEO_CLIENT_ID,
        secret=VIMEO_CLIENT_SECRET
    )


def format_duration(seconds: int) -> str:
    """Format duration in seconds to human-readable string."""
    minutes = seconds // 60
    hours = minutes // 60
    minutes = minutes % 60
    
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def cmd_list(args):
    """List all videos in review queue."""
    queue = load_review_queue()
    videos = queue.get('videos', [])
    
    pending = [v for v in videos if v.get('status') == 'pending_review']
    reviewed = [v for v in videos if v.get('status') != 'pending_review']
    
    print("\n" + "=" * 80)
    print("VIDEOS IN REVIEW QUEUE")
    print("=" * 80)
    
    if pending:
        print(f"\nPENDING REVIEW ({len(pending)} videos)\n")
        for i, video in enumerate(pending, 1):
            print(f"{i}. {video['video_name']}")
            print(f"   Video ID: {video['video_id']}")
            print(f"   Modified: {video['modified_time']}")
            print(f"   Duration: {format_duration(video.get('duration', 0))}")
            print(f"   Confidence: {video['classification_attempts'][-1]['confidence']:.0%}")
            print(f"   Reason: {video['classification_attempts'][-1]['warnings'][0]}")
            print(f"   Added: {video['added_to_queue']}")
            print()
    else:
        print("\nNo videos pending review\n")
    
    if reviewed and args.show_reviewed:
        print(f"\nRECENTLY REVIEWED ({len(reviewed)} videos)\n")
        for i, video in enumerate(reviewed[-10:], 1):
            print(f"{i}. {video['video_name']} - {video.get('status', 'unknown')}")
            print()
    
    print("=" * 80)
    print(f"\nTotal pending: {len(pending)}")
    print(f"Total reviewed: {len(reviewed)}")
    print()


def cmd_show(args):
    """Show detailed information about a specific video."""
    queue = load_review_queue()
    config = load_config()
    
    video = None
    for v in queue['videos']:
        if v['video_id'] == args.video_id:
            video = v
            break
    
    if not video:
        print(f"ERROR: Video {args.video_id} not found in review queue")
        return
    
    print("\n" + "=" * 80)
    print(f"VIDEO DETAILS: {video['video_id']}")
    print("=" * 80)
    
    print(f"\nName: {video['video_name']}")
    print(f"URI: {video['uri']}")
    print(f"Video ID: {video['video_id']}")
    print(f"Modified Time: {video['modified_time']}")
    print(f"Duration: {format_duration(video.get('duration', 0))}")
    print(f"Status: {video.get('status', 'unknown')}")
    print(f"Added to Queue: {video['added_to_queue']}")
    
    print(f"\nCLASSIFICATION ATTEMPTS")
    print("-" * 80)
    for i, attempt in enumerate(video['classification_attempts'], 1):
        print(f"\nAttempt #{i}:")
        print(f"  Timestamp: {attempt['timestamp']}")
        print(f"  Method: {attempt['method']}")
        print(f"  Confidence: {attempt['confidence']:.0%}")
        if attempt.get('suggested_title'):
            print(f"  Suggested Title: {attempt['suggested_title']}")
        if attempt['warnings']:
            print(f"  Warnings:")
            for warning in attempt['warnings']:
                print(f"    - {warning}")
    
    print(f"\nPOSSIBLE CLASSIFICATIONS")
    print("-" * 80)
    print("\nMatching events by name:\n")
    
    for event_id, event_config in config['events'].items():
        if video['video_name'] == event_config['name']:
            print(f"Event: {event_config['name']} (ID: {event_id})")
            print(f"Folder: {event_config['folder']}")
            print(f"Services:")
            for i, service in enumerate(event_config['services']):
                print(f"  [{i}] {service['day']} {service['stream_time']} - {service['title_format']}")
            print()
    
    print("\nTo classify this video:")
    print(f"  python3 review_videos.py classify {video['video_id']} --event EVENT_ID --service SERVICE_INDEX")
    print()
    print("To skip this video:")
    print(f"  python3 review_videos.py skip {video['video_id']} --reason \"Your reason\"")
    print()


def cmd_classify(args):
    """Manually classify a video."""
    queue = load_review_queue()
    config = load_config()
    client = get_vimeo_client()
    
    video = None
    video_index = None
    for i, v in enumerate(queue['videos']):
        if v['video_id'] == args.video_id:
            video = v
            video_index = i
            break
    
    if not video:
        print(f"ERROR: Video {args.video_id} not found in review queue")
        return
    
    event_config = config['events'].get(args.event_id)
    if not event_config:
        print(f"ERROR: Event {args.event_id} not found in configuration")
        return
    
    if args.service_index >= len(event_config['services']):
        print(f"ERROR: Service index {args.service_index} out of range (0-{len(event_config['services'])-1})")
        return
    
    service = event_config['services'][args.service_index]
    
    print(f"\n{'='*80}")
    print("MANUAL CLASSIFICATION")
    print("=" * 80)
    print(f"\nVideo: {video['video_name']}")
    print(f"Video ID: {video['video_id']}")
    print(f"\nEvent: {event_config['name']} (ID: {args.event_id})")
    print(f"Service: {service['day']} {service['stream_time']}")
    print(f"Title Format: {service['title_format']}")
    print(f"Folder: {event_config['folder']}")
    
    modified_time = datetime.fromisoformat(video['modified_time'].replace('Z', '+00:00'))
    modified_time = modified_time.astimezone(pytz.timezone(config['settings']['timezone']))
    
    title = service['title_format']
    title = title.replace('YYYY', str(modified_time.year))
    title = title.replace('MM', f'{modified_time.month:02d}')
    title = title.replace('DD', f'{modified_time.day:02d}')
    
    print(f"\nNew Title: {title}")
    
    if not args.force:
        confirm = input("\nProceed with classification? [y/N]: ")
        if confirm.lower() != 'y':
            print("Cancelled")
            return
    
    try:
        print(f"\nUpdating title...")
        response = client.patch(f"/videos/{video['video_id']}", data={"name": title})
        if response.status_code not in [200, 204]:
            print(f"ERROR: Failed to update title: {response.status_code}")
            return
        print(f"✓ Title updated")
        
        print(f"\nMoving to folder...")
        user_response = client.get("/me")
        user_uri = user_response.json()["uri"]
        project_uri = f"{user_uri}/projects/{event_config['folder_id']}"
        
        move_response = client.put(f"{project_uri}/videos/{video['video_id']}")
        if move_response.status_code != 204:
            print(f"ERROR: Failed to move video: {move_response.status_code}")
            return
        print(f"✓ Video moved to {event_config['folder']}")
        
        queue['videos'][video_index]['status'] = 'manually_classified'
        queue['videos'][video_index]['manual_classification'] = {
            'timestamp': datetime.now(pytz.timezone(config['settings']['timezone'])).isoformat(),
            'event_id': args.event_id,
            'service_index': args.service_index,
            'title': title,
            'folder': event_config['folder']
        }
        save_review_queue(queue)
        
        print(f"\n{'='*80}")
        print("✓ Classification complete!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        return


def cmd_skip(args):
    """Skip a video (mark as reviewed but not classified)."""
    queue = load_review_queue()
    
    video_index = None
    for i, v in enumerate(queue['videos']):
        if v['video_id'] == args.video_id:
            video_index = i
            break
    
    if video_index is None:
        print(f"ERROR: Video {args.video_id} not found in review queue")
        return
    
    queue['videos'][video_index]['status'] = 'skipped'
    queue['videos'][video_index]['skip_reason'] = args.reason
    queue['videos'][video_index]['skip_timestamp'] = datetime.now().isoformat()
    
    save_review_queue(queue)
    
    print(f"\n✓ Video {args.video_id} marked as skipped")
    print(f"Reason: {args.reason}\n")


def cmd_stats(args):
    """Show statistics about the review queue."""
    queue = load_review_queue()
    videos = queue.get('videos', [])
    
    pending = len([v for v in videos if v.get('status') == 'pending_review'])
    classified = len([v for v in videos if v.get('status') == 'manually_classified'])
    skipped = len([v for v in videos if v.get('status') == 'skipped'])
    
    print("\n" + "=" * 80)
    print("REVIEW QUEUE STATISTICS")
    print("=" * 80)
    print(f"\nTotal Videos: {len(videos)}")
    print(f"  Pending Review: {pending}")
    print(f"  Manually Classified: {classified}")
    print(f"  Skipped: {skipped}")
    
    if pending > 0:
        print(f"\nTo review pending videos:")
        print(f"  python3 review_videos.py list")
        print(f"  python3 review_videos.py show VIDEO_ID")
    
    print()


def cmd_clean(args):
    """Remove old reviewed videos from queue."""
    queue = load_review_queue()
    config = load_config()
    
    retention_days = args.days
    cutoff = datetime.now(pytz.timezone(config['settings']['timezone'])) - timedelta(days=retention_days)
    
    original_count = len(queue['videos'])
    
    queue['videos'] = [
        v for v in queue['videos']
        if v.get('status') == 'pending_review' or
        datetime.fromisoformat(v.get('skip_timestamp', v.get('manual_classification', {}).get('timestamp', '9999-01-01'))) > cutoff
    ]
    
    removed_count = original_count - len(queue['videos'])
    
    if removed_count > 0:
        save_review_queue(queue)
        print(f"\n✓ Removed {removed_count} old entries from review queue\n")
    else:
        print(f"\nNo old entries to remove\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Manual Review Tool for Vimeo Video Classification",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    list_parser = subparsers.add_parser("list", help="List videos in review queue")
    list_parser.add_argument("--show-reviewed", action="store_true", help="Show recently reviewed videos")
    list_parser.set_defaults(func=cmd_list)
    
    show_parser = subparsers.add_parser("show", help="Show details for a specific video")
    show_parser.add_argument("video_id", help="Video ID to show")
    show_parser.set_defaults(func=cmd_show)
    
    classify_parser = subparsers.add_parser("classify", help="Manually classify a video")
    classify_parser.add_argument("video_id", help="Video ID to classify")
    classify_parser.add_argument("--event", dest="event_id", required=True, help="Event ID")
    classify_parser.add_argument("--service", dest="service_index", type=int, required=True, help="Service index (0, 1, 2, etc.)")
    classify_parser.add_argument("--force", action="store_true", help="Skip confirmation")
    classify_parser.set_defaults(func=cmd_classify)
    
    skip_parser = subparsers.add_parser("skip", help="Skip a video")
    skip_parser.add_argument("video_id", help="Video ID to skip")
    skip_parser.add_argument("--reason", required=True, help="Reason for skipping")
    skip_parser.set_defaults(func=cmd_skip)
    
    stats_parser = subparsers.add_parser("stats", help="Show review queue statistics")
    stats_parser.set_defaults(func=cmd_stats)
    
    clean_parser = subparsers.add_parser("clean", help="Remove old reviewed videos")
    clean_parser.add_argument("--days", type=int, default=30, help="Days to keep (default: 30)")
    clean_parser.set_defaults(func=cmd_clean)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    args.func(args)


if __name__ == "__main__":
    from datetime import timedelta
    main()
