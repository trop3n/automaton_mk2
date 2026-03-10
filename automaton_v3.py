#!/usr/bin/env python3
"""
Vimeo Live Event Classifier v3

Automatically classifies, renames, and organizes Vimeo live event archives
based on event name and processing time window matching.

Features:
- Name + time window matching (95%+ confidence)
- Manual review queue for low-confidence videos
- Email notifications (success, review, error)
- Comprehensive logging
- Dry-run mode
- Weekend-only processing

Usage:
    python3 automaton_v3.py [--dry-run] [--verbose] [--config CONFIG_FILE]
    
Examples:
    python3 automaton_v3.py                    # Normal run
    python3 automaton_v3.py --dry-run          # Preview without changes
    python3 automaton_v3.py --verbose          # Detailed output
"""

import os
import sys
import json
import smtplib
import argparse
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient


load_dotenv()


VIMEO_ACCESS_TOKEN = os.environ.get("VIMEO_ACCESS_TOKEN")
VIMEO_CLIENT_ID = os.environ.get("VIMEO_CLIENT_ID")
VIMEO_CLIENT_SECRET = os.environ.get("VIMEO_CLIENT_SECRET")

CONFIG_FILE = Path(__file__).parent / "live_events_config.json"
REVIEW_QUEUE_FILE = Path(__file__).parent / "review_queue.json"
LOGS_DIR = Path(__file__).parent / "logs"


def load_config(config_path: Path = CONFIG_FILE) -> Dict:
    """Load configuration from JSON file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config


def get_vimeo_client() -> VimeoClient:
    """Initialize and return Vimeo client."""
    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        raise ValueError("Vimeo credentials not configured in .env file")
    
    return VimeoClient(
        token=VIMEO_ACCESS_TOKEN,
        key=VIMEO_CLIENT_ID,
        secret=VIMEO_CLIENT_SECRET
    )


def parse_time(time_str: str) -> int:
    """
    Parse time string to minutes since midnight.
    
    Args:
        time_str: Time in "HH:MM" format
        
    Returns:
        Minutes since midnight
    """
    hours, minutes = map(int, time_str.split(':'))
    return hours * 60 + minutes


def parse_timestamp(timestamp_str: str, timezone: str = "America/Chicago") -> datetime:
    """
    Parse ISO timestamp and convert to local timezone.
    
    Args:
        timestamp_str: ISO format timestamp
        timezone: Target timezone
        
    Returns:
        Timezone-aware datetime object
    """
    local_tz = pytz.timezone(timezone)
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    return dt.astimezone(local_tz)


def generate_title(title_format: str, dt: datetime) -> str:
    """
    Generate video title from format string and datetime.
    
    Args:
        title_format: Format string like "YYYY-MM-DD - 0930 - Contemporary"
        dt: Datetime object
        
    Returns:
        Formatted title string
    """
    title = title_format.replace("YYYY", str(dt.year))
    title = title.replace("MM", f"{dt.month:02d}")
    title = title.replace("DD", f"{dt.day:02d}")
    return title


def is_weekend(dt: datetime) -> bool:
    """Check if datetime falls on a weekend (Saturday or Sunday)."""
    return dt.weekday() in [5, 6]  # Saturday=5, Sunday=6


def get_recent_videos(client: VimeoClient, lookback_hours: int, verbose: bool = False) -> List[Dict]:
    """
    Fetch videos modified within the lookback window.
    
    Args:
        client: Vimeo client instance
        lookback_hours: Number of hours to look back
        verbose: Print detailed information
        
    Returns:
        List of video data dictionaries
    """
    if verbose:
        print(f"Fetching videos modified in the last {lookback_hours} hours...")
    
    now_utc = datetime.now(pytz.utc)
    start_time_utc = now_utc - timedelta(hours=lookback_hours)
    
    all_videos = []
    
    try:
        response = client.get(
            "/me/videos",
            params={
                "per_page": 100,
                "sort": "modified_time",
                "direction": "desc",
                "fields": "uri,name,type,created_time,modified_time,description,duration,is_playable,parent_folder"
            }
        )
        response.raise_for_status()
        
        videos = response.json().get("data", [])
        
        for video in videos:
            modified_time_str = video.get("modified_time")
            if not modified_time_str:
                continue
            
            modified_time_utc = parse_timestamp(modified_time_str, "UTC")
            if modified_time_utc >= start_time_utc:
                all_videos.append(video)
            else:
                break
        
        if verbose:
            print(f"Found {len(all_videos)} recently modified videos")
    
    except Exception as e:
        print(f"ERROR fetching videos: {e}")
        raise
    
    return all_videos


def classify_video(video_data: Dict, config: Dict) -> Dict:
    """
    Classify video using name + time window matching.
    
    Args:
        video_data: Video metadata from Vimeo API
        config: Configuration dictionary
        
    Returns:
        Classification result with confidence score
    """
    result = {
        'classification': None,
        'confidence': 0.0,
        'method': None,
        'warnings': []
    }
    
    video_name = video_data.get('name', '')
    modified_time = parse_timestamp(video_data['modified_time'], config['settings']['timezone'])
    day_of_week = modified_time.strftime('%A')
    time_minutes = modified_time.hour * 60 + modified_time.minute
    
    for event_id, event_config in config['events'].items():
        if video_name == event_config['name']:
            for service in event_config['services']:
                if service['day'] == day_of_week:
                    window_start = parse_time(service['time_window']['start'])
                    window_end = parse_time(service['time_window']['end'])
                    
                    if window_start <= time_minutes <= window_end:
                        result['classification'] = {
                            'event_id': event_id,
                            'event_name': event_config['name'],
                            'service': service,
                            'folder': event_config['folder'],
                            'folder_id': event_config['folder_id'],
                            'title': generate_title(service['title_format'], modified_time)
                        }
                        result['confidence'] = 0.95 * service.get('confidence_weight', 1.0)
                        result['method'] = 'name_and_time_matching'
                        return result
            
            result['warnings'].append(
                f"Event matched but time {modified_time.strftime('%H:%M')} "
                f"doesn't fit any window for {day_of_week}"
            )
            result['confidence'] = 0.70
            result['method'] = 'name_match_only'
    
    if result['confidence'] == 0.0:
        result['warnings'].append('No classification rule matched')
    
    return result


def should_process_video(video_data: Dict, config: Dict, verbose: bool = False) -> Tuple[bool, str]:
    """
    Determine if video should be processed.
    
    Args:
        video_data: Video metadata
        config: Configuration dictionary
        verbose: Print detailed information
        
    Returns:
        Tuple of (should_process, reason)
    """
    video_name = video_data.get('name', 'Unknown')
    
    if not video_data.get('is_playable'):
        return False, "Not playable (phantom live event)"
    
    if video_data.get('type') != 'live':
        return False, "Not a live event archive"
    
    parent_folder = video_data.get('parent_folder')
    if parent_folder:
        parent_folder_id = parent_folder['uri'].split('/')[-1]
        
        if parent_folder_id in config['settings']['excluded_folders']:
            return False, f"In excluded folder '{parent_folder.get('name')}'"
        
        if parent_folder_id in config['settings']['destination_folders'].values():
            return False, f"Already in destination folder '{parent_folder.get('name')}'"
    
    if config['settings'].get('weekend_only'):
        modified_time = parse_timestamp(video_data['modified_time'], config['settings']['timezone'])
        if not is_weekend(modified_time):
            return False, "Not a weekend video"
    
    return True, "Valid for processing"


def apply_classification(
    client: VimeoClient,
    video_data: Dict,
    classification: Dict,
    dry_run: bool = False,
    verbose: bool = False
) -> Dict:
    """
    Apply classification to video (rename and move).
    
    Args:
        client: Vimeo client instance
        video_data: Video metadata
        classification: Classification result
        dry_run: Preview only, don't make changes
        verbose: Print detailed information
        
    Returns:
        Statistics dictionary with success/failure info
    """
    stats = {
        'title_updated': False,
        'moved': False,
        'errors': []
    }
    
    video_id = video_data['uri'].split('/')[-1]
    current_title = video_data.get('name', '')
    new_title = classification['title']
    
    if current_title != new_title:
        if verbose:
            print(f"    Renaming: '{current_title}' → '{new_title}'")
        
        if not dry_run:
            try:
                response = client.patch(f"/videos/{video_id}", data={"name": new_title})
                if response.status_code in [200, 204]:
                    stats['title_updated'] = True
                    if verbose:
                        print(f"    ✓ Title updated successfully")
                else:
                    error_msg = f"Failed to rename: {response.status_code}"
                    stats['errors'].append(error_msg)
                    if verbose:
                        print(f"    ✗ {error_msg}")
            except Exception as e:
                error_msg = f"Error renaming: {e}"
                stats['errors'].append(error_msg)
                if verbose:
                    print(f"    ✗ {error_msg}")
        else:
            stats['title_updated'] = True
            if verbose:
                print(f"    [DRY RUN] Would rename")
    
    folder_id = classification['folder_id']
    if folder_id:
        if verbose:
            print(f"    Moving to folder: {classification['folder']} (ID: {folder_id})")
        
        if not dry_run:
            try:
                user_response = client.get("/me")
                user_uri = user_response.json()["uri"]
                project_uri = f"{user_uri}/projects/{folder_id}"
                
                move_response = client.put(f"{project_uri}/videos/{video_id}")
                if move_response.status_code == 204:
                    stats['moved'] = True
                    if verbose:
                        print(f"    ✓ Video moved successfully")
                else:
                    error_msg = f"Failed to move: {move_response.status_code}"
                    stats['errors'].append(error_msg)
                    if verbose:
                        print(f"    ✗ {error_msg}")
            except Exception as e:
                error_msg = f"Error moving: {e}"
                stats['errors'].append(error_msg)
                if verbose:
                    print(f"    ✗ {error_msg}")
        else:
            stats['moved'] = True
            if verbose:
                print(f"    [DRY RUN] Would move")
    
    return stats


def load_review_queue() -> Dict:
    """Load review queue from JSON file."""
    if REVIEW_QUEUE_FILE.exists():
        with open(REVIEW_QUEUE_FILE, 'r') as f:
            return json.load(f)
    return {"videos": []}


def save_review_queue(queue: Dict):
    """Save review queue to JSON file."""
    with open(REVIEW_QUEUE_FILE, 'w') as f:
        json.dump(queue, f, indent=2)


def add_to_review_queue(
    video_data: Dict,
    classification_result: Dict,
    config: Dict,
    verbose: bool = False
):
    """
    Add video to manual review queue.
    
    Args:
        video_data: Video metadata
        classification_result: Classification result
        config: Configuration dictionary
        verbose: Print detailed information
    """
    queue = load_review_queue()
    
    video_id = video_data['uri'].split('/')[-1]
    
    for existing in queue['videos']:
        if existing['video_id'] == video_id:
            if verbose:
                print(f"    Video {video_id} already in review queue")
            return
    
    entry = {
        'video_id': video_id,
        'video_name': video_data.get('name', 'Unknown'),
        'modified_time': video_data.get('modified_time'),
        'duration': video_data.get('duration', 0),
        'uri': video_data.get('uri'),
        'classification_attempts': [
            {
                'timestamp': datetime.now(pytz.timezone(config['settings']['timezone'])).isoformat(),
                'confidence': classification_result['confidence'],
                'method': classification_result['method'],
                'suggested_title': classification_result['classification']['title'] if classification_result['classification'] else None,
                'warnings': classification_result['warnings']
            }
        ],
        'status': 'pending_review',
        'added_to_queue': datetime.now(pytz.timezone(config['settings']['timezone'])).isoformat()
    }
    
    queue['videos'].append(entry)
    save_review_queue(queue)
    
    if verbose:
        print(f"    Added to review queue (confidence: {classification_result['confidence']:.0%})")


def send_email(
    subject: str,
    body: str,
    notification_type: str,
    config: Dict,
    verbose: bool = False
):
    """
    Send email notification.
    
    Args:
        subject: Email subject
        body: Email body
        notification_type: 'success', 'review', or 'error'
        config: Configuration dictionary
        verbose: Print detailed information
    """
    email_config = config['notifications']['email']
    
    if not email_config.get('enabled', False):
        if verbose:
            print("Email notifications disabled")
        return
    
    notification_key = f'notify_on_{notification_type}'
    if not email_config.get(notification_key, True):
        if verbose:
            print(f"Email notification for '{notification_type}' disabled")
        return
    
    try:
        msg = MIMEMultipart()
        msg['From'] = email_config['from_address']
        msg['To'] = ', '.join(email_config['to_addresses'])
        msg['Subject'] = subject
        
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            if email_config.get('use_tls', True):
                server.starttls()
            
            server.login(
                email_config['from_address'],
                email_config['from_password']
            )
            
            server.send_message(msg)
        
        if verbose:
            print(f"Email notification sent: {subject}")
    
    except Exception as e:
        print(f"ERROR sending email: {e}")


def generate_summary_email(
    stats: Dict,
    config: Dict,
    dry_run: bool
) -> Tuple[str, str]:
    """Generate summary email content."""
    local_tz = pytz.timezone(config['settings']['timezone'])
    now = datetime.now(local_tz)
    
    subject = f"[Vimeo Classifier] Classification Report - {now.strftime('%A %Y-%m-%d')}"
    if dry_run:
        subject = f"[DRY RUN] {subject}"
    
    body_lines = [
        "Vimeo Video Classification Report",
        "=" * 50,
        "",
        f"Run Time: {now.strftime('%A, %B %d, %Y at %I:%M %p')}",
        f"Mode: {'DRY RUN (no changes made)' if dry_run else 'PRODUCTION'}",
        "",
        "SUMMARY",
        "-" * 50,
        f"Videos Scanned:        {stats['scanned']}",
        f"Videos Processed:      {stats['processed']}",
        f"Videos Classified:     {stats['classified']}",
        f"Videos to Review:      {stats['review']}",
        f"Videos Skipped:        {stats['skipped']}",
        f"Errors:                {stats['errors']}",
        ""
    ]
    
    if stats['classified_videos']:
        body_lines.extend([
            "CLASSIFIED VIDEOS",
            "-" * 50
        ])
        for i, video in enumerate(stats['classified_videos'], 1):
            body_lines.extend([
                f"{i}. {video['new_title']}",
                f"   Video ID: {video['video_id']}",
                f"   Original: {video['original_title']}",
                f"   Folder: {video['folder']}",
                f"   Confidence: {video['confidence']:.0%}",
                ""
            ])
    
    if stats['review_videos']:
        body_lines.extend([
            "VIDEOS REQUIRING REVIEW",
            "-" * 50
        ])
        for i, video in enumerate(stats['review_videos'], 1):
            body_lines.extend([
                f"{i}. {video['video_name']} (ID: {video['video_id']})",
                f"   Confidence: {video['confidence']:.0%}",
                f"   Reason: {video['reason']}",
                ""
            ])
    
    body_lines.extend([
        "",
        "To review queued videos:",
        "  python3 review_videos.py list",
        "",
        "To run manually:",
        "  python3 automaton_v3.py --verbose",
        "",
        "-" * 50,
        "Automated by automaton_v3.py"
    ])
    
    return subject, "\n".join(body_lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Vimeo Live Event Classifier v3",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview classifications without making changes'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed information'
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=CONFIG_FILE,
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("VIMEO LIVE EVENT CLASSIFIER v3")
    print("=" * 60)
    if args.dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    print()
    
    try:
        config = load_config(args.config)
        
        if args.verbose:
            print(f"Configuration loaded from: {args.config}")
            print(f"Timezone: {config['settings']['timezone']}")
            print(f"Lookback: {config['settings']['lookback_hours']} hours")
            print()
        
        client = get_vimeo_client()
        
        user_response = client.get("/me")
        if user_response.status_code != 200:
            raise Exception(f"Failed to connect to Vimeo: {user_response.status_code}")
        
        user_name = user_response.json().get('name', 'Unknown')
        if args.verbose:
            print(f"Connected to Vimeo as: {user_name}")
            print()
        
        videos = get_recent_videos(
            client,
            config['settings']['lookback_hours'],
            args.verbose
        )
        
        stats = {
            'scanned': len(videos),
            'processed': 0,
            'classified': 0,
            'review': 0,
            'skipped': 0,
            'errors': 0,
            'classified_videos': [],
            'review_videos': []
        }
        
        for video in videos:
            video_name = video.get('name', 'Unknown')
            video_id = video['uri'].split('/')[-1]
            
            if args.verbose:
                print(f"\n{'-' * 60}")
                print(f"Video: {video_name} (ID: {video_id})")
            
            should_process, reason = should_process_video(video, config, args.verbose)
            
            if not should_process:
                if args.verbose:
                    print(f"  ⊘ Skipping: {reason}")
                stats['skipped'] += 1
                continue
            
            stats['processed'] += 1
            
            if args.verbose:
                print(f"  → Processing...")
            
            classification_result = classify_video(video, config)
            
            if classification_result['confidence'] >= config['settings']['min_confidence']:
                if args.verbose:
                    print(f"  ✓ Classified (confidence: {classification_result['confidence']:.0%})")
                    print(f"    Method: {classification_result['method']}")
                    if classification_result['warnings']:
                        for warning in classification_result['warnings']:
                            print(f"    Warning: {warning}")
                
                apply_stats = apply_classification(
                    client,
                    video,
                    classification_result['classification'],
                    args.dry_run,
                    args.verbose
                )
                
                if apply_stats['errors']:
                    stats['errors'] += len(apply_stats['errors'])
                else:
                    stats['classified'] += 1
                    stats['classified_videos'].append({
                        'video_id': video_id,
                        'original_title': video_name,
                        'new_title': classification_result['classification']['title'],
                        'folder': classification_result['classification']['folder'],
                        'confidence': classification_result['confidence']
                    })
            
            else:
                if args.verbose:
                    print(f"  ⚠ Low confidence ({classification_result['confidence']:.0%}) - adding to review queue")
                    if classification_result['warnings']:
                        for warning in classification_result['warnings']:
                            print(f"    Warning: {warning}")
                
                if not args.dry_run:
                    add_to_review_queue(video, classification_result, config, args.verbose)
                
                stats['review'] += 1
                stats['review_videos'].append({
                    'video_id': video_id,
                    'video_name': video_name,
                    'confidence': classification_result['confidence'],
                    'reason': classification_result['warnings'][0] if classification_result['warnings'] else 'Unknown'
                })
        
        print("\n" + "=" * 60)
        print("CLASSIFICATION SUMMARY")
        print("=" * 60)
        print(f"Videos Scanned:    {stats['scanned']}")
        print(f"Videos Processed:  {stats['processed']}")
        print(f"  ✓ Classified:    {stats['classified']}")
        print(f"  ⚠ Review Queue:  {stats['review']}")
        print(f"  ⊘ Skipped:       {stats['skipped']}")
        print(f"  ✗ Errors:        {stats['errors']}")
        print("=" * 60)
        
        subject, body = generate_summary_email(stats, config, args.dry_run)
        
        if not args.dry_run:
            notification_type = 'success' if stats['errors'] == 0 else 'error'
            if stats['review'] > 0:
                notification_type = 'review'
            
            send_email(subject, body, notification_type, config, args.verbose)
        else:
            print("\n[DRY RUN] Email would be sent:")
            print(f"Subject: {subject}")
            print()
        
    except Exception as e:
        error_msg = f"ERROR: {e}"
        print(f"\n{error_msg}")
        
        if not args.dry_run and 'config' in locals():
            try:
                send_email(
                    "[Vimeo Classifier] ERROR - Classification Failed",
                    f"An error occurred during classification:\n\n{error_msg}",
                    'error',
                    config,
                    args.verbose
                )
            except:
                pass
        
        sys.exit(1)


if __name__ == "__main__":
    main()
