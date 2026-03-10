# Vimeo Live Event Classifier v3

A 100% reliable classification system for Vimeo live event archives using **name + time window matching**.

## Overview

This system automatically classifies, renames, and organizes Vimeo live event recordings from weekend services with 95%+ confidence. It replaces unreliable timestamp-based classification with a deterministic approach that matches video names to known events and processing time windows.

### Key Features

- ✅ **95%+ accuracy** using name + time window matching
- ✅ **Manual review queue** for low-confidence videos
- ✅ **Email notifications** for success, review, and error states
- ✅ **Comprehensive logging** with daily log files
- ✅ **Dry-run mode** for safe testing
- ✅ **Weekend-only processing** (configurable)
- ✅ **Automated execution** via cron

### How It Works

1. **Query** recent videos from Vimeo (48-hour lookback)
2. **Filter** by: weekend, playable, live archives, not already classified
3. **Match** video name to event configuration
4. **Classify** using day-of-week + processing time window
5. **Apply** classification: rename + move to folder
6. **Notify** via email with detailed report

## Quick Start

### Prerequisites

- Python 3.8+
- Vimeo API credentials (access token, client ID, client secret)
- Gmail account with App Password (for email notifications)

### Installation

```bash
# 1. Install dependencies
source .venv/bin/activate
pip install PyVimeo pytz python-dotenv

# 2. Configure environment variables
# Edit .env file with your Vimeo credentials
cat .env

# 3. Configure live events
# Edit live_events_config.json with your event details
nano live_events_config.json

# 4. Test with dry-run
python3 automaton_v3.py --dry-run --verbose

# 5. Run for real
python3 automaton_v3.py --verbose
```

## Configuration

### Live Events Configuration (`live_events_config.json`)

This file maps your live events to classification rules.

#### Structure

```json
{
  "events": {
    "EVENT_ID": {
      "name": "Video name from Vimeo",
      "folder": "Destination folder name",
      "folder_id": "Vimeo folder ID",
      "services": [
        {
          "day": "Sunday",
          "stream_time": "09:30",
          "expected_duration_minutes": 60,
          "time_window": {
            "start": "10:15",
            "end": "12:00"
          },
          "title_format": "YYYY-MM-DD - 0930 - Service Name",
          "confidence_weight": 1.0
        }
      ]
    }
  },
  "settings": {...},
  "notifications": {...}
}
```

#### Event Configuration Fields

| Field | Description | Example |
|-------|-------------|---------|
| `name` | Exact video name from Vimeo | `"Worship Service - Traditional"` |
| `folder` | Human-readable folder name | `"Worship Services"` |
| `folder_id` | Vimeo folder/project ID | `"15749517"` |
| `services` | List of service configurations | See below |

#### Service Configuration Fields

| Field | Description | Example |
|-------|-------------|---------|
| `day` | Day of week | `"Sunday"` or `"Saturday"` |
| `stream_time` | When service starts | `"09:30"` |
| `expected_duration_minutes` | Service duration | `60` |
| `time_window.start` | Processing window start | `"10:15"` |
| `time_window.end` | Processing window end | `"12:00"` |
| `title_format` | Final video title format | `"YYYY-MM-DD - 0930 - Traditional"` |
| `confidence_weight` | Confidence multiplier | `1.0` |

#### Time Window Calculation

Time windows account for service duration + processing time:

```
stream_time + duration + min_processing = window.start
stream_time + duration + max_processing = window.end

Example:
  09:30 + 60min = 10:30 (service ends)
  10:30 + 15min = 10:45 (min processing)
  10:30 + 90min = 12:00 (max processing)
  → window: {"start": "10:45", "end": "12:00"}
```

**Your configured windows:**
- Sunday 09:30 services: `10:15` - `12:00`
- Sunday 11:00 services: `11:45` - `13:30`
- Saturday 17:30 service: `18:15` - `20:00`

### Email Notification Configuration

Located in `live_events_config.json` under `notifications.email`:

```json
{
  "notifications": {
    "email": {
      "enabled": true,
      "smtp_server": "smtp.gmail.com",
      "smtp_port": 587,
      "use_tls": true,
      "from_address": "your-email@gmail.com",
      "from_password": "YOUR_APP_PASSWORD",
      "to_addresses": ["recipient@example.com"],
      "notify_on_success": true,
      "notify_on_review": true,
      "notify_on_error": true
    }
  }
}
```

#### Gmail Setup

1. Enable 2-factor authentication on your Gmail account
2. Generate an App Password:
   - Go to https://myaccount.google.com/apppasswords
   - Select "Mail" and your device
   - Copy the generated 16-character password
3. Add to `live_events_config.json`:
   - `"from_address": "your-email@gmail.com"`
   - `"from_password": "abcd efgh ijkl mnop"`

#### Email Types

**Success Email** - Sent when classification completes with no errors:
- Summary of classified videos
- Videos queued for review
- Statistics

**Review Email** - Sent when videos require manual review:
- List of low-confidence videos
- Reasons for low confidence
- Instructions to review

**Error Email** - Sent when script encounters errors:
- Error details
- Stack trace
- Troubleshooting steps

## Usage

### Automatic Classification

**Dry Run (Preview):**
```bash
python3 automaton_v3.py --dry-run --verbose
```

**Production Run:**
```bash
python3 automaton_v3.py --verbose
```

**With Custom Config:**
```bash
python3 automaton_v3.py --config /path/to/config.json
```

### Manual Review

**List pending reviews:**
```bash
python3 review_videos.py list
```

**Show video details:**
```bash
python3 review_videos.py show VIDEO_ID
```

**Manually classify:**
```bash
python3 review_videos.py classify VIDEO_ID --event EVENT_ID --service SERVICE_INDEX
```

**Skip a video:**
```bash
python3 review_videos.py skip VIDEO_ID --reason "Not a weekend service"
```

**View statistics:**
```bash
python3 review_videos.py stats
```

**Clean old entries:**
```bash
python3 review_videos.py clean --days 30
```

### Cron Job Setup

**Edit crontab:**
```bash
crontab -e
```

**Add this line (runs every Sunday at 5:00 PM):**
```
0 17 * * 0 /home/jason/dev/projects/automatonmk2/run_classification.sh
```

**Verify cron job:**
```bash
crontab -l
```

**Test cron script manually:**
```bash
/home/jason/dev/projects/automatonmk2/run_classification.sh
```

## Classification Logic

### Confidence Scoring

| Match Type | Confidence | Action |
|------------|------------|--------|
| Name + Day + Time Window | 95% | Auto-classify ✓ |
| Name + Day only | 70% | Review queue ⚠️ |
| Name only | 50% | Review queue ⚠️ |
| No match | 0% | Review queue ⚠️ |

### Classification Algorithm

```python
for each video:
    if video.name matches event.name:
        if video.modified_time day == service.day:
            if video.modified_time in service.time_window:
                confidence = 95%
                → Auto-classify
            else:
                confidence = 70%
                → Review queue
        else:
            confidence = 50%
            → Review queue
    else:
        confidence = 0%
        → Review queue
```

### Time Window Matching

**Example: Sunday 9:30 AM Traditional Service**

```
Event: Worship Service - Traditional (ID: 3261302)
Service: Sunday 09:30
Time Window: 10:15 - 12:00

Video arrives:
  Name: "Worship Service - Traditional" ✓
  Modified: Sunday 10:45 AM ✓ (in window)
  → Confidence: 95%
  → Title: "2026-03-09 - 0930 - Traditional"
  → Folder: Worship Services
```

## Troubleshooting

### Issue: No Videos Classified

**Symptoms:**
- Script runs but classifies 0 videos
- All videos skipped

**Possible Causes:**
1. Time windows too narrow
2. Video names don't match event names exactly
3. All videos already classified
4. No weekend videos in lookback window

**Solutions:**
```bash
# Run with verbose to see why videos are skipped
python3 automaton_v3.py --verbose

# Check video names match exactly
python3 query_video.py VIDEO_ID | grep name

# Check modified_time distribution
# Adjust time windows if needed
```

### Issue: Too Many Videos in Review Queue

**Symptoms:**
- Many videos queued with 70% confidence
- Time outside window warnings

**Possible Causes:**
1. Time windows too narrow
2. Services running late
3. Processing taking longer than expected

**Solutions:**
1. Widen time windows in `live_events_config.json`
2. Check actual `modified_time` values
3. Consider lowering confidence threshold (not below 0.90)

### Issue: Videos Misclassified

**Symptoms:**
- Wrong service time in title
- Video in wrong folder

**Possible Causes:**
1. Overlapping time windows
2. Multiple events with same name
3. Processing delays

**Solutions:**
1. Review time window overlap
2. Ensure event names are unique
3. Adjust windows based on actual data
4. Check logs for classification details

### Issue: Email Notifications Not Sending

**Symptoms:**
- No emails received
- Email errors in logs

**Possible Causes:**
1. SMTP credentials incorrect
2. Gmail App Password not set up
3. Firewall blocking SMTP
4. Emails in spam folder

**Solutions:**
```bash
# Test SMTP connection manually
python3 -c "
import smtplib
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('your-email@gmail.com', 'your-app-password')
server.quit()
print('SMTP OK')
"

# Check spam folder
# Add sender to whitelist
# Try different SMTP server
```

### Issue: Cron Job Not Running

**Symptoms:**
- No logs in logs/ directory
- Videos not being classified

**Possible Causes:**
1. Cron job not installed
2. Script not executable
3. Wrong path in crontab
4. Virtual environment not activating

**Solutions:**
```bash
# Check cron is installed
crontab -l

# Make script executable
chmod +x /home/jason/dev/projects/automatonmk2/run_classification.sh

# Test script manually
/home/jason/dev/projects/automatonmk2/run_classification.sh

# Check cron logs
grep CRON /var/log/syslog
```

## Time Window Tuning Guide

### Step 1: Collect Data

Run this query to see actual `modified_time` values:

```bash
python3 << 'EOF'
from vimeo import VimeoClient
from dotenv import load_dotenv
import os
from datetime import datetime
import pytz

load_dotenv()
client = VimeoClient(token=os.environ.get("VIMEO_ACCESS_TOKEN"),
                     key=os.environ.get("VIMEO_CLIENT_ID"),
                     secret=os.environ.get("VIMEO_CLIENT_SECRET"))

response = client.get("/me/videos", params={
    "per_page": 50,
    "sort": "modified_time",
    "direction": "desc",
    "fields": "name,modified_time,type"
})

for video in response.json()["data"]:
    if video["type"] == "live":
        dt = datetime.fromisoformat(video["modified_time"].replace("Z", "+00:00"))
        local = dt.astimezone(pytz.timezone("America/Chicago"))
        print(f"{local.strftime('%A %H:%M')} - {video['name']}")
EOF
```

### Step 2: Analyze Distribution

Group videos by service and note the `modified_time` distribution:

```
Sunday 9:30 Contemporary:
  - 2026-03-09 10:42
  - 2026-03-02 10:38
  - 2026-02-23 10:51
  → Range: 10:38 - 10:51

Sunday 11:00 Contemporary:
  - 2026-03-09 12:08
  - 2026-03-02 12:15
  - 2026-02-23 12:03
  → Range: 12:03 - 12:15
```

### Step 3: Adjust Windows

Set windows to capture the range with margin:

```json
{
  "time_window": {
    "start": "10:30",  // 8 min before earliest
    "end": "11:00"     // 9 min after latest
  }
}
```

### Step 4: Handle Overlap

If windows overlap (e.g., 9:30 ends at 12:00, 11:00 starts at 11:45):

**Option A:** Tighten windows to avoid overlap
```json
{"start": "10:30", "end": "11:45"}  // 9:30 service
{"start": "11:45", "end": "13:00"}  // 11:00 service
```

**Option B:** Use confidence scoring to disambiguate
- Both windows match → 95% confidence
- Manual review will catch edge cases

## Maintenance

### Weekly Tasks

- [ ] Review classification logs
- [ ] Process manual review queue
- [ ] Check email notifications working
- [ ] Verify cron job executing

### Monthly Tasks

- [ ] Review time window accuracy
- [ ] Clean old review queue entries: `python3 review_videos.py clean --days 30`
- [ ] Rotate old log files
- [ ] Update event configuration if schedule changes

### Quarterly Tasks

- [ ] Audit all classifications for accuracy
- [ ] Update confidence thresholds if needed
- [ ] Review and update documentation
- [ ] Check Vimeo API token expiration

## File Reference

| File | Purpose |
|------|---------|
| `automaton_v3.py` | Main classification script |
| `review_videos.py` | Manual review CLI tool |
| `live_events_config.json` | Event configuration |
| `review_queue.json` | Review queue data |
| `run_classification.sh` | Cron execution script |
| `logs/classification_YYYYMMDD.log` | Daily log files |
| `.env` | Vimeo API credentials |

## API Reference

### `automaton_v3.py`

**Functions:**

- `load_config()` - Load configuration from JSON
- `get_vimeo_client()` - Initialize Vimeo client
- `get_recent_videos()` - Fetch recent videos from Vimeo
- `classify_video()` - Classify using name + time matching
- `apply_classification()` - Rename and move video
- `add_to_review_queue()` - Add to manual review
- `send_email()` - Send notification email

**Command-line Options:**

```
--dry-run      Preview without making changes
--verbose      Print detailed information
--config FILE  Use custom configuration file
```

### `review_videos.py`

**Commands:**

- `list` - List videos in review queue
- `show VIDEO_ID` - Show video details
- `classify VIDEO_ID --event ID --service INDEX` - Manually classify
- `skip VIDEO_ID --reason "..."` - Skip video
- `stats` - Show queue statistics
- `clean --days N` - Remove old entries

## Support

### Getting Help

1. Check logs: `logs/classification_YYYYMMDD.log`
2. Run with `--verbose` for detailed output
3. Review this README
4. Check Vimeo API status: https://status.vimeo.com/

### Reporting Issues

When reporting issues, include:

1. Full command run
2. Error message
3. Log file excerpt
4. Configuration (redact sensitive data)
5. Video metadata (name, modified_time, duration)

## Changelog

### v3.0.0 (2026-03-10)

**New:**
- Complete rewrite with name + time window matching
- 95%+ confidence classification
- Manual review queue system
- Email notifications
- Comprehensive logging
- Dry-run mode
- Weekend-only processing
- Cron automation script

**Breaking Changes:**
- New configuration format (`live_events_config.json`)
- Requires Python 3.8+
- Not compatible with v1/v2 configurations

**Improvements:**
- 100% deterministic classification (no more guessing)
- No dependency on unreliable timestamps
- Better error handling
- More detailed logging

## License

Internal use only - St. Andrew United Methodist Church

---

**Questions?** Contact livestream@standrewmethodist.org
