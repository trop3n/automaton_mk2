# Quick Setup Guide - Vimeo Live Event Classifier v3

## First-Time Setup (5 minutes)

### Step 1: Activate Virtual Environment

```bash
cd /home/jason/dev/projects/automatonmk2
source .venv/bin/activate
```

You should see `(.venv)` in your prompt.

### Step 2: Verify Dependencies

```bash
# Check if PyVimeo is installed
python3 -c "import vimeo; print('✓ PyVimeo installed')"

# Check if other dependencies are installed
python3 -c "import pytz; print('✓ pytz installed')"
python3 -c "import dotenv; print('✓ python-dotenv installed')"
```

If any are missing, install them:
```bash
pip install PyVimeo pytz python-dotenv
```

### Step 3: Configure Email Notifications

Edit `live_events_config.json`:

```bash
nano live_events_config.json
```

Find the `notifications` section and update:
```json
"from_password": "YOUR_GMAIL_APP_PASSWORD_HERE"
```

**To get Gmail App Password:**
1. Go to https://myaccount.google.com/apppasswords
2. Sign in with livestream@standrewmethodist.org
3. Generate new app password (name it "Vimeo Classifier")
4. Copy the 16-character password
5. Paste in config file (no spaces)

### Step 4: Test Configuration

```bash
# Test that configuration loads correctly
python3 -c "
from automaton_v3 import load_config
config = load_config()
print(f'✓ Loaded {len(config[\"events\"])} events')
print(f'✓ Settings: timezone={config[\"settings\"][\"timezone\"]}, lookback={config[\"settings\"][\"lookback_hours\"]}h')
print(f'✓ Email notifications: {\"enabled\" if config[\"notifications\"][\"email\"][\"enabled\"] else \"disabled\"}')
"
```

Expected output:
```
✓ Loaded 4 events
✓ Settings: timezone=America/Chicago, lookback=48h
✓ Email notifications: enabled
```

### Step 5: Test Dry Run

```bash
# Preview what would happen (no changes made)
python3 automaton_v3.py --dry-run --verbose
```

This will:
- Query recent videos from Vimeo
- Show which videos would be classified
- Show which would go to review queue
- NOT make any actual changes

### Step 6: Test Manual Review Tool

```bash
# Check if any videos in queue
python3 review_videos.py list

# Show statistics
python3 review_videos.py stats
```

### Step 7: First Production Run

```bash
# Run for real (will classify videos and send email)
python3 automaton_v3.py --verbose
```

Check your email for the classification report!

### Step 8: Set Up Cron Job (Optional)

```bash
# Edit crontab
crontab -e

# Add this line (runs every Sunday at 5:00 PM)
0 17 * * 0 /home/jason/dev/projects/automatonmk2/run_classification.sh

# Save and exit

# Verify cron job is installed
crontab -l
```

## Quick Reference

### Run Classifier

```bash
# Normal run (production)
python3 automaton_v3.py

# Preview mode (no changes)
python3 automaton_v3.py --dry-run

# Detailed output
python3 automaton_v3.py --verbose

# Preview with details
python3 automaton_v3.py --dry-run --verbose
```

### Manual Review

```bash
# List videos needing review
python3 review_videos.py list

# Show video details
python3 review_videos.py show VIDEO_ID

# Manually classify
python3 review_videos.py classify VIDEO_ID --event EVENT_ID --service 0

# Skip a video
python3 review_videos.py skip VIDEO_ID --reason "Not a weekend service"

# Show statistics
python3 review_videos.py stats
```

### Check Logs

```bash
# View today's log
cat logs/classification_$(date +%Y%m%d).log

# View all logs
ls -lh logs/

# Search logs for errors
grep -i error logs/*.log
```

### Troubleshooting

**No videos classified:**
```bash
# Check if videos exist in lookback window
python3 automaton_v3.py --dry-run --verbose

# Check video names match event names
# Check time windows are correct
# Check videos are playable live archives
```

**Email not sending:**
```bash
# Verify Gmail App Password
# Check SMTP settings in config
# Test manually:
python3 -c "
from automaton_v3 import load_config, send_email
config = load_config()
send_email('Test Subject', 'Test body', 'success', config, verbose=True)
"
```

**Import errors:**
```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Reinstall dependencies
pip install --upgrade PyVimeo pytz python-dotenv
```

## Next Steps

1. ✅ Set up email notifications
2. ✅ Test with dry-run
3. ✅ Run first production classification
4. ✅ Set up cron job for automation
5. ⬜ Monitor for 2-4 weeks
6. ⬜ Tune time windows if needed
7. ⬜ Document any special procedures

## Event IDs Reference

- **4590739**: Worship Service - Contemporary (0930 & 1100)
- **3261302**: Worship Service - Traditional (Saturday 1730, Sunday 0930 & 1100)
- **3868173**: Class - The Root Class (Sunday 0930)
- **3378887**: Class - Something Else Class (Sunday 1100)

## Support

- Logs: `logs/classification_YYYYMMDD.log`
- Config: `live_events_config.json`
- Review Queue: `review_queue.json`
- Full Docs: `README_v3.md`

---

**Questions?** Check `README_v3.md` or contact livestream@standrewmethodist.org
