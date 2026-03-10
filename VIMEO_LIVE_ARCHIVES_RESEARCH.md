# Vimeo Live Event Archives - Research Summary

**Date**: 2026-03-09  
**Purpose**: Understanding how Vimeo handles live event archives and their metadata

## Executive Summary

Vimeo live events that have been archived retain their status as `type: "live"` videos, but their timestamp metadata reflects the event creation time, NOT the actual streaming time. This creates classification challenges when relying on timestamps alone.

## Key Findings

### 1. Live Event Archive Metadata Structure

Based on actual API responses from Vimeo:

```json
{
  "type": "live",
  "created_time": "2025-11-16T16:34:12+00:00",
  "release_time": "2025-11-16T16:34:12+00:00",
  "modified_time": "2026-01-31T00:39:12+00:00",
  "embed": {
    "badges": {
      "live": {
        "streaming": false,
        "archived": true
      }
    }
  }
}
```

**Critical Discovery**: 
- `created_time` = When the live event was **created/scheduled**, not when it was streamed
- `release_time` = Same as created_time
- `modified_time` = When metadata was last changed (rename, move, etc.)
- `embed.badges.live.archived` = Indicates this is an archived live stream
- `embed.badges.live.streaming` = false for archived events

### 2. No Dedicated "Live" Object with Schedule Info

Contrary to expectations, the Vimeo API does **NOT** return a dedicated `live` object with fields like:
- `live.scheduled_start_time`
- `live.archived_time`
- `live.actual_start_time`
- `live.actual_end_time`

**API Query Results**: When requesting the `live` field specifically, it does not appear in the response, suggesting:
1. This field may require specific API permissions or Enterprise plans
2. It may only be available on the live event endpoint (`/live_events/{id}`), not the video endpoint
3. It may have been deprecated or not yet implemented in the current API version

### 3. Live Events vs. Regular Videos

**Live Events** (`type: "live"`):
- Created when event is scheduled (days/weeks before actual stream)
- `created_time` reflects scheduling time, not streaming time
- `release_time` = `created_time` 
- Becomes playable after streaming completes
- Marked as `archived: true` in embed badges

**Regular Videos** (`type: "video"`):
- `created_time` = actual upload time
- `release_time` = when video was made available
- No live streaming metadata

### 4. The Classification Problem

**Example Timeline**:
```
March 1, 2026 10:45 AM: Live event created for March 8, 2026 9:30 AM service
  → created_time = 2026-03-01T16:45:08+00:00 (March 1)
  → release_time = 2026-03-01T16:45:08+00:00 (March 1)

March 8, 2026 9:30 AM: Live stream actually happens
  → No timestamp recorded for this event!

March 9, 2026: Video is renamed/modified
  → modified_time = 2026-03-09T14:39:33+00:00
```

**Problem**: If classification logic uses `created_time` or `release_time`, it will classify based on March 1 (event creation), not March 8 (actual service date).

## Recommended Classification Fields

### Primary Fields (Most Reliable)

1. **Video Title Pattern Matching**
   - Extract date/time from title if present
   - Format: `YYYY-MM-DD - HHMM - Event Type`
   - Most reliable if consistently formatted

2. **Custom Metadata in Description**
   - Embed structured metadata in event description
   - Parse `AUTOMATON_METADATA` JSON block
   - Contains: scheduled_date, scheduled_time, event_type, folder_destination

3. **Schedule Tracker JSON File**
   - Maintain local record of scheduled events
   - Link video IDs to scheduled events
   - Use `automaton_scheduler.py` to manage this

### Secondary Fields (Less Reliable)

4. **modified_time** (better than created_time)
   - Reflects when video was last touched
   - Usually updated shortly after stream completes
   - Still subject to timing issues

5. **Day of Week + Time Window Logic**
   - Use day of week from any timestamp
   - Apply time window rules (e.g., "if Sunday and modified_time is morning, classify as Sunday service")
   - Works for predictable schedules, fails for special events

### Fields to AVOID

❌ **created_time** - Represents event creation, not stream time  
❌ **release_time** - Same as created_time  
❌ **Type field** - Only indicates "live" vs "video", not useful for classification

## API Limitations & Gotchas

### 1. Missing Schedule Information
The Vimeo API does not expose:
- Original scheduled start time
- Actual stream start/end times
- Duration of live stream vs. archive
- Link between live event and its archive

### 2. Phantom Live Events
Some live events appear in API results but are not playable:
- Check `is_playable` field
- Skip videos where `is_playable == false`
- These are events that were scheduled but never streamed

### 3. Timezone Complexity
- All timestamps are in UTC
- Must convert to local timezone for classification
- Daylight saving time transitions can cause misclassification

### 4. No Parent/Child Relationship
- Archived videos do not link back to original live event
- Cannot query `/live_events/{id}` to get archive video ID
- Must rely on title matching or embedded metadata

### 5. Modified Time Updates
The `modified_time` changes when:
- Video is renamed
- Video is moved to folder
- Description is updated
- Any metadata changes

This makes it unreliable for determining stream time.

## Best Practices for Reliable Classification

### Strategy 1: Pre-Scheduling with Metadata (Recommended)

Use `automaton_scheduler.py` to:
1. Create live events with structured titles
2. Embed classification metadata in description
3. Track in local JSON file
4. Match archived videos by parsing description or title

**Advantages**:
- Most reliable approach
- Works even if timestamps are wrong
- Provides audit trail

**Disadvantages**:
- Requires pre-scheduling all events
- Manual process for ad-hoc events

### Strategy 2: Title-Based Classification

Enforce strict title format: `YYYY-MM-DD - HHMM - Event Type`

```python
def extract_date_from_title(title):
    """Extract date/time from standardized title."""
    import re
    match = re.match(r'(\d{4}-\d{2}-\d{2}) - (\d{4}) - (.+)', title)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        event_type = match.group(3)
        return {
            'date': date_str,
            'time': f"{time_str[:2]}:{time_str[2:]}",
            'type': event_type
        }
    return None
```

**Advantages**:
- Simple to implement
- No external dependencies

**Disadvantages**:
- Relies on consistent title formatting
- Breaks if title is changed

### Strategy 3: Hybrid Approach (Current Implementation)

Combine multiple signals:
1. Try parsing title first
2. Fall back to description metadata
3. Use modified_time + day of week + time windows as last resort
4. Flag videos that can't be reliably classified for manual review

## Suggested Implementation Improvements

### 1. Enhanced Video Query

When fetching videos, include these fields:
```python
fields = [
    'uri', 'name', 'description', 'type',
    'created_time', 'modified_time', 'release_time',
    'duration', 'is_playable',
    'parent_folder'
]
```

### 2. Live Event Detection

```python
def is_live_archive(video_data):
    """Check if video is an archived live event."""
    if video_data.get('type') != 'live':
        return False
    
    embed = video_data.get('embed', {})
    badges = embed.get('badges', {})
    live_badge = badges.get('live', {})
    
    return (
        live_badge.get('archived') == True and
        live_badge.get('streaming') == False and
        video_data.get('is_playable') == True
    )
```

### 3. Reliable Timestamp Selection

```python
def get_best_timestamp(video_data, local_tz):
    """
    Get most reliable timestamp for classification.
    
    Priority:
    1. Extracted from title (if standardized format)
    2. Extracted from description metadata
    3. modified_time (better than created_time for live archives)
    4. created_time (last resort)
    """
    # Try title extraction first
    title_date = extract_date_from_title(video_data.get('name', ''))
    if title_date:
        return parse_date_from_title(title_date)
    
    # Try description metadata
    desc_metadata = extract_metadata_from_description(
        video_data.get('description', '')
    )
    if desc_metadata:
        return parse_metadata_date(desc_metadata)
    
    # Use modified_time for live archives
    if video_data.get('type') == 'live':
        return parse_timestamp(video_data['modified_time'], local_tz)
    
    # Fall back to created_time
    return parse_timestamp(video_data['created_time'], local_tz)
```

### 4. Classification with Confidence Score

```python
def classify_video_with_confidence(video_data):
    """Classify video with confidence score."""
    result = {
        'classification': None,
        'confidence': 0,
        'method': None,
        'warnings': []
    }
    
    # Method 1: Title parsing (high confidence)
    if title_date := extract_date_from_title(video_data.get('name', '')):
        result['classification'] = classify_from_title(title_date)
        result['confidence'] = 0.95
        result['method'] = 'title_parsing'
        return result
    
    # Method 2: Description metadata (high confidence)
    if metadata := extract_metadata_from_description(video_data.get('description', '')):
        result['classification'] = classify_from_metadata(metadata)
        result['confidence'] = 0.90
        result['method'] = 'description_metadata'
        return result
    
    # Method 3: Schedule tracker lookup (high confidence)
    if event := lookup_in_schedule_tracker(video_data['uri']):
        result['classification'] = classify_from_schedule(event)
        result['confidence'] = 0.95
        result['method'] = 'schedule_tracker'
        return result
    
    # Method 4: Time-based heuristics (medium confidence)
    if classification := classify_by_time_heuristics(video_data):
        result['classification'] = classification
        result['confidence'] = 0.60
        result['method'] = 'time_heuristics'
        result['warnings'].append('Low confidence - verify manually')
        return result
    
    # Unclassifiable
    result['warnings'].append('Could not classify video')
    return result
```

## Alternative Approaches

### 1. Webhook Integration
If Vimeo supports webhooks for live events:
- Listen for "live stream ended" events
- Capture actual stream end time
- Store in local database
- Use for classification

**Status**: Unknown if Vimeo API supports this

### 2. Polling for Archive Creation
- Poll recently created live events
- Detect when `archived` becomes `true`
- Record timestamp at that moment
- More accurate than created_time

**Disadvantages**: Requires continuous polling

### 3. Manual Review Queue
- Classify high-confidence videos automatically
- Queue low-confidence videos for manual review
- Build training data for ML classifier
- Improve heuristics over time

## Conclusion

**The core problem**: Vimeo's API does not preserve the actual streaming timestamp for live events. The `created_time` and `release_time` fields represent when the event was scheduled, not when it was streamed.

**The solution**: Do NOT rely on timestamps for classification. Instead:
1. Use `automaton_scheduler.py` to embed metadata in event descriptions
2. Parse standardized titles to extract date/time
3. Maintain a local schedule tracker
4. Use time-based heuristics only as a last resort
5. Implement confidence scoring and manual review

**Key takeaway**: Pre-scheduling with embedded metadata is the only reliable way to classify live event archives in Vimeo's current API.

## Next Steps

1. ✅ Implement metadata extraction from description
2. ✅ Implement title parsing for standardized format
3. ✅ Add confidence scoring to classification
4. ⬜ Add manual review queue for low-confidence videos
5. ⬜ Explore webhook support (if available)
6. ⬜ Document standard operating procedures for event creation

## References

- Vimeo API Documentation: https://developer.vimeo.com/api/reference
- Python Vimeo Client: https://github.com/vimeo/vimeo.py
- Project Documentation: `/home/jason/dev/projects/automatonmk2/AGENTS.md`
