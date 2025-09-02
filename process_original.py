#!/usr/bin/env python3
"""
Unified data processing script that goes directly from original concatenated JSON to segmented JSONL.
Combines normalization, timestamp conversion, feature extraction, and conversation segmentation in one pipeline.
"""

import json
import sys
import re
from datetime import datetime, timezone
from urllib.parse import urlparse
import emoji
from collections import Counter


def parse_timestamp(timestamp_str):
    """Parse timestamp string and convert to ISO-8601 UTC format."""
    if not timestamp_str:
        return None
    
    try:
        # Parse format like "Feb 27, 2025  6:20:21 PM"
        dt = datetime.strptime(timestamp_str, "%b %d, %Y  %I:%M:%S %p")
        # Make it timezone-aware (assuming local time, convert to UTC)
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return None


def parse_timestamp_iso(timestamp_str):
    """Parse ISO timestamp string into datetime object."""
    if not timestamp_str:
        return None
    
    try:
        # Parse ISO format like "2025-02-27T18:20:21+00:00"
        dt = datetime.fromisoformat(timestamp_str)
        return dt
    except ValueError:
        return None


def normalize_sender(sender):
    """Normalize sender field."""
    if sender == "Me":
        return True
    elif sender and sender.startswith('+'):
        return sender
    else:
        return sender


def create_stable_id(record):
    """Create stable ID from guid or generate fingerprint."""
    guid = record.get('guid')
    if guid:
        return guid
    
    # Create SHA256 fingerprint if no guid
    import hashlib
    fingerprint_data = f"{record.get('timestamp', '')}|{record.get('sender', '')}|{record.get('contents', '')}"
    return hashlib.sha256(fingerprint_data.encode('utf-8')).hexdigest()


def strip_control_chars(text):
    """Strip control characters and enforce UTF-8."""
    if not text:
        return text
    
    # Remove control characters except newlines and tabs
    cleaned = ''.join(char for char in text if char.isprintable() or char in '\n\t')
    return cleaned.strip()


def extract_emojis(text):
    """Extract emojis from text and return cleaned text and emoji list."""
    if not text:
        return text, []
    
    # Find all emojis
    emoji_list = emoji.emoji_list(text)
    emojis = [e['emoji'] for e in emoji_list]
    
    # Remove emojis from text by replacing them with empty string
    cleaned_text = text
    for e in emojis:
        cleaned_text = cleaned_text.replace(e, '')
    
    return cleaned_text.strip(), emojis


def extract_urls(text):
    """Extract URLs from text and return cleaned text and URL list."""
    if not text:
        return text, []
    
    # URL regex pattern
    url_pattern = r'https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?'
    
    urls = re.findall(url_pattern, text)
    
    # Remove URLs from text
    cleaned_text = re.sub(url_pattern, '', text)
    
    return cleaned_text.strip(), urls


def detect_question(text):
    """Detect if message is a question."""
    if not text:
        return False
    
    # Check for question marks
    if '?' in text:
        return True
    
    # Check for question words at beginning
    question_words = ['what', 'when', 'where', 'who', 'why', 'how', 'which', 'whose', 'whom']
    text_lower = text.lower().strip()
    
    for word in question_words:
        if text_lower.startswith(word + ' '):
            return True
    
    return False


def detect_exclamation(text):
    """Detect if message contains exclamation."""
    return '!' in text if text else False


def detect_contains_date(text):
    """Detect if message contains date-like patterns."""
    if not text:
        return False
    
    # Date patterns
    date_patterns = [
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',  # MM/DD/YYYY
        r'\b\d{1,2}-\d{1,2}-\d{2,4}\b',  # MM-DD-YYYY
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}\b',  # Month DD
        r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}\b',  # Full month DD
        r'\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\b',  # Time
        r'\b(?:today|tomorrow|yesterday|tonight|this morning|this afternoon|this evening)\b'  # Relative dates
    ]
    
    for pattern in date_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    
    return False


def detect_contains_place(text):
    """Detect if message contains place-like patterns."""
    if not text:
        return False
    
    # Place patterns
    place_patterns = [
        r'\b(?:at|in|to|from)\s+\w+',  # Preposition + place
        r'\b(?:restaurant|cafe|bar|store|shop|mall|park|beach|airport|station)\b',  # Common places
        r'\b(?:street|avenue|road|drive|lane|way|plaza|square)\b',  # Street types
        r'\b[A-Z][a-z]+(?:[-\s][A-Z][a-z]+)*\s+(?:Street|Ave|Road|Drive|Lane|Way|Plaza|Square)\b',  # Named streets
        r'\b(?:New York|Los Angeles|Chicago|Houston|Phoenix|Philadelphia|San Antonio|San Diego|Dallas|San Jose)\b'  # Major cities
    ]
    
    for pattern in place_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    
    return False


def detect_contains_money(text):
    """Detect if message contains money patterns."""
    if not text:
        return False
    
    # Money patterns
    money_patterns = [
        r'\$\d+(?:\.\d{2})?',  # $123.45
        r'\b\d+(?:\.\d{2})?\s*(?:dollars?|bucks?|USD)\b',  # 123.45 dollars
        r'\b(?:free|cheap|expensive|cost|price|pay|paid|spent|bought|sold)\b'  # Money-related words
    ]
    
    for pattern in money_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    
    return False


def extract_mentions(text):
    """Extract @mentions from text."""
    if not text:
        return []
    
    # Find @mentions
    mention_pattern = r'@(\w+)'
    mentions = re.findall(mention_pattern, text)
    
    return mentions


def count_tokens(text):
    """Count tokens (words) in text."""
    if not text:
        return 0
    
    # Simple word count (can be enhanced with proper tokenization)
    words = re.findall(r'\b\w+\b', text)
    return len(words)


def normalize_record(record):
    """Normalize a single record with all transformations."""
    
    # Normalize timestamp
    timestamp = record.get('timestamp')
    normalized_timestamp = parse_timestamp(timestamp) if timestamp else None
    
    # Normalize sender
    sender = record.get('sender')
    normalized_sender = normalize_sender(sender)
    
    # Create stable ID
    stable_id = create_stable_id(record)
    
    # Clean and extract content
    original_content = record.get('contents', '')
    content_no_emojis, emojis = extract_emojis(original_content)
    content_no_urls, urls = extract_urls(content_no_emojis)
    final_content = content_no_urls
    
    # Strip control characters
    final_content = strip_control_chars(final_content)
    
    # Compute message features
    features = {
        "token_count": count_tokens(final_content),
        "character_count": len(final_content),
        "is_question": detect_question(final_content),
        "is_exclamation": detect_exclamation(final_content),
        "contains_date": detect_contains_date(final_content),
        "contains_place": detect_contains_place(final_content),
        "contains_money": detect_contains_money(final_content),
        "mentions": extract_mentions(final_content),
        "has_emojis": len(emojis) > 0,
        "has_urls": len(urls) > 0,
        "emoji_count": len(emojis),
        "url_count": len(urls)
    }
    
    # Build normalized record
    normalized = {
        "id": stable_id,
        "timestamp": normalized_timestamp,
        "sender": normalized_sender,
        "is_from_me": record.get('is_from_me'),
        "readtime": record.get('readtime'),  # Preserve as-is
        "contents": final_content,
        "attachments": record.get('attachments', []),
        "source_device_id": "unknown",
        "fingerprint": stable_id,
        "extracted_data": {
            "emojis": emojis,
            "urls": urls
        },
        "features": features
    }
    
    return normalized


def split_concatenated_json(json_content):
    """Split concatenated JSON objects into individual records."""
    records = []
    
    # Find all JSON objects using regex
    # Look for patterns like {...} that are valid JSON
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    
    matches = re.finditer(json_pattern, json_content)
    
    for match in matches:
        try:
            json_str = match.group(0)
            record = json.loads(json_str)
            records.append(record)
        except json.JSONDecodeError:
            # Skip malformed JSON
            continue
    
    return records


def segment_conversations(messages, time_window_hours=2):
    """Segment conversations based on date and time gaps."""
    
    if not messages:
        return []
    
    # Sort by timestamp
    messages.sort(key=lambda x: x['timestamp'])
    
    # Group by date first
    date_groups = {}
    for msg in messages:
        date_key = msg['timestamp'].date().isoformat()
        if date_key not in date_groups:
            date_groups[date_key] = []
        date_groups[date_key].append(msg)
    
    # Segment conversations within each date group
    all_segments = []
    segment_id = 1
    
    for date_key, date_messages in sorted(date_groups.items()):
        # Sort messages within the date by timestamp
        date_messages.sort(key=lambda x: x['timestamp'])
        
        # Start new segment
        current_segment = {
            'segment_id': f"segment_{segment_id:04d}",
            'date': date_key,
            'start_time': date_messages[0]['timestamp'].isoformat(),
            'end_time': date_messages[0]['timestamp'].isoformat(),
            'message_count': 1,
            'participants': set(),
            'messages': [date_messages[0]['record']],
            'time_gaps': [],
            'total_duration_minutes': 0
        }
        
        # Add participant
        sender = date_messages[0]['record'].get('sender')
        if sender:
            current_segment['participants'].add(str(sender))
        
        # Process remaining messages in the date
        for i in range(1, len(date_messages)):
            current_msg = date_messages[i]
            prev_msg = date_messages[i-1]
            
            # Check if within time window
            time_diff = (current_msg['timestamp'] - prev_msg['timestamp']).total_seconds() / 3600
            if time_diff <= time_window_hours:
                # Continue current segment
                current_segment['messages'].append(current_msg['record'])
                current_segment['message_count'] += 1
                current_segment['end_time'] = current_msg['timestamp'].isoformat()
                
                # Add participant
                sender = current_msg['record'].get('sender')
                if sender:
                    current_segment['participants'].add(str(sender))
                
                # Calculate time gap
                time_gap = (current_msg['timestamp'] - prev_msg['timestamp']).total_seconds() / 60
                current_segment['time_gaps'].append(time_gap)
                
            else:
                # Time gap too large, start new segment
                # Calculate total duration for current segment
                if len(current_segment['time_gaps']) > 0:
                    current_segment['total_duration_minutes'] = sum(current_segment['time_gaps'])
                
                # Convert participants set to list for JSON serialization
                current_segment['participants'] = list(current_segment['participants'])
                
                # Add segment statistics
                current_segment['avg_gap_minutes'] = (
                    sum(current_segment['time_gaps']) / len(current_segment['time_gaps'])
                    if current_segment['time_gaps'] else 0
                )
                current_segment['max_gap_minutes'] = max(current_segment['time_gaps']) if current_segment['time_gaps'] else 0
                current_segment['min_gap_minutes'] = min(current_segment['time_gaps']) if current_segment['time_gaps'] else 0
                
                all_segments.append(current_segment)
                
                # Start new segment
                segment_id += 1
                current_segment = {
                    'segment_id': f"segment_{segment_id:04d}",
                    'date': date_key,
                    'start_time': current_msg['timestamp'].isoformat(),
                    'end_time': current_msg['timestamp'].isoformat(),
                    'message_count': 1,
                    'participants': set(),
                    'messages': [current_msg['record']],
                    'time_gaps': [],
                    'total_duration_minutes': 0
                }
                
                # Add participant
                sender = current_msg['record'].get('sender')
                if sender:
                    current_segment['participants'].add(str(sender))
        
        # Don't forget the last segment of the date
        if current_segment['message_count'] > 0:
            # Calculate total duration for final segment
            if len(current_segment['time_gaps']) > 0:
                current_segment['total_duration_minutes'] = sum(current_segment['time_gaps'])
            
            # Convert participants set to list for JSON serialization
            current_segment['participants'] = list(current_segment['participants'])
            
            # Add segment statistics
            current_segment['avg_gap_minutes'] = (
                sum(current_segment['time_gaps']) / len(current_segment['time_gaps'])
                if current_segment['time_gaps'] else 0
            )
            current_segment['max_gap_minutes'] = max(current_segment['time_gaps']) if current_segment['time_gaps'] else 0
            current_segment['min_gap_minutes'] = min(current_segment['time_gaps']) if current_segment['time_gaps'] else 0
            
            all_segments.append(current_segment)
    
    return all_segments


def process_original_file(input_file, output_file, time_window_hours=2):
    """Process original concatenated JSON file directly to segmented JSONL."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        return
    
    print(f"🚀 Processing original file: {input_file}")
    print(f"📁 File size: {len(content):,} characters")
    
    # Split concatenated JSON
    print("🔍 Splitting concatenated JSON objects...")
    records = split_concatenated_json(content)
    print(f"✅ Found {len(records)} JSON objects")
    
    if not records:
        print("❌ No valid JSON objects found!")
        return
    
    # Process each record
    print("🧹 Normalizing and enriching records...")
    normalized_records = []
    error_count = 0
    
    for i, record in enumerate(records):
        try:
            normalized_record = normalize_record(record)
            normalized_records.append(normalized_record)
        except Exception as e:
            print(f"⚠️  Error processing record {i+1}: {e}")
            error_count += 1
    
    print(f"✅ Processed {len(normalized_records)} records")
    
    # Segment conversations
    print("📅 Segmenting conversations...")
    
    # Prepare messages for segmentation
    messages_for_segmentation = []
    for record in normalized_records:
        timestamp = parse_timestamp_iso(record.get('timestamp'))
        if timestamp:
            messages_for_segmentation.append({
                'record': record,
                'timestamp': timestamp
            })
    
    segments = segment_conversations(messages_for_segmentation, time_window_hours)
    
    print(f"✅ Created {len(segments)} conversation segments")
    
    # Write segmented conversations
    print("💾 Writing segmented JSONL output...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for segment in segments:
            f.write(json.dumps(segment, ensure_ascii=False) + '\n')
    
    # Print summary statistics
    print(f"\n🎉 Processing complete!")
    print(f"📊 Input JSON objects: {len(records)}")
    print(f"📊 Output records: {len(normalized_records)}")
    print(f"📊 Conversation segments: {len(segments)}")
    print(f"⚠️  Errors: {error_count}")
    print(f"📁 Output file: {output_file}")
    
    if segments:
        total_messages = sum(seg['message_count'] for seg in segments)
        avg_messages_per_segment = total_messages / len(segments)
        avg_duration = sum(seg['total_duration_minutes'] for seg in segments) / len(segments)
        
        print(f"\n📈 Summary Statistics:")
        print(f"   Total messages: {total_messages:,}")
        print(f"   Average messages per segment: {avg_messages_per_segment:.1f}")
        print(f"   Average segment duration: {avg_duration:.1f} minutes")
        
        # Segment size distribution
        small_segments = sum(1 for seg in segments if seg['message_count'] <= 5)
        medium_segments = sum(1 for seg in segments if 6 <= seg['message_count'] <= 20)
        large_segments = sum(1 for seg in segments if seg['message_count'] > 20)
        
        print(f"\n📊 Segment size distribution:")
        print(f"   Small (≤5 messages): {small_segments} ({small_segments/len(segments)*100:.1f}%)")
        print(f"   Medium (6-20 messages): {medium_segments} ({medium_segments/len(segments)*100:.1f}%)")
        print(f"   Large (>20 messages): {large_segments} ({large_segments/len(segments)*100:.1f}%)")


def main():
    if len(sys.argv) != 3:
        print("Usage: python process_original.py <input_file.json> <output_file.jsonl>")
        print("Example: python process_original.py +19178268897.json +19178268897_segmented.jsonl")
        print("\nThis script will:")
        print("  1. Parse concatenated JSON objects")
        print("  2. Clean and normalize data")
        print("  3. Extract features (emojis, URLs, etc.)")
        print("  4. Segment conversations by date and time")
        print("  5. Output final segmented JSONL")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_original_file(input_file, output_file)


if __name__ == "__main__":
    main()
