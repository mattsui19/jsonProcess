#!/usr/bin/env python3
"""
Unified data processing script that goes directly from original concatenated JSON to cleaned JSONL.
Combines normalization, timestamp conversion, and feature extraction in one pipeline.
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


def normalize_record(record, schema_version="1.0"):
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
        "schema_version": schema_version,
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


def process_original_file(input_file, output_file, schema_version="1.0"):
    """Process original concatenated JSON file directly to cleaned JSONL."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        return
    
    print(f"Processing original file: {input_file}")
    print(f"File size: {len(content):,} characters")
    
    # Split concatenated JSON
    print("Splitting concatenated JSON objects...")
    records = split_concatenated_json(content)
    print(f"Found {len(records)} JSON objects")
    
    if not records:
        print("No valid JSON objects found!")
        return
    
    # Process each record
    print("Normalizing and enriching records...")
    normalized_records = []
    error_count = 0
    
    for i, record in enumerate(records):
        try:
            normalized_record = normalize_record(record, schema_version)
            normalized_records.append(normalized_record)
        except Exception as e:
            print(f"Error processing record {i+1}: {e}")
            error_count += 1
    
    # Write cleaned records
    print("Writing cleaned JSONL output...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in normalized_records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    print(f"Processing complete!")
    print(f"  Input JSON objects: {len(records)}")
    print(f"  Output records: {len(normalized_records)}")
    print(f"  Errors: {error_count}")
    print(f"  Output file: {output_file}")


def main():
    if len(sys.argv) != 3:
        print("Usage: python process_original.py <original_file.json> <output_file.jsonl>")
        print("Example: python process_original.py +19178268897.json +19178268897_cleaned.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_original_file(input_file, output_file)


if __name__ == "__main__":
    main()
