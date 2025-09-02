#!/usr/bin/env python3
"""
Data ingestion and cleaning script for normalized JSONL output.
Parses JSON, normalizes timestamps to UTC, extracts emojis/URLs, and computes message features.
"""

import json
import sys
import re
from datetime import datetime, timezone
from urllib.parse import urlparse
import emoji
from collections import Counter


def normalize_timestamp(timestamp_str):
    """Normalize timestamp to UTC ISO format."""
    if not timestamp_str:
        return None
    
    try:
        # Parse ISO format and ensure UTC
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return None


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


def clean_and_enrich_record(record):
    """Clean and enrich a single record with computed features."""
    
    # Extract and clean content
    original_content = record.get('contents', '')
    
    # Extract emojis and URLs
    content_no_emojis, emojis = extract_emojis(original_content)
    content_no_urls, urls = extract_urls(content_no_emojis)
    final_content = content_no_urls
    
    # Normalize timestamp
    normalized_timestamp = normalize_timestamp(record.get('timestamp'))
    
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
    
    # Build cleaned record
    cleaned_record = {
        "id": record.get('id'),
        "timestamp": normalized_timestamp,
        "sender": record.get('phone') or record.get('me'),
        "is_from_me": record.get('is_from_me'),
        "readtime": record.get('readtime'),
        "contents": final_content,
        "attachments": record.get('attachments', []),
        "source_device_id": record.get('source_device_id'),
        "schema_version": record.get('schema_version'),
        "fingerprint": record.get('fingerprint'),
        "extracted_data": {
            "emojis": emojis,
            "urls": urls
        },
        "features": features
    }
    
    return cleaned_record


def process_jsonl_file(input_file, output_file):
    """Process JSONL file and output cleaned records."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        return
    
    print(f"Processing {len(lines)} records...")
    
    cleaned_records = []
    error_count = 0
    
    for i, line in enumerate(lines):
        try:
            record = json.loads(line.strip())
            cleaned_record = clean_and_enrich_record(record)
            cleaned_records.append(cleaned_record)
        except json.JSONDecodeError as e:
            print(f"Error parsing line {i+1}: {e}")
            error_count += 1
        except Exception as e:
            print(f"Error processing line {i+1}: {e}")
            error_count += 1
    
    # Write cleaned records
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in cleaned_records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    print(f"Processing complete!")
    print(f"  Input records: {len(lines)}")
    print(f"  Output records: {len(cleaned_records)}")
    print(f"  Errors: {error_count}")
    print(f"  Output file: {output_file}")


def main():
    if len(sys.argv) != 3:
        print("Usage: python ingest_clean.py <input_file.jsonl> <output_file.jsonl>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_jsonl_file(input_file, output_file)


if __name__ == "__main__":
    main()
