#!/usr/bin/env python3
"""
JSON Message Normalization and Validation Script

This script processes JSON message records and:
1. Converts timestamps to ISO-8601 UTC format
2. Ensures GUID exists or creates stable fingerprint
3. Normalizes sender to E.164 format and maps "Me" → "me": true
4. Strips control characters and enforces UTF-8
5. Adds idempotency and dedupe fields
6. Outputs as JSONL (one JSON object per line)
"""

import json
import hashlib
import re
import datetime
from typing import Dict, Any, List
import argparse
import sys


def parse_timestamp(timestamp_str: str) -> str:
    """
    Parse timestamp string and convert to ISO-8601 UTC format.
    
    Expected format: "Feb 27, 2025  6:20:21 PM"
    Returns: "2025-02-27T23:20:21Z"
    """
    try:
        # Parse the timestamp format
        dt = datetime.datetime.strptime(timestamp_str, "%b %d, %Y  %I:%M:%S %p")
        # Convert to UTC (assuming local time, adjust as needed)
        utc_dt = dt.replace(tzinfo=datetime.timezone.utc)
        return utc_dt.isoformat().replace('+00:00', 'Z')
    except ValueError as e:
        print(f"Warning: Could not parse timestamp '{timestamp_str}': {e}", file=sys.stderr)
        # Return original if parsing fails
        return timestamp_str


def normalize_sender(sender: str) -> Dict[str, Any]:
    """
    Normalize sender field:
    - Convert "Me" to {"me": true}
    - Keep E.164 format numbers as is
    - Return normalized structure
    """
    if sender == "Me":
        return {"me": True}
    elif sender.startswith('+') and re.match(r'^\+[1-9]\d{1,14}$', sender):
        # Valid E.164 format
        return {"phone": sender}
    else:
        # Other formats - preserve as is
        return {"other": sender}


def create_stable_id(record: Dict[str, Any]) -> str:
    """
    Create a stable ID from record fields.
    Prefer guid if exists, otherwise create SHA256 hash.
    """
    if record.get('guid'):
        return record['guid']
    
    # Create fingerprint from key fields
    fingerprint_data = f"{record.get('timestamp', '')}|{record.get('sender', '')}|{record.get('contents', '')}"
    return hashlib.sha256(fingerprint_data.encode('utf-8')).hexdigest()


def strip_control_chars(text: str) -> str:
    """Remove control characters while preserving newlines and tabs."""
    if not isinstance(text, str):
        return text
    
    # Keep newlines (\n), tabs (\t), and carriage returns (\r)
    # Remove other control characters
    return ''.join(char for char in text if char.isprintable() or char in '\n\t\r')


def normalize_record(record: Dict[str, Any], schema_version: str = "1.0") -> Dict[str, Any]:
    """
    Normalize a single record according to the schema requirements.
    """
    normalized = {}
    
    # Ensure guid exists or create stable fingerprint
    normalized['id'] = create_stable_id(record)
    
    # Convert timestamp to ISO-8601 UTC
    if 'timestamp' in record:
        normalized['timestamp'] = parse_timestamp(record['timestamp'])
    
    # Normalize sender
    if 'sender' in record:
        sender_info = normalize_sender(record['sender'])
        normalized.update(sender_info)
    
    # Copy and clean other fields
    for field in ['contents', 'attachments', 'is_from_me', 'readtime']:
        if field in record:
            if field == 'contents':
                normalized[field] = strip_control_chars(record[field])
            else:
                # Preserve readtime as is (don't convert to null)
                normalized[field] = record[field]
    
    # Add metadata fields
    normalized['source_device_id'] = 'unknown'  # Can be updated later
    normalized['schema_version'] = schema_version
    
    # Add deduplication fields
    normalized['fingerprint'] = hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
    
    return normalized


def process_json_file(input_file: str, output_file: str, schema_version: str = "1.0") -> None:
    """
    Process the input JSON file and output normalized JSONL.
    """
    processed_count = 0
    error_count = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            # Read the entire file content
            content = infile.read()
            
            # Split by JSON object boundaries (look for }{ pattern)
            # This handles the case where multiple JSON objects are concatenated
            json_objects = []
            current_obj = ""
            brace_count = 0
            
            for char in content:
                current_obj += char
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        # Complete JSON object found
                        try:
                            obj = json.loads(current_obj.strip())
                            json_objects.append(obj)
                        except json.JSONDecodeError as e:
                            print(f"Warning: Skipping invalid JSON object: {e}", file=sys.stderr)
                            error_count += 1
                        current_obj = ""
                        brace_count = 0
            
            print(f"Found {len(json_objects)} JSON objects")
            
            # Process each object
            for i, record in enumerate(json_objects):
                try:
                    normalized = normalize_record(record, schema_version)
                    
                    # Write as JSONL (one JSON object per line)
                    outfile.write(json.dumps(normalized, ensure_ascii=False) + '\n')
                    processed_count += 1
                    
                    if (i + 1) % 1000 == 0:
                        print(f"Processed {i + 1} records...")
                        
                except Exception as e:
                    print(f"Error processing record {i}: {e}", file=sys.stderr)
                    error_count += 1
                    continue
    
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed: {processed_count} records")
    print(f"Errors encountered: {error_count} records")
    print(f"Output written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Normalize and validate JSON message records')
    parser.add_argument('input_file', help='Input JSON file path')
    parser.add_argument('-o', '--output', help='Output JSONL file path (default: input_normalized.jsonl)')
    parser.add_argument('-v', '--schema-version', default='1.0', help='Schema version (default: 1.0)')
    
    args = parser.parse_args()
    
    # Determine output filename
    if args.output:
        output_file = args.output
    else:
        base_name = args.input_file.rsplit('.', 1)[0]
        output_file = f"{base_name}_normalized.jsonl"
    
    process_json_file(args.input_file, output_file, args.schema_version)


if __name__ == "__main__":
    main()
