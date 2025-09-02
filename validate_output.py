#!/usr/bin/env python3
"""
Validation script for normalized JSONL output.
Checks schema compliance and data quality.
"""

import json
import sys
from collections import Counter


def validate_record(record: dict, line_num: int) -> list:
    """Validate a single record and return list of errors."""
    errors = []
    
    # Check required fields
    required_fields = ['id', 'timestamp', 'contents', 'source_device_id', 'schema_version', 'fingerprint']
    for field in required_fields:
        if field not in record:
            errors.append(f"Line {line_num}: Missing required field '{field}'")
    
    # Check timestamp format (should be ISO-8601)
    if 'timestamp' in record:
        timestamp = record['timestamp']
        if not isinstance(timestamp, str) or not timestamp.endswith('Z'):
            errors.append(f"Line {line_num}: Invalid timestamp format: {timestamp}")
    
    # Check sender normalization
    sender_fields = [k for k in record.keys() if k in ['me', 'phone', 'other']]
    if not sender_fields:
        errors.append(f"Line {line_num}: Missing sender normalization")
    elif len(sender_fields) > 1:
        errors.append(f"Line {line_num}: Multiple sender fields: {sender_fields}")
    
    # Check ID format
    if 'id' in record:
        record_id = record['id']
        if not isinstance(record_id, str) or len(record_id) < 10:
            errors.append(f"Line {line_num}: Invalid ID format: {record_id}")
    
    # Check fingerprint format
    if 'fingerprint' in record:
        fp = record['fingerprint']
        if not isinstance(fp, str) or len(fp) != 64:  # SHA256 hex length
            errors.append(f"Line {line_num}: Invalid fingerprint format: {fp}")
    
    return errors


def main():
    if len(sys.argv) != 2:
        print("Usage: python validate_output.py <normalized_file.jsonl>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    
    print(f"Validating {len(lines)} records...")
    
    total_errors = 0
    schema_versions = Counter()
    sender_types = Counter()
    timestamp_formats = Counter()
    
    for i, line in enumerate(lines, 1):
        try:
            record = json.loads(line.strip())
            
            # Validate the record
            errors = validate_record(record, i)
            if errors:
                for error in errors:
                    print(f"ERROR: {error}")
                total_errors += len(errors)
            
            # Collect statistics
            schema_versions[record.get('schema_version', 'unknown')] += 1
            if 'me' in record:
                sender_types['me'] += 1
            elif 'phone' in record:
                sender_types['phone'] += 1
            elif 'other' in record:
                sender_types['other'] += 1
            
            if 'timestamp' in record:
                timestamp_formats[record['timestamp'][:10]] += 1  # Date part only
                
        except json.JSONDecodeError as e:
            print(f"ERROR: Line {i}: Invalid JSON: {e}")
            total_errors += 1
        except Exception as e:
            print(f"ERROR: Line {i}: Unexpected error: {e}")
            total_errors += 1
    
    # Print summary
    print(f"\n=== VALIDATION SUMMARY ===")
    print(f"Total records: {len(lines)}")
    print(f"Total errors: {total_errors}")
    print(f"Success rate: {((len(lines) - total_errors) / len(lines) * 100):.1f}%")
    
    print(f"\n=== SCHEMA VERSIONS ===")
    for version, count in schema_versions.most_common():
        print(f"  {version}: {count}")
    
    print(f"\n=== SENDER TYPES ===")
    for sender_type, count in sender_types.most_common():
        print(f"  {sender_type}: {count}")
    
    print(f"\n=== TIMESTAMP DATES ===")
    for date, count in timestamp_formats.most_common(5):
        print(f"  {date}: {count} records")
    
    if total_errors == 0:
        print(f"\n✅ All records passed validation!")
        sys.exit(0)
    else:
        print(f"\n❌ {total_errors} errors found")
        sys.exit(1)


if __name__ == "__main__":
    main()
