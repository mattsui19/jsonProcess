# JSON Message Normalization Script

This script normalizes and validates JSON message records according to a consistent schema.

## Features

- **Timestamp Conversion**: Converts timestamps to ISO-8601 UTC format (e.g., "2025-02-27T23:20:21Z")
- **GUID Validation**: Ensures each record has a unique ID, creating SHA256 fingerprints if missing
- **Sender Normalization**: Maps "Me" to `{"me": true}` and preserves E.164 phone numbers
- **Control Character Stripping**: Removes control characters while preserving newlines and tabs
- **UTF-8 Enforcement**: Ensures proper text encoding
- **Idempotency Fields**: Adds fingerprint hashes for deduplication
- **Schema Versioning**: Includes schema version and source device tracking
- **JSONL Output**: Outputs one JSON object per line for easy processing

## Usage

### Basic Usage

```bash
python normalize_json.py +19178268897.json
```

This will create `+19178268897_normalized.jsonl` as output.

### Custom Output File

```bash
python normalize_json.py +19178268897.json -o normalized_messages.jsonl
```

### Custom Schema Version

```bash
python normalize_json.py +19178268897.json -v 2.0
```

### Help

```bash
python normalize_json.py --help
```

## Input Format

The script expects a file containing concatenated JSON objects (not a JSON array). Each object should have these fields:

```json
{
  "attachments": [],
  "contents": "Message text",
  "guid": "5206B046-1C97-4B71-8C53-6DC57442CBA5",
  "is_from_me": false,
  "readtime": null,
  "sender": "+19178268897",
  "timestamp": "Feb 27, 2025  6:20:21 PM"
}
```

## Output Format

Each line contains a normalized JSON object with this schema:

```json
{
  "id": "5206B046-1C97-4B71-8C53-6DC57442CBA5",
  "timestamp": "2025-02-27T23:20:21Z",
  "phone": "+19178268897",
  "contents": "Message text",
  "attachments": [],
  "is_from_me": false,
  "readtime": null,
  "source_device_id": "unknown",
  "schema_version": "1.0",
  "fingerprint": "abc123..."
}
```

### Field Mappings

- **sender**: 
  - `"Me"` → `{"me": true}`
  - E.164 numbers → `{"phone": "+19178268897"}`
  - Others → `{"other": "value"}`
- **timestamp**: Converted to ISO-8601 UTC format
- **id**: Uses existing `guid` or creates SHA256 hash from content
- **fingerprint**: SHA256 hash of normalized record for deduplication

## Requirements

- Python 3.6 or higher
- No external dependencies (uses only standard library)

## Error Handling

- Invalid JSON objects are skipped with warnings
- Timestamp parsing errors preserve original values
- Processing continues even if individual records fail
- Progress updates every 1000 records

## Example

```bash
# Process the file
python normalize_json.py +19178268897.json

# Check the first few lines of output
head -5 +19178268897_normalized.jsonl | jq .

# Count total records
wc -l +19178268897_normalized.jsonl
```
