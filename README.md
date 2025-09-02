# JSON Data Processing Pipeline

A streamlined pipeline for processing concatenated JSON message data, cleaning it, and segmenting conversations.

## 🚀 Quick Start

```bash
# Clean and normalize the data
python clean_data.py +19178268897.json

# Segment conversations
python segment_conversations.py +19178268897_cleaned.jsonl +19178268897_segmented.jsonl

# Analyze segments
python analyze_segments.py +19178268897_segmented.jsonl
```

## 📁 Files

### Core Scripts
- **`clean_data.py`** - Main wrapper script for data cleaning
- **`process_original.py`** - Core processing logic (JSON parsing, normalization, feature extraction)
- **`segment_conversations.py`** - Conversation segmentation by date and time gaps
- **`analyze_segments.py`** - Analysis and insights from segmented conversations

### Input/Output
- **`+19178268897.json`** - Original concatenated JSON file
- **`+19178268897_cleaned.jsonl`** - Cleaned and normalized JSONL output
- **`+19178268897_segmented.jsonl`** - Final segmented conversations

### Documentation
- **`README.md`** - This file
- **`SEGMENTATION_SUMMARY.md`** - Detailed segmentation process documentation
- **`requirements.txt`** - Python dependencies

## 🔧 Features

### Data Cleaning (`clean_data.py`)
- ✅ Parses concatenated JSON objects
- ✅ Normalizes timestamps to UTC ISO-8601
- ✅ Extracts emojis and URLs into separate fields
- ✅ Computes message-level features (token count, questions, dates, etc.)
- ✅ Preserves all original data fields
- ✅ Strips control characters and enforces UTF-8

### Conversation Segmentation (`segment_conversations.py`)
- ✅ Groups messages by date
- ✅ Connects messages within 2-hour time windows
- ✅ Creates conversation segments with metadata
- ✅ Calculates timing statistics and gaps

### Analysis (`analyze_segments.py`)
- ✅ Segment size distribution
- ✅ Time gap analysis
- ✅ Participant behavior patterns
- ✅ Daily and weekly patterns
- ✅ Conversation flow insights

## 📊 Output Schema

### Cleaned Messages
```json
{
  "id": "unique_identifier",
  "timestamp": "2025-02-27T18:20:21+00:00",
  "sender": "+19178268897",
  "is_from_me": false,
  "readtime": null,
  "contents": "Hey",
  "attachments": [],
  "source_device_id": "unknown",
  "fingerprint": "unique_identifier",
  "extracted_data": {
    "emojis": [],
    "urls": []
  },
  "features": {
    "token_count": 1,
    "character_count": 3,
    "is_question": false,
    "is_exclamation": false,
    "contains_date": false,
    "contains_place": false,
    "contains_money": false,
    "mentions": [],
    "has_emojis": false,
    "has_urls": false,
    "emoji_count": 0,
    "url_count": 0
  }
}
```

### Conversation Segments
```json
{
  "segment_id": "segment_0001",
  "date": "2025-02-27",
  "start_time": "2025-02-27T18:20:21+00:00",
  "end_time": "2025-02-27T18:20:21+00:00",
  "message_count": 1,
  "participants": ["+19178268897"],
  "messages": [...],
  "time_gaps": [],
  "total_duration_minutes": 0,
  "avg_gap_minutes": 0,
  "max_gap_minutes": 0,
  "min_gap_minutes": 0
}
```

## 🛠️ Requirements

- Python 3.6+
- Virtual environment (recommended)
- Dependencies: `emoji>=2.14.0`

## 📈 Performance

- **Processing Speed**: ~6,000 messages in <2 minutes
- **Memory Usage**: Line-by-line processing for large files
- **Output Format**: JSONL for easy analysis and streaming

## 🔍 Usage Examples

### Basic Cleaning
```bash
python clean_data.py input.json
# Creates input_cleaned.jsonl
```

### Custom Segmentation
```bash
python segment_conversations.py input_cleaned.jsonl output_segmented.jsonl
```

### Analysis
```bash
python analyze_segments.py input_segmented.jsonl
```

## 🎯 Key Benefits

1. **Unified Pipeline**: Single command from raw JSON to clean data
2. **No Schema Version**: Clean, simple output structure
3. **Efficient Processing**: Handles large files without memory issues
4. **Rich Features**: Comprehensive message analysis and conversation insights
5. **Flexible Output**: JSONL format for easy integration with other tools

## 📝 Notes

- All timestamps are converted to UTC ISO-8601 format
- Emojis and URLs are extracted and preserved in separate fields
- Conversation segmentation uses a 2-hour time window (configurable)
- The pipeline preserves all original data while adding computed features
