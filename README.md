# JSON Data Processing Pipeline

A streamlined pipeline for processing concatenated JSON message data directly to segmented conversations in one unified step.

## 🚀 Quick Start

```bash
# Process from original JSON to segmented conversations in one command
python clean_data.py +19178268897.json

# Analyze the segmented data
python analyze_segments.py +19178268897_segmented.jsonl
```

## 📁 Files

### Core Scripts (3 files total)
- **`clean_data.py`** - Simple wrapper for the unified pipeline
- **`process_original.py`** - Complete processing logic (JSON parsing, normalization, features, segmentation)
- **`analyze_segments.py`** - Analysis and insights from segmented conversations

### Input/Output
- **`+19178268897.json`** - Original concatenated JSON file
- **`+19178268897_segmented.jsonl`** - Final segmented conversations (no intermediate files!)

### Documentation
- **`README.md`** - This file
- **`SEGMENTATION_SUMMARY.md`** - Detailed segmentation process documentation
- **`requirements.txt`** - Python dependencies

## 🔧 Features

### Unified Processing (`clean_data.py` → `process_original.py`)
- ✅ Parses concatenated JSON objects
- ✅ Normalizes timestamps to UTC ISO-8601
- ✅ Extracts emojis and URLs into separate fields
- ✅ Computes message-level features (token count, questions, dates, etc.)
- ✅ Preserves all original data fields
- ✅ Strips control characters and enforces UTF-8
- ✅ **Segments conversations by date and time gaps**
- ✅ **Outputs final segmented JSONL directly**

### Analysis (`analyze_segments.py`)
- ✅ Segment size distribution
- ✅ Time gap analysis
- ✅ Participant behavior patterns
- ✅ Daily and weekly patterns
- ✅ Conversation flow insights

## 📊 Output Schema

### Final Conversation Segments
```json
{
  "segment_id": "segment_0001",
  "date": "2025-02-27",
  "start_time": "2025-02-27T18:20:21+00:00",
  "end_time": "2025-02-27T18:20:21+00:00",
  "message_count": 1,
  "participants": ["+19178268897"],
  "messages": [
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
  ],
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
- **Pipeline Efficiency**: **Single command from raw JSON to segmented output**

## 🔍 Usage Examples

### One-Command Processing
```bash
python clean_data.py input.json
# Creates input_segmented.jsonl directly
```

### Custom Output
```bash
python process_original.py input.json output_segmented.jsonl
```

### Analysis
```bash
python analyze_segments.py input_segmented.jsonl
```

## 🎯 Key Benefits

1. **True One-Step Pipeline**: Single command from raw JSON to segmented conversations
2. **No Intermediate Files**: Goes directly to final output
3. **No Schema Version**: Clean, simple output structure
4. **Efficient Processing**: Handles large files without memory issues
5. **Rich Features**: Comprehensive message analysis and conversation insights
6. **Flexible Output**: JSONL format for easy integration with other tools

## 📝 Notes

- All timestamps are converted to UTC ISO-8601 format
- Emojis and URLs are extracted and preserved in separate fields
- Conversation segmentation uses a 2-hour time window (configurable)
- The pipeline preserves all original data while adding computed features
- **No intermediate cleaning step needed** - everything happens in one pass
