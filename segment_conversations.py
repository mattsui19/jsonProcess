#!/usr/bin/env python3
"""
Conversation segmentation script.
Segments messages based on date and connects messages within 2 hours into conversation segments.
"""

import json
import sys
from datetime import datetime, timezone, timedelta
from collections import defaultdict


def parse_timestamp(timestamp_str):
    """Parse ISO timestamp string and return datetime object."""
    if not timestamp_str:
        return None
    
    try:
        # Parse ISO format like "2025-02-27T18:20:21+00:00"
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt
    except ValueError:
        return None


def get_date_key(timestamp):
    """Get date key for grouping (YYYY-MM-DD)."""
    if not timestamp:
        return None
    return timestamp.date().isoformat()


def is_within_time_window(timestamp1, timestamp2, hours=2):
    """Check if two timestamps are within specified hours of each other."""
    if not timestamp1 or not timestamp2:
        return False
    
    time_diff = abs(timestamp1 - timestamp2)
    return time_diff <= timedelta(hours=hours)


def segment_conversations(input_file, output_file, time_window_hours=2):
    """Segment conversations based on date and time gaps."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        return
    
    print(f"Processing {len(lines)} messages for conversation segmentation...")
    
    # Parse and sort messages by timestamp
    messages = []
    for line in lines:
        try:
            record = json.loads(line.strip())
            timestamp = parse_timestamp(record.get('timestamp'))
            if timestamp:
                messages.append({
                    'record': record,
                    'timestamp': timestamp,
                    'date_key': get_date_key(timestamp)
                })
        except json.JSONDecodeError:
            continue
    
    # Sort by timestamp
    messages.sort(key=lambda x: x['timestamp'])
    
    print(f"Valid messages with timestamps: {len(messages)}")
    
    # Group by date first
    date_groups = defaultdict(list)
    for msg in messages:
        date_groups[msg['date_key']].append(msg)
    
    print(f"Date groups found: {len(date_groups)}")
    
    # Segment conversations within each date group
    all_segments = []
    segment_id = 1
    
    for date_key, date_messages in sorted(date_groups.items()):
        print(f"Processing date: {date_key} ({len(date_messages)} messages)")
        
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
            if is_within_time_window(prev_msg['timestamp'], current_msg['timestamp'], time_window_hours):
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
    
    # Write segmented conversations
    print(f"Writing {len(all_segments)} conversation segments...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for segment in all_segments:
            f.write(json.dumps(segment, ensure_ascii=False) + '\n')
    
    # Print summary statistics
    print(f"\n=== CONVERSATION SEGMENTATION SUMMARY ===")
    print(f"Total segments: {len(all_segments)}")
    
    if all_segments:
        total_messages = sum(seg['message_count'] for seg in all_segments)
        avg_messages_per_segment = total_messages / len(all_segments)
        avg_duration = sum(seg['total_duration_minutes'] for seg in all_segments) / len(all_segments)
        
        print(f"Total messages: {total_messages}")
        print(f"Average messages per segment: {avg_messages_per_segment:.1f}")
        print(f"Average segment duration: {avg_duration:.1f} minutes")
        
        # Date distribution
        date_counts = defaultdict(int)
        for seg in all_segments:
            date_counts[seg['date']] += 1
        
        print(f"\nSegments per date:")
        for date, count in sorted(date_counts.items()):
            print(f"  {date}: {count} segments")
        
        # Segment size distribution
        small_segments = sum(1 for seg in all_segments if seg['message_count'] <= 5)
        medium_segments = sum(1 for seg in all_segments if 6 <= seg['message_count'] <= 20)
        large_segments = sum(1 for seg in all_segments if seg['message_count'] > 20)
        
        print(f"\nSegment size distribution:")
        print(f"  Small (≤5 messages): {small_segments} ({small_segments/len(all_segments)*100:.1f}%)")
        print(f"  Medium (6-20 messages): {medium_segments} ({medium_segments/len(all_segments)*100:.1f}%)")
        print(f"  Large (>20 messages): {large_segments} ({large_segments/len(all_segments)*100:.1f}%)")
    
    print(f"\nOutput saved to: {output_file}")


def main():
    if len(sys.argv) != 3:
        print("Usage: python segment_conversations.py <input_file.jsonl> <output_file.jsonl>")
        print("Example: python segment_conversations.py +19178268897_cleaned.jsonl +19178268897_segmented.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    segment_conversations(input_file, output_file)


if __name__ == "__main__":
    main()
