#!/usr/bin/env python3
"""
Analysis script for segmented conversations.
Provides insights about conversation patterns, timing, and participant behavior.
"""

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone


def analyze_segments(input_file):
    """Analyze segmented conversations and provide insights."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        return
    
    print(f"Analyzing {len(lines)} conversation segments...")
    
    segments = []
    for line in lines:
        try:
            segment = json.loads(line.strip())
            segments.append(segment)
        except json.JSONDecodeError:
            continue
    
    if not segments:
        print("No valid segments found!")
        return
    
    # Sort segments by date and start time
    segments.sort(key=lambda x: (x['date'], x['start_time']))
    
    print(f"\n=== CONVERSATION SEGMENT ANALYSIS ===")
    print(f"Total segments: {len(segments)}")
    print(f"Date range: {segments[0]['date']} to {segments[-1]['date']}")
    
    # Basic statistics
    total_messages = sum(seg['message_count'] for seg in segments)
    avg_messages_per_segment = total_messages / len(segments)
    avg_duration = sum(seg['total_duration_minutes'] for seg in segments) / len(segments)
    
    print(f"Total messages: {total_messages:,}")
    print(f"Average messages per segment: {avg_messages_per_segment:.1f}")
    print(f"Average segment duration: {avg_duration:.1f} minutes")
    
    # Segment size analysis
    print(f"\n=== SEGMENT SIZE ANALYSIS ===")
    small_segments = [seg for seg in segments if seg['message_count'] <= 5]
    medium_segments = [seg for seg in segments if 6 <= seg['message_count'] <= 20]
    large_segments = [seg for seg in segments if seg['message_count'] > 20]
    
    print(f"Small segments (≤5 messages): {len(small_segments)} ({len(small_segments)/len(segments)*100:.1f}%)")
    print(f"Medium segments (6-20 messages): {len(medium_segments)} ({len(medium_segments)/len(segments)*100:.1f}%)")
    print(f"Large segments (>20 messages): {len(large_segments)} ({len(large_segments)/len(segments)*100:.1f}%)")
    
    # Largest segments
    largest_segments = sorted(segments, key=lambda x: x['message_count'], reverse=True)[:5]
    print(f"\nTop 5 largest conversation segments:")
    for i, seg in enumerate(largest_segments, 1):
        duration_hours = seg['total_duration_minutes'] / 60
        print(f"  {i}. {seg['date']} - {seg['message_count']} messages ({duration_hours:.1f} hours)")
    
    # Time gap analysis
    print(f"\n=== TIME GAP ANALYSIS ===")
    all_gaps = []
    for seg in segments:
        all_gaps.extend(seg['time_gaps'])
    
    if all_gaps:
        avg_gap = sum(all_gaps) / len(all_gaps)
        max_gap = max(all_gaps)
        min_gap = min(all_gaps)
        
        print(f"Average time between messages: {avg_gap:.1f} minutes")
        print(f"Maximum gap: {max_gap:.1f} minutes ({max_gap/60:.1f} hours)")
        print(f"Minimum gap: {min_gap:.1f} minutes")
        
        # Gap distribution
        immediate = sum(1 for gap in all_gaps if gap < 1)  # < 1 minute
        quick = sum(1 for gap in all_gaps if 1 <= gap < 5)  # 1-5 minutes
        moderate = sum(1 for gap in all_gaps if 5 <= gap < 30)  # 5-30 minutes
        slow = sum(1 for gap in all_gaps if 30 <= gap < 120)  # 30 min - 2 hours
        
        print(f"Gap distribution:")
        print(f"  Immediate (<1 min): {immediate} ({immediate/len(all_gaps)*100:.1f}%)")
        print(f"  Quick (1-5 min): {quick} ({quick/len(all_gaps)*100:.1f}%)")
        print(f"  Moderate (5-30 min): {moderate} ({moderate/len(all_gaps)*100:.1f}%)")
        print(f"  Slow (30 min-2h): {slow} ({slow/len(all_gaps)*100:.1f}%)")
    
    # Participant analysis
    print(f"\n=== PARTICIPANT ANALYSIS ===")
    all_participants = []
    for seg in segments:
        all_participants.extend(seg['participants'])
    
    participant_counts = Counter(all_participants)
    print(f"Unique participants: {len(participant_counts)}")
    for participant, count in participant_counts.most_common():
        print(f"  {participant}: {count} segments")
    
    # Solo vs group conversations
    solo_conversations = sum(1 for seg in segments if len(seg['participants']) == 1)
    group_conversations = len(segments) - solo_conversations
    
    print(f"\nConversation types:")
    print(f"  Solo conversations: {solo_conversations} ({solo_conversations/len(segments)*100:.1f}%)")
    print(f"  Group conversations: {group_conversations} ({group_conversations/len(segments)*100:.1f}%)")
    
    # Daily patterns
    print(f"\n=== DAILY PATTERNS ===")
    date_stats = defaultdict(lambda: {'segments': 0, 'messages': 0, 'duration': 0})
    
    for seg in segments:
        date_stats[seg['date']]['segments'] += 1
        date_stats[seg['date']]['messages'] += seg['message_count']
        date_stats[seg['date']]['duration'] += seg['total_duration_minutes']
    
    # Top 10 most active dates
    top_dates = sorted(date_stats.items(), key=lambda x: x[1]['messages'], reverse=True)[:10]
    print(f"Top 10 most active dates:")
    for date, stats in top_dates:
        duration_hours = stats['duration'] / 60
        print(f"  {date}: {stats['segments']} segments, {stats['messages']} messages ({duration_hours:.1f} hours)")
    
    # Conversation flow analysis
    print(f"\n=== CONVERSATION FLOW ANALYSIS ===")
    
    # Segments that span multiple hours
    long_segments = [seg for seg in segments if seg['total_duration_minutes'] > 120]  # > 2 hours
    print(f"Long conversations (>2 hours): {len(long_segments)} ({len(long_segments)/len(segments)*100:.1f}%)")
    
    # Quick exchanges
    quick_exchanges = [seg for seg in segments if seg['total_duration_minutes'] < 10]  # < 10 minutes
    print(f"Quick exchanges (<10 minutes): {len(quick_exchanges)} ({len(quick_exchanges)/len(segments)*100:.1f}%)")
    
    # Average conversation length by participant
    print(f"\nAverage conversation length by participant:")
    participant_stats = defaultdict(lambda: {'segments': 0, 'total_messages': 0, 'total_duration': 0})
    
    for seg in segments:
        for participant in seg['participants']:
            participant_stats[participant]['segments'] += 1
            participant_stats[participant]['total_messages'] += seg['message_count']
            participant_stats[participant]['total_duration'] += seg['total_duration_minutes']
    
    for participant, stats in participant_stats.items():
        avg_messages = stats['total_messages'] / stats['segments']
        avg_duration = stats['total_duration'] / stats['segments']
        print(f"  {participant}: {avg_messages:.1f} messages, {avg_duration:.1f} minutes per segment")
    
    # Time of day analysis
    print(f"\n=== TIME OF DAY ANALYSIS ===")
    hour_counts = defaultdict(int)
    
    for seg in segments:
        try:
            start_time = datetime.fromisoformat(seg['start_time'].replace('Z', '+00:00'))
            hour = start_time.hour
            hour_counts[hour] += 1
        except:
            continue
    
    print(f"Conversation start times by hour (UTC):")
    for hour in sorted(hour_counts.keys()):
        count = hour_counts[hour]
        print(f"  {hour:02d}:00: {count} conversations ({count/len(segments)*100:.1f}%)")
    
    # Weekend vs weekday analysis
    print(f"\n=== WEEKEND VS WEEKDAY ANALYSIS ===")
    weekday_count = 0
    weekend_count = 0
    
    for seg in segments:
        try:
            date_obj = datetime.strptime(seg['date'], '%Y-%m-%d')
            if date_obj.weekday() < 5:  # Monday = 0, Sunday = 6
                weekday_count += 1
            else:
                weekend_count += 1
        except:
            continue
    
    total_dated = weekday_count + weekend_count
    if total_dated > 0:
        print(f"Weekday conversations: {weekday_count} ({weekday_count/total_dated*100:.1f}%)")
        print(f"Weekend conversations: {weekend_count} ({weekend_count/total_dated*100:.1f}%)")


def main():
    if len(sys.argv) != 2:
        print("Usage: python analyze_segments.py <segmented_file.jsonl>")
        print("Example: python analyze_segments.py +19178268897_segmented.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    analyze_segments(input_file)


if __name__ == "__main__":
    main()
