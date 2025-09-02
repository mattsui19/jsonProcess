#!/usr/bin/env python3
"""
Data analysis script for normalized JSONL output.
Provides insights and statistics about the message data in JSON format.
"""

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
import re


def parse_readtime(readtime_str):
    """Parse readtime string and return timezone-aware datetime object."""
    if not readtime_str or readtime_str == "null":
        return None
    
    try:
        # Parse format like "Feb 28, 2025  6:32:51 AM"
        dt = datetime.strptime(readtime_str, "%b %d, %Y  %I:%M:%S %p")
        # Make it timezone-aware (assuming local time, convert to UTC)
        dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def parse_timestamp_iso(timestamp_str):
    """Parse ISO timestamp string and return timezone-aware datetime object."""
    if not timestamp_str:
        return None
    
    try:
        # Parse format like "2025-02-28T06:32:21Z"
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt
    except ValueError:
        return None


def analyze_readtime_distribution(readtimes, label):
    """Analyze readtime distribution for a specific sender type and return structured data."""
    if not readtimes:
        return {
            "label": label,
            "count": 0,
            "message": "No readtime data available"
        }
    
    # Calculate statistics
    avg_readtime = sum(readtimes) / len(readtimes)
    min_readtime = min(readtimes)
    max_readtime = max(readtimes)
    median_readtime = sorted(readtimes)[len(readtimes)//2]
    
    # Categorize readtimes
    immediate = sum(1 for t in readtimes if t < 1)  # < 1 minute
    quick = sum(1 for t in readtimes if 1 <= t < 5)  # 1-5 minutes
    moderate = sum(1 for t in readtimes if 5 <= t < 30)  # 5-30 minutes
    slow = sum(1 for t in readtimes if 30 <= t < 120)  # 30 min - 2 hours
    very_slow = sum(1 for t in readtimes if t >= 120)  # 2+ hours
    
    return {
        "label": label,
        "count": len(readtimes),
        "statistics": {
            "average_minutes": round(avg_readtime, 1),
            "median_minutes": round(median_readtime, 1),
            "min_minutes": round(min_readtime, 1),
            "max_minutes": round(max_readtime, 1)
        },
        "distribution": {
            "immediate_under_1min": {
                "count": immediate,
                "percentage": round(immediate/len(readtimes)*100, 1)
            },
            "quick_1_to_5min": {
                "count": quick,
                "percentage": round(quick/len(readtimes)*100, 1)
            },
            "moderate_5_to_30min": {
                "count": moderate,
                "percentage": round(moderate/len(readtimes)*100, 1)
            },
            "slow_30min_to_2h": {
                "count": slow,
                "percentage": round(slow/len(readtimes)*100, 1)
            },
            "very_slow_over_2h": {
                "count": very_slow,
                "percentage": round(very_slow/len(readtimes)*100, 1)
            }
        }
    }


def analyze_data(input_file: str):
    """Analyze the normalized data and return structured results."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        return {"error": f"File '{input_file}' not found"}
    
    # Initialize counters
    total_chars = 0
    total_words = 0
    attachment_types = Counter()
    message_lengths = []
    date_distribution = Counter()
    sender_distribution = Counter()
    
    # Readtime analysis
    all_readtimes = []
    me_readtimes = []
    phone_readtimes = []
    
    # Process each record
    for line in lines:
        try:
            record = json.loads(line.strip())
            
            # Count characters and words
            contents = record.get('contents', '')
            if contents:
                total_chars += len(contents)
                total_words += len(contents.split())
                message_lengths.append(len(contents))
            
            # Count attachments by type
            attachments = record.get('attachments', [])
            for attachment in attachments:
                mime_type = attachment.get('mime_type', 'unknown')
                attachment_types[mime_type] += 1
            
            # Count by date
            timestamp = record.get('timestamp', '')
            if timestamp:
                try:
                    date = timestamp[:10]  # Extract date part
                    date_distribution[date] += 1
                except Exception:
                    pass
            
            # Count sender types and collect readtime data
            if 'me' in record:
                sender_distribution['me'] += 1
                # For "me" messages, check if they have readtime
                readtime = record.get('readtime')
                if readtime and readtime != "null":
                    readtime_dt = parse_readtime(readtime)
                    if readtime_dt and timestamp:
                        try:
                            message_dt = parse_timestamp_iso(timestamp)
                            if message_dt:
                                # Calculate time difference in minutes
                                time_diff = (readtime_dt - message_dt).total_seconds() / 60
                                if time_diff >= 0:  # Only positive readtimes
                                    me_readtimes.append(time_diff)
                                    all_readtimes.append(time_diff)
                        except Exception:
                            pass
                            
            elif 'phone' in record:
                sender_distribution['phone'] += 1
                # For phone messages, check if they have readtime
                readtime = record.get('readtime')
                if readtime and readtime != "null":
                    readtime_dt = parse_readtime(readtime)
                    if readtime_dt and timestamp:
                        try:
                            message_dt = parse_timestamp_iso(timestamp)
                            if message_dt:
                                # Calculate time difference in minutes
                                time_diff = (readtime_dt - message_dt).total_seconds() / 60
                                if time_diff >= 0:  # Only positive readtimes
                                    phone_readtimes.append(time_diff)
                                    all_readtimes.append(time_diff)
                        except Exception:
                            pass
        except json.JSONDecodeError:
            continue
    
    # Build results structure
    results = {
        "analysis_metadata": {
            "input_file": input_file,
            "total_records": len(lines),
            "analysis_timestamp": datetime.now().isoformat()
        },
        "message_statistics": {
            "total_messages": len(lines),
            "total_characters": total_chars,
            "total_words": total_words,
            "average_message_length": round(total_chars / len(lines), 1),
            "average_words_per_message": round(total_words / len(lines), 1)
        },
        "sender_distribution": {
            sender_type: {
                "count": count,
                "percentage": round((count / len(lines)) * 100, 1)
            }
            for sender_type, count in sender_distribution.items()
        },
        "readtime_analysis": {
            "overall": {
                "total_messages_with_readtime": len(all_readtimes),
                "statistics": {
                    "average_minutes": round(sum(all_readtimes) / len(all_readtimes), 1) if all_readtimes else 0,
                    "median_minutes": round(sorted(all_readtimes)[len(all_readtimes)//2], 1) if all_readtimes else 0,
                    "min_minutes": round(min(all_readtimes), 1) if all_readtimes else 0,
                    "max_minutes": round(max(all_readtimes), 1) if all_readtimes else 0
                }
            } if all_readtimes else {"total_messages_with_readtime": 0},
            "individual_distributions": [
                analyze_readtime_distribution(me_readtimes, "'Me' messages"),
                analyze_readtime_distribution(phone_readtimes, "Phone messages")
            ]
        },
        "response_time_comparison": {}
    }
    
    # Add response time comparison if both have data
    if me_readtimes and phone_readtimes:
        avg_me = sum(me_readtimes) / len(me_readtimes)
        avg_phone = sum(phone_readtimes) / len(phone_readtimes)
        
        results["response_time_comparison"] = {
            "me_average_minutes": round(avg_me, 1),
            "phone_average_minutes": round(avg_phone, 1),
            "comparison": {
                "me_vs_phone_ratio": round(avg_me / avg_phone, 1) if avg_phone > 0 else 0,
                "phone_vs_me_ratio": round(avg_phone / avg_me, 1) if avg_me > 0 else 0,
                "faster_responder": "phone" if avg_phone < avg_me else "me",
                "speed_difference_factor": round(max(avg_me, avg_phone) / min(avg_me, avg_phone), 1) if min(avg_me, avg_phone) > 0 else 0
            }
        }
    
    # Add overall readtime distribution if data exists
    if all_readtimes:
        immediate = sum(1 for t in all_readtimes if t < 1)
        quick = sum(1 for t in all_readtimes if 1 <= t < 5)
        moderate = sum(1 for t in all_readtimes if 5 <= t < 30)
        slow = sum(1 for t in all_readtimes if 30 <= t < 120)
        very_slow = sum(1 for t in all_readtimes if t >= 120)
        
        results["readtime_analysis"]["overall"]["distribution"] = {
            "immediate_under_1min": {
                "count": immediate,
                "percentage": round(immediate/len(all_readtimes)*100, 1)
            },
            "quick_1_to_5min": {
                "count": quick,
                "percentage": round(quick/len(all_readtimes)*100, 1)
            },
            "moderate_5_to_30min": {
                "count": moderate,
                "percentage": round(moderate/len(all_readtimes)*100, 1)
            },
            "slow_30min_to_2h": {
                "count": slow,
                "percentage": round(slow/len(all_readtimes)*100, 1)
            },
            "very_slow_over_2h": {
                "count": very_slow,
                "percentage": round(very_slow/len(all_readtimes)*100, 1)
            }
        }
    
    # Add attachment analysis
    total_attachments = sum(attachment_types.values())
    results["attachment_analysis"] = {
        "total_attachments": total_attachments,
        "types": [
            {
                "mime_type": mime_type,
                "count": count,
                "percentage": round((count / total_attachments) * 100, 1) if total_attachments > 0 else 0
            }
            for mime_type, count in attachment_types.most_common()
        ]
    }
    
    # Add date distribution
    results["date_distribution"] = {
        "top_dates": [
            {
                "date": date,
                "message_count": count
            }
            for date, count in date_distribution.most_common(10)
        ]
    }
    
    # Add message length analysis
    if message_lengths:
        message_lengths.sort()
        results["message_length_analysis"] = {
            "shortest_message": min(message_lengths),
            "longest_message": max(message_lengths),
            "median_length": message_lengths[len(message_lengths)//2],
            "empty_messages": {
                "count": sum(1 for length in message_lengths if length == 0),
                "percentage": round(sum(1 for length in message_lengths if length == 0) / len(message_lengths) * 100, 1)
            }
        }
    
    # Add content analysis
    emoji_pattern = re.compile(r'[^\w\s]')
    emoji_count = 0
    for line in lines:
        try:
            record = json.loads(line.strip())
            contents = record.get('contents', '')
            emojis = emoji_pattern.findall(contents)
            emoji_count += len(emojis)
        except:
            continue
    
    long_messages = [length for length in message_lengths if length > 200]
    results["content_analysis"] = {
        "special_characters_emojis": {
            "total_count": emoji_count,
            "average_per_message": round(emoji_count / len(lines), 1)
        },
        "long_messages_over_200_chars": {
            "count": len(long_messages),
            "percentage": round(len(long_messages) / len(message_lengths) * 100, 1) if message_lengths else 0
        }
    }
    
    return results


def main():
    if len(sys.argv) != 2:
        print(json.dumps({"error": "Usage: python analyze_data.py <normalized_file.jsonl>"}))
        sys.exit(1)
    
    input_file = sys.argv[1]
    results = analyze_data(input_file)
    
    # Output as formatted JSON
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
