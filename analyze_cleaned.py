#!/usr/bin/env python3
"""
Analysis script for cleaned JSONL data.
Shows statistics about extracted features, emojis, URLs, and message characteristics.
"""

import json
import sys
from collections import Counter


def analyze_cleaned_data(input_file):
    """Analyze the cleaned data and show feature statistics."""
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        return
    
    print(f"Analyzing {len(lines)} cleaned records...")
    
    # Initialize counters
    total_emojis = 0
    total_urls = 0
    questions = 0
    exclamations = 0
    dates_detected = 0
    places_detected = 0
    money_detected = 0
    mentions_found = 0
    
    # Feature distributions
    token_counts = []
    character_counts = []
    emoji_counts = []
    url_counts = []
    
    # Emoji and URL collections
    all_emojis = []
    all_urls = []
    all_mentions = []
    
    # Process each record
    for line in lines:
        try:
            record = json.loads(line.strip())
            features = record.get('features', {})
            extracted = record.get('extracted_data', {})
            
            # Count features
            if features.get('is_question'):
                questions += 1
            if features.get('is_exclamation'):
                exclamations += 1
            if features.get('contains_date'):
                dates_detected += 1
            if features.get('contains_place'):
                places_detected += 1
            if features.get('contains_money'):
                money_detected += 1
            
            # Collect counts
            token_counts.append(features.get('token_count', 0))
            character_counts.append(features.get('character_count', 0))
            emoji_counts.append(features.get('emoji_count', 0))
            url_counts.append(features.get('url_count', 0))
            
            # Collect emojis and URLs
            emojis = extracted.get('emojis', [])
            urls = extracted.get('urls', [])
            mentions = features.get('mentions', [])
            
            all_emojis.extend(emojis)
            all_urls.extend(urls)
            all_mentions.extend(mentions)
            
            total_emojis += len(emojis)
            total_urls += len(urls)
            mentions_found += len(mentions)
            
        except json.JSONDecodeError:
            continue
    
    # Print analysis results
    print(f"\n=== CLEANED DATA STATISTICS ===")
    print(f"Total records: {len(lines):,}")
    
    print(f"\n=== MESSAGE FEATURES ===")
    print(f"Questions detected: {questions:,} ({questions/len(lines)*100:.1f}%)")
    print(f"Exclamations detected: {exclamations:,} ({exclamations/len(lines)*100:.1f}%)")
    print(f"Date references: {dates_detected:,} ({dates_detected/len(lines)*100:.1f}%)")
    print(f"Place references: {places_detected:,} ({places_detected/len(lines)*100:.1f}%)")
    print(f"Money references: {money_detected:,} ({money_detected/len(lines)*100:.1f}%)")
    print(f"Total mentions (@): {mentions_found:,}")
    
    print(f"\n=== CONTENT STATISTICS ===")
    print(f"Total emojis: {total_emojis:,}")
    print(f"Total URLs: {total_urls:,}")
    print(f"Average tokens per message: {sum(token_counts)/len(token_counts):.1f}")
    print(f"Average characters per message: {sum(character_counts)/len(character_counts):.1f}")
    print(f"Average emojis per message: {sum(emoji_counts)/len(emoji_counts):.2f}")
    print(f"Average URLs per message: {sum(url_counts)/len(url_counts):.2f}")
    
    print(f"\n=== TOP EMOJIS ===")
    emoji_counter = Counter(all_emojis)
    for emoji_char, count in emoji_counter.most_common(10):
        print(f"  {emoji_char}: {count:,} times")
    
    print(f"\n=== TOP URLS ===")
    url_counter = Counter(all_urls)
    for url, count in url_counter.most_common(5):
        print(f"  {url}: {count:,} times")
    
    print(f"\n=== TOP MENTIONS ===")
    mention_counter = Counter(all_mentions)
    for mention, count in mention_counter.most_common(5):
        print(f"  @{mention}: {count:,} times")
    
    print(f"\n=== FEATURE DISTRIBUTIONS ===")
    
    # Token count distribution
    short_msgs = sum(1 for t in token_counts if t <= 5)
    medium_msgs = sum(1 for t in token_counts if 6 <= t <= 20)
    long_msgs = sum(1 for t in token_counts if t > 20)
    
    print(f"Token count distribution:")
    print(f"  Short (≤5 words): {short_msgs:,} ({short_msgs/len(token_counts)*100:.1f}%)")
    print(f"  Medium (6-20 words): {medium_msgs:,} ({medium_msgs/len(token_counts)*100:.1f}%)")
    print(f"  Long (>20 words): {long_msgs:,} ({long_msgs/len(token_counts)*100:.1f}%)")
    
    # Emoji usage distribution
    no_emojis = sum(1 for e in emoji_counts if e == 0)
    with_emojis = sum(1 for e in emoji_counts if e > 0)
    
    print(f"Emoji usage:")
    print(f"  No emojis: {no_emojis:,} ({no_emojis/len(emoji_counts)*100:.1f}%)")
    print(f"  With emojis: {with_emojis:,} ({with_emojis/len(emoji_counts)*100:.1f}%)")
    
    # URL usage distribution
    no_urls = sum(1 for u in url_counts if u == 0)
    with_urls = sum(1 for u in url_counts if u > 0)
    
    print(f"URL usage:")
    print(f"  No URLs: {no_urls:,} ({no_urls/len(url_counts)*100:.1f}%)")
    print(f"  With URLs: {with_urls:,} ({with_urls/len(url_counts)*100:.1f}%)")


def main():
    if len(sys.argv) != 2:
        print("Usage: python analyze_cleaned.py <cleaned_file.jsonl>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    analyze_cleaned_data(input_file)


if __name__ == "__main__":
    main()
