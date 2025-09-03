#!/usr/bin/env python3
"""
GPT-5 Integration Script for Conversation Summarization
Generates 3-sentence summaries for conversation segments using OpenAI's GPT-4.
"""

import json
import sys
import os
import time
from datetime import datetime
import openai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
GPT_MODEL = "gpt-5-mini"  # Use GPT-4o (latest available model)
MAX_TOKENS = 1000  # Reduced from 100000 to stay within GPT-4o's 16,384 limit
TEMPERATURE = 0.7
DEFAULT_MAX_SEGMENTS = 3


def format_segment_for_gpt(segment):
    """Format a segment into a prompt for GPT-4."""
    
    # Extract key information
    date = segment.get('date', 'Unknown date')
    start_time = segment.get('start_time', '')
    end_time = segment.get('end_time', '')
    message_count = segment.get('message_count', 0)
    participants = segment.get('participants', [])
    
    # Format timeframe
    if start_time and end_time:
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            # Format as readable time
            start_str = start_dt.strftime("%I:%M %p")
            end_str = end_dt.strftime("%I:%M %p")
            timeframe = f"{start_str} - {end_str}"
        except:
            timeframe = f"{start_time} to {end_time}"
    else:
        timeframe = "Unknown timeframe"
    
    # Extract message contents with sender information
    messages = segment.get('messages', [])
    message_details = []
    
    for msg in messages:
        content = msg.get('contents', '')
        is_from_me = msg.get('is_from_me', False)
        sender = "Me" if is_from_me else "Other person"
        message_details.append(f"- {sender}: {content}")
    
    # Build the prompt
    prompt = f"""Please provide a summary of this conversation segment:

Date: {date}
Timeframe: {timeframe}
Participants: {', '.join(str(p) for p in participants)}
Message Count: {message_count}

Conversation Content:
{chr(10).join(message_details)}

Please provide a short that summarize:
1. The main topic or purpose of this conversation
2. The key points or developments discussed
3. The outcome or conclusion of the conversation

Focus on the dynamic between the two participants and how the relationship between them.

Summary:"""

    return prompt


def generate_gpt_summary(segment, api_key=None, use_gpt=True):
    """Generate a GPT summary for a segment."""
    
    date = segment.get('date', 'Unknown date')
    start_time = segment.get('start_time', '')
    end_time = segment.get('end_time', '')
    
    # Format timeframe
    if start_time and end_time:
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            start_str = start_dt.strftime("%I:%M %p")
            end_str = end_dt.strftime("%I:%M %p")
            timeframe = f"{start_str} - {end_str}"
        except:
            timeframe = f"{start_time} to {end_time}"
    else:
        timeframe = "Unknown timeframe"
    
    if use_gpt and api_key:
        max_retries = 3
        retry_delay = 1  # Start with 1 second delay
        
        for attempt in range(max_retries):
            try:
                # Configure OpenAI client
                client = openai.OpenAI(api_key=api_key)
                
                # Generate prompt
                prompt = format_segment_for_gpt(segment)
                thread = client.beta.threads.create()
                                # Call GPT
                response = client.responses.create(
                    model=GPT_MODEL,
                    thread_id=thread.id,
                    input=(
                        "You are summarizes conversation segments "
                        "focusing on the dynamic between 'Me' and someone else. "
                        "If you've already figured out who the other person is, or me use their name."
                        + prompt
                    )
                )
                
                # Get the summary from response
                summary = getattr(response, "output_text", None)
                break  # Success, exit retry loop
                
            except openai.RateLimitError as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"⚠️  Rate limit hit, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"⚠️  Rate limit exceeded after {max_retries} attempts")
                    raise e
            except Exception as e:
                print(f"⚠️  GPT API error: {e}")
                print(f"   📝 Falling back to placeholder summary due to API error")
                # Fall back to placeholder summary
                summary = f"Conversation on {date} from {timeframe} involving {len(segment.get('participants', []))} participants. The exchange consisted of {segment.get('message_count', 0)} messages covering various topics. This appears to be a {'brief' if segment.get('message_count', 0) <= 5 else 'substantial'} conversation segment."
                break
    else:
        # Placeholder summary when GPT is not available
        summary = f"Conversation on {date} from {timeframe} involving {len(segment.get('participants', []))} participants. The exchange consisted of {segment.get('message_count', 0)} messages covering various topics. This appears to be a {'brief' if segment.get('message_count', 0) <= 5 else 'substantial'} conversation segment."
    
    return {
        "date": date,
        "timeframe": timeframe,
        "summary": summary,
        "segment_id": segment.get('segment_id', 'unknown'),
        "message_count": segment.get('message_count', 0),
        "participants": segment.get('participants', []),
        "gpt_generated": use_gpt and api_key is not None
    }


def process_segments(input_file, max_segments=None, api_key=None):
    """Process segments and generate summaries."""
    
    if max_segments is None:
        max_segments = DEFAULT_MAX_SEGMENTS
    
    if not os.path.exists(input_file):
        print(f"❌ Error: File '{input_file}' not found")
        return
    
    print(f"📖 Processing segments from: {input_file}")
    print(f"🎯 Generating summaries for first {max_segments} segments...")
    
    if api_key:
        print("🤖 Using GPT-4 for intelligent summaries")
        print(f"   Model: {GPT_MODEL}")
        print(f"   Max tokens: {MAX_TOKENS}")
        print(f"   Temperature: {TEMPERATURE}")
        print("   Focus: 'Me' vs 'Other person' dynamics")
    else:
        print("📝 Using placeholder summaries (set OPENAI_API_KEY in .env for GPT-4)")
        print("   💡 Tip: Check your .env file for the OPENAI_API_KEY")
    
    summaries = []
    segment_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if segment_count >= max_segments:
                break
                
            try:
                segment = json.loads(line.strip())
                segment_count += 1
                
                print(f"\n📅 Processing segment {segment_count}: {segment.get('date', 'Unknown')}")
                
                # Generate summary
                summary = generate_gpt_summary(segment, api_key, use_gpt=bool(api_key))
                summaries.append(summary)
                
                # Display the summary
                print(f"   📍 {summary['timeframe']}")
                print(f"   💬 {summary['summary']}")
                if summary['gpt_generated']:
                    print(f"   🤖 Generated by {GPT_MODEL}")
                else:
                    print(f"   📝 Placeholder summary")
                
            except json.JSONDecodeError as e:
                print(f"⚠️  Error parsing segment: {e}")
                continue
    
    print(f"\n✅ Generated {len(summaries)} summaries")
    
    # Save summaries to file
    output_file = input_file.replace('.jsonl', '_summaries.jsonl')
    with open(output_file, 'w', encoding='utf-8') as f:
        for summary in summaries:
            f.write(json.dumps(summary, ensure_ascii=False) + '\n')
    
    print(f"💾 Summaries saved to: {output_file}")
    
    return summaries


def main():
    if len(sys.argv) < 2:
        print("Usage: python gpt_summarizer.py <segmented_file.jsonl> [max_segments]")
        print("Example: python gpt_summarizer.py +19178268897_segmented.jsonl 5")
        print("\nThis script will:")
        print("  1. Read the segmented conversation file")
        print("  2. Generate 3-sentence summaries for the specified number of segments")
        print("  3. Use GPT-4 if OPENAI_API_KEY is set in .env, otherwise use placeholders")
        print("  4. Focus on 'Me' vs 'Other person' dynamics in summaries")
        print("  5. Output summaries with date, timeframe, and summary text")
        print("\nTo use GPT-4, set your OpenAI API key in .env file:")
        print("  OPENAI_API_KEY=sk-your-api-key-here")
        sys.exit(1)
    
    input_file = sys.argv[1]
    max_segments = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_SEGMENTS
    
    # Check for OpenAI API key from .env file
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        print("⚠️  No OPENAI_API_KEY found in .env file")
        print("   Will use placeholder summaries")
    
    process_segments(input_file, max_segments=max_segments, api_key=api_key)


if __name__ == "__main__":
    main()
