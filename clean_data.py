#!/usr/bin/env python3
"""
Simple wrapper script for cleaning JSON data.
Automatically detects input format and processes accordingly.
"""

import sys
import os
from process_original import process_original_file


def main():
    if len(sys.argv) != 2:
        print("Usage: python clean_data.py <input_file>")
        print("The script will automatically:")
        print("  1. Detect if it's original JSON or normalized JSONL")
        print("  2. Process and clean the data")
        print("  3. Output cleaned JSONL with features")
        print("\nExample: python clean_data.py +19178268897.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    
    # Generate output filename
    base_name = os.path.splitext(input_file)[0]
    output_file = f"{base_name}_cleaned.jsonl"
    
    print(f"🚀 Starting data cleaning process...")
    print(f"📁 Input: {input_file}")
    print(f"📤 Output: {output_file}")
    print(f"⏳ Processing...")
    
    # Process the file
    process_original_file(input_file, output_file)
    
    print(f"\n✅ Cleaning complete!")
    print(f"📊 Output saved to: {output_file}")
    print(f"🔍 You can now analyze the cleaned data with:")
    print(f"   python analyze_cleaned.py {output_file}")


if __name__ == "__main__":
    main()
