"""
Quick sample inspection of raw.json (efficient version)
"""

import json
from pathlib import Path
import ijson

RAW_JSON = Path("data/raw.json")

if not RAW_JSON.exists():
    print(f"❌ {RAW_JSON} not found")
    exit(1)

print(f"[*] Sampling records from {RAW_JSON} ...")

# Use ijson to stream instead of loading entire file
all_keys = set()
param_keywords = ["model", "temperature", "max_tokens", "top_p", "frequency_penalty", 
                  "presence_penalty", "param", "config", "setting", "api"]
found_params = {}
count = 0
sample_records = []

with open(RAW_JSON, "rb") as f:
    for record in ijson.items(f, "item"):
        count += 1
        all_keys.update(record.keys())
        
        # Sample first 3 records
        if len(sample_records) < 3:
            sample_records.append(record)
        
        # Check for params
        for key in record.keys():
            key_lower = key.lower()
            for param in param_keywords:
                if param in key_lower:
                    found_params[key] = found_params.get(key, 0) + 1
        
        # Show progress
        if count % 100000 == 0:
            print(f"  [{count:,} records scanned]", flush=True)

print(f"\nTotal records scanned: {count:,}\n")

print("=" * 70)
print("SAMPLE RECORD STRUCTURES (first 3)")
print("=" * 70)

for i, record in enumerate(sample_records):
    print(f"\nRecord {i}:")
    print(f"  Top-level keys: {list(record.keys())}")
    
    if "conversations" in record:
        convs = record["conversations"]
        print(f"  Conversation turns: {len(convs)}")
        if len(convs) > 0:
            print(f"    Turn 0: {list(convs[0].keys())} = {str(convs[0])[:80]}")

print("\n" + "=" * 70)
print("ALL UNIQUE TOP-LEVEL KEYS")
print("=" * 70)
for key in sorted(all_keys):
    print(f"  • {key}")

print("\n" + "=" * 70)
print("REQUEST PARAMETERS CHECK")
print("=" * 70)
if found_params:
    print("❌ Parameter-like keys found:")
    for key, cnt in sorted(found_params.items(), key=lambda x: x[1], reverse=True):
        print(f"  {key:.<40} {cnt:>6,} records")
else:
    print("❌ NO request parameters found in raw.json")
    print("\nData structure is: {id, conversations: [{from, value}, ...]}")
    print("Available in preprocessing:")
    print("  • text_len (prompt character count)")
    print("  • num_words / avg_word_len")
    print("  • question_flag (contains question keywords)")
    print("  • turn_pos (conversation turn number)")
    print("  • input_tokens / output_tokens (estimated from char count)")
    print("\n❌ MISSING:")
    print("  • max_tokens parameter")
    print("  • temperature")
    print("  • model version/name")
    print("  • top_p, frequency_penalty, etc.")
    print("  • cache status")
    print("  • API configuration")
