"""
Inspect raw.json structure to see if request parameters are available
"""

import json
from pathlib import Path

RAW_JSON = Path("data/raw.json")

if not RAW_JSON.exists():
    print(f"❌ {RAW_JSON} not found")
    exit(1)

print(f"[*] Reading first 100 records from {RAW_JSON} ...")

with open(RAW_JSON, "r", encoding="utf-8") as f:
    all_records = json.load(f)

print(f"Total records: {len(all_records)}\n")

# Inspect structure of first few records
print("=" * 70)
print("SAMPLE RECORD STRUCTURES")
print("=" * 70)

for i in range(min(3, len(all_records))):
    record = all_records[i]
    print(f"\nRecord {i}:")
    print(f"  Keys: {list(record.keys())}")
    
    # Show first conversation turn
    if "conversations" in record:
        convs = record["conversations"]
        print(f"  Conversation turns: {len(convs)}")
        if len(convs) > 0:
            print(f"    Turn 0 keys: {list(convs[0].keys())}")
            print(f"    Turn 0 from: {convs[0].get('from')}")
            print(f"    Turn 0 value (first 100 chars): {convs[0].get('value', '')[:100]}")

# Check all unique top-level keys across all records
print("\n" + "=" * 70)
print("ALL UNIQUE TOP-LEVEL KEYS ACROSS ALL RECORDS")
print("=" * 70)
all_keys = set()
for record in all_records:
    all_keys.update(record.keys())

for key in sorted(all_keys):
    count = sum(1 for r in all_records if key in r)
    print(f"  {key:.<40} {count:>6,} records have this key")

# Check if any records have parameters like model, temperature, max_tokens
print("\n" + "=" * 70)
print("CHECKING FOR REQUEST PARAMETERS")
print("=" * 70)

param_keywords = ["model", "temperature", "max_tokens", "top_p", "frequency_penalty", 
                  "presence_penalty", "param", "config", "setting", "api"]

found_params = {}
for record in all_records:
    for key in record.keys():
        key_lower = key.lower()
        for param in param_keywords:
            if param in key_lower:
                found_params[key] = found_params.get(key, 0) + 1

if found_params:
    print("Found parameter-like keys:")
    for key, count in sorted(found_params.items(), key=lambda x: x[1], reverse=True):
        print(f"  {key:.<40} {count:>6,} records")
else:
    print("❌ NO request parameters found in raw.json")
    print("   Data structure: {id, conversations: [{from, value}, ...]}")
    print("   No metadata about model parameters, temperature, max_tokens, etc.")
