from datasets import load_dataset
import json

# Load dataset
ds = load_dataset("Dans-DiscountModels/ConversationChronicles-sharegpt")

# Take train split
data = ds["train"].select(range(100000))

# Convert to list of dicts
records = []

for item in data:
    records.append({
        "id": item["id"],
        "conversations": item["conversations"]
    })

# Save locally
with open("data/raw.json", "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

print("✅ Dataset saved to data/raw.json")