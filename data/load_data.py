from datasets import load_dataset
import json
import os
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "raw.json"

# Load .env if available and read HF_ACCESS_TOKEN
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # python-dotenv not installed; rely on environment variables
    pass

# Hugging Face access token (set HF_ACCESS_TOKEN in .env or environment)
HF_TOKEN = os.getenv("HF_ACCESS_TOKEN")

records = []


def add_sharegpt(items, source=""):
    """Append records that already follow {id, conversations:[{from,value}]}."""
    for item in items:
        cid = item.get("id") or item.get("question_id") or f"{source}_{len(records)}"
        convs = item.get("conversations")
        if convs and isinstance(convs, list):
            records.append({"id": str(cid), "conversations": convs})


# ---------------------------------------------------------------------------
# 1. Dans-DiscountModels/ConversationChronicles-sharegpt  (original dataset)
# ---------------------------------------------------------------------------
print("Loading ConversationChronicles-sharegpt ...")
try:
    if HF_TOKEN:
        print("Using HF_ACCESS_TOKEN from environment/.env")
        ds = load_dataset("Dans-DiscountModels/ConversationChronicles-sharegpt", use_auth_token=HF_TOKEN)
    else:
        ds = load_dataset("Dans-DiscountModels/ConversationChronicles-sharegpt")
    data = ds["train"]
    for item in data:
        records.append({"id": item["id"], "conversations": item["conversations"]})
    print(f"  → {len(records)} records so far")
except Exception as e:
    print(f"  ✗ Failed: {e}")


# ---------------------------------------------------------------------------
# 2. Arist12/EABF-ShareGPT-Long-3.5k
#    Schema: {id, model, conversations:[{from, value}]}
# ---------------------------------------------------------------------------
print("Loading EABF-ShareGPT-Long-3.5k ...")
try:
    ds = load_dataset("Arist12/EABF-ShareGPT-Long-3.5k")
    split = ds["train"]
    before = len(records)
    add_sharegpt(split, source="eabf")
    print(f"  → added {len(records) - before} records  (total {len(records)})")
except Exception as e:
    print(f"  ✗ Failed: {e}")


# ---------------------------------------------------------------------------
# 3. DSULT-Core/ShareGPT-X
#    Schema: {id, length, $modelId, language, conversations:[{from, value}]}
#    Use the cleaner ShareGPT-flavour file.
# ---------------------------------------------------------------------------
print("Loading ShareGPT-X ...")
try:
    ds = load_dataset(
        "DSULT-Core/ShareGPT-X",
        data_files={"train": "ChatGPT-Simple_ShareGPT_Full.json"},
    )
    split = ds["train"]
    before = len(records)
    add_sharegpt(split, source="sharegpt_x")
    print(f"  → added {len(records) - before} records  (total {len(records)})")
except Exception as e:
    print(f"  ✗ Failed: {e}")


# ---------------------------------------------------------------------------
# 4. lmsys/chatbot_arena_conversations
#    Schema: {question_id, model_a, model_b,
#             conversation_a:[{role,content}],
#             conversation_b:[{role,content}], ...}
#    Normalize: role="user"→from="human", role="assistant"→from="gpt"
#    NOTE: requires HF login + accepting dataset terms.
# ---------------------------------------------------------------------------
print("Loading chatbot_arena_conversations ...")
try:
    ds = load_dataset("lmsys/chatbot_arena_conversations")
    split = ds["train"]
    before = len(records)

    def openai_to_sharegpt(turns):
        result = []
        for t in turns:
            role = t.get("role", "")
            content = t.get("content", "")
            result.append({
                "from": "human" if role == "user" else "gpt",
                "value": content,
            })
        return result

    for item in split:
        qid = item.get("question_id", f"arena_{len(records)}")
        for suffix, conv_key in [("a", "conversation_a"), ("b", "conversation_b")]:
            raw_turns = item.get(conv_key, [])
            if raw_turns:
                convs = openai_to_sharegpt(raw_turns)
                records.append({"id": f"{qid}_{suffix}", "conversations": convs})

    print(f"  → added {len(records) - before} records  (total {len(records)})")
except Exception as e:
    print(f"  ✗ Failed (may need HF login + accepted terms): {e}")


# ---------------------------------------------------------------------------
# 5. OpenAssistant/oasst1
#    Schema (flat): {message_id, parent_id, message_tree_id, text, role, ...}
#    Reconstruct linear conversations: root → first child at each level.
# ---------------------------------------------------------------------------
print("Loading OpenAssistant/oasst1 ...")
try:
    ds = load_dataset("OpenAssistant/oasst1")
    split = ds["train"]
    before = len(records)

    # Build parent→children map (keep only non-deleted messages)
    children = defaultdict(list)
    msg_map = {}
    for item in split:
        mid = item["message_id"]
        msg_map[mid] = item
        pid = item.get("parent_id")
        if pid:
            children[pid].append(mid)

    # Find root messages (no parent_id)
    roots = [m for m in msg_map.values() if not m.get("parent_id")]

    def build_conversation(node_id):
        """Walk the first child at each depth to produce a linear conversation."""
        turns = []
        current = node_id
        while current:
            msg = msg_map[current]
            role = msg.get("role", "")
            text = msg.get("text", "")
            turns.append({
                "from": "human" if role == "prompter" else "gpt",
                "value": text,
            })
            kids = children.get(current, [])
            current = kids[0] if kids else None
        return turns

    for root in roots:
        tree_id = root["message_tree_id"]
        convs = build_conversation(root["message_id"])
        if len(convs) >= 2:          # skip single-message stubs
            records.append({"id": f"oasst1_{tree_id}", "conversations": convs})

    print(f"  → added {len(records) - before} records  (total {len(records)})")
except Exception as e:
    print(f"  ✗ Failed: {e}")


# ---------------------------------------------------------------------------
# 6. Alignment-Lab-AI/Maverick-sharegpt-3m
#    Replaces RyokoAI/ShareGPT52K which has broken mixed-type JSON files.
#    Schema: {id, conversations:[{from, value}]}  — standard ShareGPT
#    Size: 2,830,405 rows / 2.02 GB  (clean Parquet format)
# ---------------------------------------------------------------------------
print("Loading Maverick-sharegpt-3m ...")
try:
    ds = load_dataset("Alignment-Lab-AI/Maverick-sharegpt-3m")
    split = ds["train"]
    before = len(records)
    add_sharegpt(split, source="maverick")
    print(f"  → added {len(records) - before} records  (total {len(records)})")
except Exception as e:
    print(f"  ✗ Failed: {e}")


# ---------------------------------------------------------------------------
# NOTE: badlogicgames/pi-mono is an agent-trace dataset (JSONL coding sessions)
# with a completely different schema and is intentionally skipped here.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
print(f"\nTotal records collected: {len(records)}")
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

print(f"✅ Dataset saved to {OUTPUT_PATH}")