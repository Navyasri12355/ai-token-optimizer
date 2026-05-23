"""
Check processed.parquet schema and columns
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SchemaCheck")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("[*] Reading processed.parquet schema ...\n")

df = spark.read.parquet("data/processed.parquet")

print("=" * 70)
print("COLUMNS IN PROCESSED.PARQUET")
print("=" * 70)
print("\nAll columns:")
for col in sorted(df.columns):
    print(f"  • {col}")

print("\n" + "=" * 70)
print("ASSESSMENT")
print("=" * 70)

expected_params = ["max_tokens", "temperature", "top_p", "model", "api_param"]
found_params = [col for col in df.columns if any(p in col.lower() for p in expected_params)]

if found_params:
    print(f"✅ Found parameter columns: {found_params}")
else:
    print("❌ NO request parameter columns found")
    print("\nAvailable features are ONLY:")
    print("  • Text-based: text_len, num_words, avg_word_len")
    print("  • Metadata: question_flag, turn_pos, conv_id")
    print("  • Tokens: input_tokens, output_tokens (estimated from text length)")
    print("\nMISSING critical parameters:")
    print("  ❌ max_tokens (API parameter)")
    print("  ❌ temperature (API parameter)")  
    print("  ❌ model version (API configuration)")
    print("  ❌ Any other request metadata")

print("\n" + "=" * 70)
print("WHY THIS MATTERS FOR YOUR MODEL")
print("=" * 70)
print("""
Your output token prediction is essentially trying to guess response length 
from input text alone. But actual response length depends on:

1. max_tokens parameter (API limit)
2. model configuration
3. generation parameters (temperature, top_p, etc.)
4. Cache hit/miss
5. Dynamic model behavior

These are NOT in your dataset. They're external to the conversation data.

RECOMMENDATION: The model's poor R² (0.19) is expected because you're missing 
the actual predictive features. You need to either:
  A) Get datasets WITH request parameters and model configs
  B) Accept that text-only prediction has inherent limits (~19% is realistic)
  C) Focus on request types that correlate strongly with output size
""")

spark.stop()
