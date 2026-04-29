from pyspark.sql import SparkSession
import pandas as pd
import json
import tiktoken

# Start Spark (just for usage later)
spark = SparkSession.builder.appName("LLM").getOrCreate()

# Load JSON using Python (NOT Spark)
with open("data/raw.json", "r", encoding="utf-8") as f:
    data = json.load(f)


enc = tiktoken.encoding_for_model("gpt-3.5-turbo")

rows = []

for item in data:
    convo = item["conversations"]

    for i in range(len(convo) - 1):
        if convo[i]["from"] == "human" and convo[i+1]["from"] == "gpt":

            prompt = convo[i]["value"]
            response = convo[i+1]["value"]

            input_tokens = len(enc.encode(prompt))
            output_tokens = len(enc.encode(response))

            words = prompt.split()

            if len(words) == 0:
                avg_word_len = 0
                num_words = 0
            else:
                num_words = len(words)
                avg_word_len = sum(len(w) for w in words) / num_words

            question_words = ["what", "why", "how", "explain", "describe"]
            question_flag = int(any(q in prompt.lower() for q in question_words))

            rows.append({
    "text_len": len(prompt),
    "context_len": len(prompt),
    "num_words": num_words,
    "avg_word_len": avg_word_len,
    "question_flag": question_flag,
    "input_tokens": input_tokens,
    "output_tokens": output_tokens
})

# Convert to pandas
df = pd.DataFrame(rows)

# Save CSV
df.to_csv("data/processed.csv", index=False)

print("✅ Processed data saved!")

# OPTIONAL: Convert to Spark DataFrame (for Big Data requirement)
spark_df = spark.createDataFrame(df)
spark_df.show(5)