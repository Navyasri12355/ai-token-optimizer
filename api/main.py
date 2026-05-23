from fastapi import FastAPI, Query
import pickle
import os
import sys
import tiktoken

# ---- Tokenizer ----
enc = tiktoken.encoding_for_model("gpt-3.5-turbo")

# ---- Resolve paths relative to this file ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

# Add root to path so optimizer.py can be imported
sys.path.insert(0, ROOT_DIR)
from optimizer import PromptOptimizer

# ---- Initialize App ----
app = FastAPI(title="LLM Cost Optimizer API")

# ---- Initialize Optimizer ----
optimizer = PromptOptimizer()

# ---- Paths ----
MODEL_INPUT_PATH  = os.path.join(ROOT_DIR, "ml", "model_input.pkl")
MODEL_OUTPUT_PATH = os.path.join(ROOT_DIR, "ml", "model_output.pkl")
ML_DIR = os.path.join(ROOT_DIR, "ml")


def train_and_save_models():
    """Train models from scratch using synthetic data and save pkl files."""
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import RandomForestRegressor
    import random

    print("⚙️  No pre-trained models found. Training from synthetic data...")

    os.makedirs(ML_DIR, exist_ok=True)

    random.seed(42)
    np.random.seed(42)

    # Generate synthetic training data that mimics real tokenization patterns
    n = 3000
    text_len     = np.random.randint(20, 800, n)
    context_len  = text_len
    num_words    = (text_len / np.random.uniform(4.5, 6.5, n)).astype(int)
    avg_word_len = text_len / np.maximum(num_words, 1)
    question_flag = np.random.randint(0, 2, n)

    # Token counts correlate with text length (~0.25 tokens per char for English)
    input_tokens  = (text_len * 0.26 + np.random.normal(0, 5, n)).clip(5).astype(int)
    output_tokens = (text_len * 0.55 * (1 + question_flag * 0.4) + np.random.normal(0, 15, n)).clip(5).astype(int)

    X = pd.DataFrame({
        "context_len":  context_len,
        "text_len":     text_len,
        "num_words":    num_words,
        "avg_word_len": avg_word_len,
        "question_flag": question_flag,
    })

    model_input  = RandomForestRegressor(n_estimators=100, random_state=42)
    model_output = RandomForestRegressor(n_estimators=100, random_state=42)

    model_input.fit(X, input_tokens)
    model_output.fit(X, output_tokens)

    pickle.dump(model_input,  open(MODEL_INPUT_PATH,  "wb"))
    pickle.dump(model_output, open(MODEL_OUTPUT_PATH, "wb"))

    print("✅ Models trained and saved.")
    return model_input, model_output


# ---- Load or Train ML Models ----
if os.path.exists(MODEL_INPUT_PATH) and os.path.exists(MODEL_OUTPUT_PATH):
    model_input  = pickle.load(open(MODEL_INPUT_PATH,  "rb"))
    model_output = pickle.load(open(MODEL_OUTPUT_PATH, "rb"))
    print("✅ Loaded pre-trained models.")
else:
    model_input, model_output = train_and_save_models()


# ---- Root Endpoint ----
@app.get("/")
def home():
    return {"message": "LLM Cost Optimizer API is running"}


# ---- Prediction Endpoint ----
@app.post("/predict")
def predict(prompt: str = Query(...)):

    prompt_clean = prompt.strip()

    # ----------------------------
    # 1. Feature Extraction
    # ----------------------------
    text_len    = len(prompt_clean)
    context_len = text_len

    words = prompt_clean.split()

    if len(words) == 0:
        num_words    = 0
        avg_word_len = 0
    else:
        num_words    = len(words)
        avg_word_len = sum(len(w) for w in words) / num_words

    question_words = ["what", "why", "how", "explain", "describe"]
    question_flag  = int(any(q in prompt_clean.lower() for q in question_words))

    # ----------------------------
    # 2. Predict Tokens
    # ----------------------------
    features = [[context_len, text_len, num_words, avg_word_len, question_flag]]
    input_tokens  = model_input.predict(features)[0]
    output_tokens = model_output.predict(features)[0]

    # ----------------------------
    # 3. Cost Calculation
    # ----------------------------
    input_price  = 0.0015
    output_price = 0.002

    cost = (
        (input_tokens  / 1000) * input_price +
        (output_tokens / 1000) * output_price
    )

    # ----------------------------
    # 4. Prompt Optimization
    # ----------------------------
    optimized_prompt = optimizer.optimize(prompt_clean)

    # ----------------------------
    # 5. Savings Calculation
    # ----------------------------
    original_tokens  = len(enc.encode(prompt_clean))
    optimized_tokens = len(enc.encode(optimized_prompt))

    token_savings = (
        ((original_tokens - optimized_tokens) / original_tokens) * 100
        if original_tokens > 0 else 0
    )
    char_savings = (
        ((len(prompt_clean) - len(optimized_prompt)) / len(prompt_clean)) * 100
        if len(prompt_clean) > 0 else 0
    )

    # ----------------------------
    # 6. Response
    # ----------------------------
    return {
        "input_tokens":           int(input_tokens),
        "output_tokens":          int(output_tokens),
        "total_tokens":           int(input_tokens + output_tokens),
        "estimated_cost":         round(cost, 6),
        "optimized_prompt":       optimized_prompt,
        "token_savings_percent":  round(token_savings, 2),
        "compression_percent":    round(char_savings, 2),
    }