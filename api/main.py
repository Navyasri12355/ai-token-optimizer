"""
FastAPI service for the AI Token Optimizer.

Model backend: Spark MLlib PipelineModels (spark/models/).
No sklearn or pickle dependencies.
"""

from __future__ import annotations

import os
import threading

import tiktoken
from fastapi import FastAPI, Query

from optimizer import PromptOptimizer

# ---------------------------------------------------------------------------
# Lazy-loaded Spark predictor (initialised once on first request)
# ---------------------------------------------------------------------------
_predictor = None
_predictor_lock = threading.Lock()


def _get_predictor():
    """Return the singleton TokenPredictor, creating it on first call."""
    global _predictor
    if _predictor is None:
        with _predictor_lock:
            if _predictor is None:
                # Import here so FastAPI workers don't start Spark at import time
                from spark.predict import TokenPredictor
                _predictor = TokenPredictor(model_dir="spark/models")
    return _predictor


# ---------------------------------------------------------------------------
# Tokenizer (for savings computation only)
# ---------------------------------------------------------------------------
enc = tiktoken.encoding_for_model("gpt-3.5-turbo")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="AI Token Optimizer API",
    description="Predict LLM token usage with distributed Spark MLlib models.",
    version="2.0.0",
)

# ---------------------------------------------------------------------------
# Optional MongoDB logging
# ---------------------------------------------------------------------------
_db_collection = None
try:
    from pymongo import MongoClient
    _client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
    _client.admin.command("ping")          # probe connectivity
    _db_collection = _client["llm_optimizer"]["logs"]
    print("✅  MongoDB connected — request logging enabled")
except Exception:
    print("ℹ️   MongoDB unavailable — logging disabled")

optimizer = PromptOptimizer()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def home():
    return {"message": "AI Token Optimizer API is running", "version": "2.0.0"}


@app.get("/health")
def health():
    """Readiness probe — checks that models are loadable."""
    try:
        _get_predictor()
        return {"status": "ok", "models": "spark/models"}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/predict")
def predict(prompt: str = Query(..., description="The raw prompt text")):
    """
    Predict input/output token counts and estimated cost, and return an
    optimised version of the prompt.
    """
    prompt_clean = prompt.strip()

    # ------------------------------------------------------------------
    # 1. Feature extraction (mirrored from preprocess.py UDFs)
    # ------------------------------------------------------------------
    text_len    = len(prompt_clean)
    context_len = text_len
    words       = prompt_clean.split()
    num_words   = len(words)
    avg_word_len = (
        float(sum(len(w) for w in words)) / float(num_words)
        if num_words else 0.0
    )
    question_words = {"what", "why", "how", "explain", "describe"}
    question_flag  = int(any(q in prompt_clean.lower() for q in question_words))

    # ------------------------------------------------------------------
    # 2. Token prediction via Spark MLlib
    # ------------------------------------------------------------------
    import pandas as pd

    features = pd.DataFrame([{
        "context_len":   float(context_len),
        "text_len":      float(text_len),
        "num_words":     float(num_words),
        "avg_word_len":  avg_word_len,
        "question_flag": float(question_flag),
    }])

    predictor = _get_predictor()
    token_pred = predictor.predict(features)
    input_tokens  = token_pred["input_tokens"]
    output_tokens = token_pred["output_tokens"]

    # ------------------------------------------------------------------
    # 3. Cost calculation
    # ------------------------------------------------------------------
    INPUT_PRICE_PER_1K  = 0.0015   # USD per 1 K input tokens
    OUTPUT_PRICE_PER_1K = 0.002    # USD per 1 K output tokens

    cost = (
        (input_tokens  / 1000) * INPUT_PRICE_PER_1K +
        (output_tokens / 1000) * OUTPUT_PRICE_PER_1K
    )

    # ------------------------------------------------------------------
    # 4. Prompt optimisation
    # ------------------------------------------------------------------
    optimized_prompt = optimizer.optimize(prompt_clean)

    original_tokens  = len(enc.encode(prompt_clean))
    optimized_tokens = len(enc.encode(optimized_prompt))

    token_savings = (
        ((original_tokens - optimized_tokens) / original_tokens * 100)
        if original_tokens else 0.0
    )
    char_savings = (
        ((len(prompt_clean) - len(optimized_prompt)) / len(prompt_clean) * 100)
        if prompt_clean else 0.0
    )

    # ------------------------------------------------------------------
    # 5. Optional MongoDB logging
    # ------------------------------------------------------------------
    if _db_collection is not None:
        try:
            _db_collection.insert_one({
                "prompt":        prompt_clean,
                "input_tokens":  input_tokens,
                "output_tokens": output_tokens,
                "cost":          round(cost, 6),
            })
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 6. Response
    # ------------------------------------------------------------------
    return {
        "input_tokens":           input_tokens,
        "output_tokens":          output_tokens,
        "total_tokens":           input_tokens + output_tokens,
        "estimated_cost":         round(cost, 6),
        "optimized_prompt":       optimized_prompt,
        "token_savings_percent":  round(token_savings, 2),
        "compression_percent":    round(char_savings, 2),
    }