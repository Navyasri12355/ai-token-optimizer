import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
import numpy as np

# Load data
df = pd.read_csv("data/processed.csv")

X = df[["context_len", "text_len", "num_words", "avg_word_len", "question_flag"]]
y_input  = df["input_tokens"]
y_output = df["output_tokens"]

# Split
X_train, X_test, y_in_train, y_in_test   = train_test_split(X, y_input,  test_size=0.2, random_state=42)
_, _,           y_out_train, y_out_test   = train_test_split(X, y_output, test_size=0.2, random_state=42)

# Lightweight pipeline: scaler + Ridge (tiny pkl, fast inference, < 1 KB)
def make_model():
    return Pipeline([
        ("scaler", StandardScaler()),
        ("ridge",  Ridge(alpha=1.0))
    ])

model_input  = make_model()
model_output = make_model()

model_input.fit(X_train, y_in_train)
model_output.fit(X_train, y_out_train)

# Predictions
pred_in  = model_input.predict(X_test)
pred_out = model_output.predict(X_test)

# ----------------------------
# Metrics for INPUT model
# ----------------------------
print("INPUT MODEL METRICS")
print("MAE: ",  round(mean_absolute_error(y_in_test, pred_in), 4))
print("RMSE:",  round(np.sqrt(mean_squared_error(y_in_test, pred_in)), 4))
print("R2:  ",  round(r2_score(y_in_test, pred_in), 4))

# ----------------------------
# Metrics for OUTPUT model
# ----------------------------
print("\nOUTPUT MODEL METRICS")
print("MAE: ",  round(mean_absolute_error(y_out_test, pred_out), 4))
print("RMSE:",  round(np.sqrt(mean_squared_error(y_out_test, pred_out)), 4))
print("R2:  ",  round(r2_score(y_out_test, pred_out), 4))

# Save models
pickle.dump(model_input,  open("ml/model_input.pkl",  "wb"))
pickle.dump(model_output, open("ml/model_output.pkl", "wb"))

sizes = {
    "model_input.pkl":  round(open("ml/model_input.pkl",  "rb").seek(0, 2) / 1024, 2),
    "model_output.pkl": round(open("ml/model_output.pkl", "rb").seek(0, 2) / 1024, 2),
}
print(f"\n✅ Models saved — sizes: {sizes} KB")