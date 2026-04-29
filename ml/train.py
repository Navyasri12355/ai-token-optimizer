import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
import numpy as np

# Load data
df = pd.read_csv("data/processed.csv")

X = df[[
    "context_len",
    "text_len",
    "num_words",
    "avg_word_len",
    "question_flag"
]]

y_input = df["input_tokens"]
y_output = df["output_tokens"]

# Split
X_train, X_test, y_in_train, y_in_test = train_test_split(X, y_input, test_size=0.2)
_, _, y_out_train, y_out_test = train_test_split(X, y_output, test_size=0.2)

# Train models
model_input = RandomForestRegressor()
model_output = RandomForestRegressor()

model_input.fit(X_train, y_in_train)
model_output.fit(X_train, y_out_train)

# Predictions
pred_in = model_input.predict(X_test)
pred_out = model_output.predict(X_test)

# ----------------------------
# Metrics for INPUT model
# ----------------------------
print("INPUT MODEL METRICS")
print("MAE:", mean_absolute_error(y_in_test, pred_in))
print("RMSE:", np.sqrt(mean_squared_error(y_in_test, pred_in)))
print("R2:", r2_score(y_in_test, pred_in))

# ----------------------------
# Metrics for OUTPUT model
# ----------------------------
print("\nOUTPUT MODEL METRICS")
print("MAE:", mean_absolute_error(y_out_test, pred_out))
print("RMSE:", np.sqrt(mean_squared_error(y_out_test, pred_out)))
print("R2:", r2_score(y_out_test, pred_out))

# Save models
pickle.dump(model_input, open("ml/model_input.pkl", "wb"))
pickle.dump(model_output, open("ml/model_output.pkl", "wb"))
print("✅ Models trained & saved")