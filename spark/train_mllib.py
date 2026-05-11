"""
Spark MLlib training module for token prediction models.
Uses RandomForestRegressor and GradientBoostedTreeRegressor from Spark MLlib.
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GradientBoostedTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TokenPredictionMLlib") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Load processed data
df_pandas = pd.read_csv("data/processed.csv")
df = spark.createDataFrame(df_pandas)

print("✅ Data loaded with shape:", df.count(), "rows")

# Feature columns
feature_cols = [
    "context_len",
    "text_len",
    "num_words",
    "avg_word_len",
    "question_flag"
]

# Split data
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_df.count()} rows")
print(f"Test set: {test_df.count()} rows")

# ============================================================
# MODEL 1: Random Forest for INPUT TOKENS
# ============================================================
print("\n" + "="*50)
print("TRAINING INPUT TOKEN PREDICTION MODEL")
print("="*50)

# Assemble features for input model
assembler_input = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Random Forest model for input tokens
rf_input = RandomForestRegressor(
    labelCol="input_tokens",
    featuresCol="features",
    numTrees=100,
    maxDepth=10,
    minInstancesPerNode=1,
    seed=42,
    parallelism=4
)

# Create pipeline
pipeline_input = Pipeline(stages=[assembler_input, rf_input])

# Train model
model_input = pipeline_input.fit(train_df)

# Predictions
predictions_input = model_input.transform(test_df)

# Evaluate
evaluator = RegressionEvaluator(
    labelCol="input_tokens",
    predictionCol="prediction",
    metricName="rmse"
)

rmse_input = evaluator.evaluate(predictions_input)

evaluator_mae = RegressionEvaluator(
    labelCol="input_tokens",
    predictionCol="prediction",
    metricName="mae"
)

mae_input = evaluator_mae.evaluate(predictions_input)

evaluator_r2 = RegressionEvaluator(
    labelCol="input_tokens",
    predictionCol="prediction",
    metricName="r2"
)

r2_input = evaluator_r2.evaluate(predictions_input)

print("\n🔍 INPUT MODEL METRICS (Random Forest)")
print(f"   MAE:  {mae_input:.4f}")
print(f"   RMSE: {rmse_input:.4f}")
print(f"   R²:   {r2_input:.4f}")

# ============================================================
# MODEL 2: Random Forest for OUTPUT TOKENS
# ============================================================
print("\n" + "="*50)
print("TRAINING OUTPUT TOKEN PREDICTION MODEL")
print("="*50)

# Assemble features for output model
assembler_output = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Random Forest model for output tokens
rf_output = RandomForestRegressor(
    labelCol="output_tokens",
    featuresCol="features",
    numTrees=100,
    maxDepth=10,
    minInstancesPerNode=1,
    seed=42,
    parallelism=4
)

# Create pipeline
pipeline_output = Pipeline(stages=[assembler_output, rf_output])

# Train model
model_output = pipeline_output.fit(train_df)

# Predictions
predictions_output = model_output.transform(test_df)

# Evaluate
evaluator_rmse = RegressionEvaluator(
    labelCol="output_tokens",
    predictionCol="prediction",
    metricName="rmse"
)

rmse_output = evaluator_rmse.evaluate(predictions_output)

evaluator_mae_out = RegressionEvaluator(
    labelCol="output_tokens",
    predictionCol="prediction",
    metricName="mae"
)

mae_output = evaluator_mae_out.evaluate(predictions_output)

evaluator_r2_out = RegressionEvaluator(
    labelCol="output_tokens",
    predictionCol="prediction",
    metricName="r2"
)

r2_output = evaluator_r2_out.evaluate(predictions_output)

print("\n🔍 OUTPUT MODEL METRICS (Random Forest)")
print(f"   MAE:  {mae_output:.4f}")
print(f"   RMSE: {rmse_output:.4f}")
print(f"   R²:   {r2_output:.4f}")

# ============================================================
# BONUS: Gradient Boosted Tree Model for Comparison
# ============================================================
print("\n" + "="*50)
print("TRAINING GBT MODEL (FOR COMPARISON)")
print("="*50)

# GBT model for input tokens
gbt_input = GradientBoostedTreeRegressor(
    labelCol="input_tokens",
    featuresCol="features",
    maxIter=20,
    maxDepth=5,
    seed=42
)

pipeline_gbt = Pipeline(stages=[assembler_input, gbt_input])
model_gbt = pipeline_gbt.fit(train_df)

predictions_gbt = model_gbt.transform(test_df)
rmse_gbt = RegressionEvaluator(
    labelCol="input_tokens",
    predictionCol="prediction",
    metricName="rmse"
).evaluate(predictions_gbt)

mae_gbt = RegressionEvaluator(
    labelCol="input_tokens",
    predictionCol="prediction",
    metricName="mae"
).evaluate(predictions_gbt)

r2_gbt = RegressionEvaluator(
    labelCol="input_tokens",
    predictionCol="prediction",
    metricName="r2"
).evaluate(predictions_gbt)

print("\n🔍 INPUT MODEL METRICS (Gradient Boosted Tree)")
print(f"   MAE:  {mae_gbt:.4f}")
print(f"   RMSE: {rmse_gbt:.4f}")
print(f"   R²:   {r2_gbt:.4f}")

# ============================================================
# SAVE MODELS
# ============================================================
print("\n" + "="*50)
print("SAVING MODELS")
print("="*50)

model_input.write().overwrite().save("spark/models/input_rf_model")
model_output.write().overwrite().save("spark/models/output_rf_model")
model_gbt.write().overwrite().save("spark/models/gbt_input_model")

print("✅ All models saved to spark/models/")

# Show sample predictions
print("\n" + "="*50)
print("SAMPLE PREDICTIONS (INPUT MODEL)")
print("="*50)
predictions_input.select("context_len", "text_len", "input_tokens", "prediction") \
    .limit(5).show(truncate=False)

print("\n" + "="*50)
print("SAMPLE PREDICTIONS (OUTPUT MODEL)")
print("="*50)
predictions_output.select("context_len", "text_len", "output_tokens", "prediction") \
    .limit(5).show(truncate=False)

spark.stop()
print("\n✅ Training completed successfully!")
