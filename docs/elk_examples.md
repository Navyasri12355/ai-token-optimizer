"""
Integration examples: Using Elasticsearch logging and metrics with existing code
"""

# ==========================================
# Example 1: Enhanced Spark Training with Metrics
# ==========================================

"""
Modify spark/train_mllib.py to include:
"""

import time
from metrics import get_metrics_collector
from logging_config import get_training_logger

metrics = get_metrics_collector()
logger = get_training_logger()

# Before training
start_time = time.time()

# ... your training code ...
# model_input = pipeline_input.fit(train_df)
# rmse_input = evaluator.evaluate(predictions_input)
# mae_input = evaluator_mae.evaluate(predictions_input)
# r2_input = evaluator_r2.evaluate(predictions_input)

# Record metrics
training_time = time.time() - start_time
metrics.record_training_metrics(
    model_name="input_rf_model",
    mae=mae_input,
    rmse=rmse_input,
    r2=r2_input,
    training_time=training_time,
    dataset_size=train_df.count(),
    num_trees=100,
    max_depth=10,
    parallelism=4
)

logger.info(f"Training completed: mae={mae_input:.4f}, rmse={rmse_input:.4f}, r2={r2_input:.4f}")


# ==========================================
# Example 2: Enhanced Prediction with Metrics
# ==========================================

"""
Modify spark/predict.py to include:
"""

import time
from metrics import get_metrics_collector
from logging_config import get_prediction_logger

metrics = get_metrics_collector()
logger = get_prediction_logger()

def predict_with_logging(self, data):
    """Enhanced predict with metrics"""
    start_time = time.time()
    
    # Make prediction
    result = self.predict(data)
    
    prediction_time = (time.time() - start_time) * 1000  # ms
    
    # Record metrics
    metrics.record_prediction_metrics(
        model_name="input_rf_model",
        input_tokens=result['input_tokens'],
        output_tokens=result['output_tokens'],
        prediction_time_ms=prediction_time
    )
    
    logger.debug(f"Prediction made in {prediction_time:.2f}ms")
    
    return result


# ==========================================
# Example 3: API Integration with FastAPI
# ==========================================

"""
Example API endpoint with logging:
"""

from fastapi import FastAPI
from metrics import get_metrics_collector
from logging_config import get_api_logger
import time

app = FastAPI()
metrics = get_metrics_collector()
logger = get_api_logger()

@app.post("/predict")
async def predict_tokens(features: dict):
    """Predict tokens with full logging"""
    start_time = time.time()
    
    try:
        # Make prediction
        result = predictor.predict(features)
        status_code = 200
    except Exception as e:
        logger.error(f"Prediction failed: {str(e)}")
        result = {"error": str(e)}
        status_code = 500
    
    response_time = (time.time() - start_time) * 1000  # ms
    
    # Record API metrics
    metrics.record_api_request(
        endpoint="/predict",
        method="POST",
        status_code=status_code,
        response_time_ms=response_time,
        input_features=list(features.keys()),
        result_keys=list(result.keys()) if status_code == 200 else []
    )
    
    logger.info(f"API /predict responded with {status_code} in {response_time:.2f}ms")
    
    return result


# ==========================================
# Example 4: Data Processing with Metrics
# ==========================================

"""
Example data processing with metrics:
"""

from metrics import get_metrics_collector
from logging_config import setup_logging
import time

metrics = get_metrics_collector()
logger = setup_logging()

def process_data_batch(data_file):
    """Process data with metrics"""
    logger.info(f"Starting data processing: {data_file}")
    
    start_time = time.time()
    
    # Load data
    df = pd.read_csv(data_file)
    rows_processed = len(df)
    
    # ... processing ...
    
    processing_time = time.time() - start_time
    
    # Record metrics
    metrics.record_data_processing(
        operation="load_and_process",
        rows_processed=rows_processed,
        processing_time_seconds=processing_time,
        file=data_file,
        features_created=len(df.columns)
    )
    
    logger.info(f"Processed {rows_processed} rows in {processing_time:.2f}s")
    
    return df


# ==========================================
# Example 5: Batch Prediction with Monitoring
# ==========================================

"""
Batch prediction with comprehensive monitoring:
"""

from metrics import get_metrics_collector
from logging_config import get_prediction_logger
import time

metrics = get_metrics_collector()
logger = get_prediction_logger()

def batch_predict_with_monitoring(predictor, data_batch):
    """Make batch predictions with detailed monitoring"""
    
    logger.info(f"Starting batch prediction for {len(data_batch)} samples")
    start_time = time.time()
    
    predictions = []
    errors = 0
    
    for idx, sample in enumerate(data_batch.iterrows()):
        try:
            pred = predictor.predict(sample[1].to_dict())
            predictions.append(pred)
        except Exception as e:
            logger.error(f"Error predicting sample {idx}: {str(e)}")
            errors += 1
    
    total_time = time.time() - start_time
    
    # Record batch metrics
    metrics.record_prediction_metrics(
        model_name="batch_prediction",
        input_tokens=sum(p['input_tokens'] for p in predictions) // len(predictions),
        output_tokens=sum(p['output_tokens'] for p in predictions) // len(predictions),
        prediction_time_ms=total_time * 1000,
        batch_size=len(data_batch),
        successful=len(predictions),
        failed=errors,
        success_rate=(len(predictions) / len(data_batch) * 100)
    )
    
    logger.info(
        f"Batch complete: {len(predictions)} successful, {errors} failed in {total_time:.2f}s"
    )
    
    return predictions


# ==========================================
# Setup Instructions
# ==========================================

"""
To integrate into your existing code:

1. Start Elasticsearch + Kibana:
   python quickstart_elk.py

2. Import logging and metrics:
   from logging_config import setup_logging, get_training_logger
   from metrics import get_metrics_collector

3. Initialize at application startup:
   logger = setup_logging()
   metrics = get_metrics_collector()

4. Add logging throughout your code:
   logger.info("Something happened")
   logger.error("Error occurred")

5. Record metrics at key points:
   metrics.record_training_metrics(...)
   metrics.record_prediction_metrics(...)
   metrics.record_api_request(...)

6. Monitor in Kibana:
   http://localhost:5601

7. Create custom dashboards for your needs
"""
