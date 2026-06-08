"""
Elasticsearch-based metrics tracking for model performance and system health.
Sends metrics to Elasticsearch for real-time visualization in Kibana.
"""

from elasticsearch import Elasticsearch
from datetime import datetime
from typing import Dict, Any, List
import json

class MetricsCollector:
    """Collect and send metrics to Elasticsearch"""
    
    def __init__(self, es_host: str = "localhost:9200", index_prefix: str = "metrics"):
        """
        Initialize metrics collector.
        
        Args:
            es_host: Elasticsearch host:port
            index_prefix: Index name prefix (date appended automatically)
        """
        self.es = Elasticsearch([f"http://{es_host}"])
        self.index_prefix = index_prefix
        self.index_name = f"{index_prefix}-{datetime.now().strftime('%Y.%m.%d')}"
        
        try:
            self.es.info()
            self.connected = True
        except Exception as e:
            print(f"⚠️ Cannot connect to Elasticsearch: {e}")
            self.connected = False
    
    def record_training_metrics(
        self,
        model_name: str,
        mae: float,
        rmse: float,
        r2: float,
        training_time: float,
        dataset_size: int,
        **kwargs
    ):
        """
        Record model training metrics.
        
        Args:
            model_name: Name of the model (e.g., 'input_rf_model')
            mae: Mean Absolute Error
            rmse: Root Mean Squared Error
            r2: R-squared score
            training_time: Training time in seconds
            dataset_size: Number of samples used
            **kwargs: Additional custom metrics
        """
        if not self.connected:
            return
        
        doc = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "training",
            "model_name": model_name,
            "metrics": {
                "mae": mae,
                "rmse": rmse,
                "r2": r2,
                "training_time_seconds": training_time,
                "dataset_size": dataset_size,
                **kwargs
            }
        }
        
        try:
            self.es.index(index=self.index_name, document=doc)
        except Exception as e:
            print(f"Error recording metrics: {e}")
    
    def record_prediction_metrics(
        self,
        model_name: str,
        input_tokens: int,
        output_tokens: int,
        prediction_time_ms: float,
        **kwargs
    ):
        """
        Record prediction metrics.
        
        Args:
            model_name: Name of the model used
            input_tokens: Predicted input tokens
            output_tokens: Predicted output tokens
            prediction_time_ms: Prediction time in milliseconds
            **kwargs: Additional custom fields
        """
        if not self.connected:
            return
        
        doc = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "prediction",
            "model_name": model_name,
            "predictions": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "prediction_time_ms": prediction_time_ms
            },
            **kwargs
        }
        
        try:
            self.es.index(index=self.index_name, document=doc)
        except Exception as e:
            print(f"Error recording prediction: {e}")
    
    def record_api_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        response_time_ms: float,
        **kwargs
    ):
        """
        Record API request metrics.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            status_code: Response status code
            response_time_ms: Response time in milliseconds
            **kwargs: Additional fields (user_id, error, etc.)
        """
        if not self.connected:
            return
        
        doc = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "api_request",
            "endpoint": endpoint,
            "method": method,
            "status_code": status_code,
            "response_time_ms": response_time_ms,
            **kwargs
        }
        
        try:
            self.es.index(index=self.index_name, document=doc)
        except Exception as e:
            print(f"Error recording API request: {e}")
    
    def record_data_processing(
        self,
        operation: str,
        rows_processed: int,
        processing_time_seconds: float,
        **kwargs
    ):
        """
        Record data processing metrics.
        
        Args:
            operation: Type of operation (preprocess, feature_engineering, etc.)
            rows_processed: Number of rows processed
            processing_time_seconds: Processing time in seconds
            **kwargs: Additional fields
        """
        if not self.connected:
            return
        
        doc = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "data_processing",
            "operation": operation,
            "rows_processed": rows_processed,
            "processing_time_seconds": processing_time_seconds,
            "throughput_rows_per_sec": rows_processed / processing_time_seconds if processing_time_seconds > 0 else 0,
            **kwargs
        }
        
        try:
            self.es.index(index=self.index_name, document=doc)
        except Exception as e:
            print(f"Error recording processing metrics: {e}")
    
    def get_training_summary(self, model_name: str, days: int = 7) -> Dict[str, Any]:
        """
        Get training metrics summary for a model.
        
        Args:
            model_name: Name of the model
            days: Number of days to look back
            
        Returns:
            Summary statistics
        """
        if not self.connected:
            return {}
        
        try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"model_name": model_name}},
                            {"match": {"event_type": "training"}},
                            {"range": {"timestamp": {"gte": f"now-{days}d"}}}
                        ]
                    }
                },
                "aggs": {
                    "mae_avg": {"avg": {"field": "metrics.mae"}},
                    "rmse_avg": {"avg": {"field": "metrics.rmse"}},
                    "r2_avg": {"avg": {"field": "metrics.r2"}},
                    "training_count": {"value_count": {"field": "model_name"}}
                }
            }
            
            response = self.es.search(index=f"{self.index_prefix}-*", body=query, size=0)
            
            aggs = response.get("aggregations", {})
            return {
                "average_mae": aggs.get("mae_avg", {}).get("value"),
                "average_rmse": aggs.get("rmse_avg", {}).get("value"),
                "average_r2": aggs.get("r2_avg", {}).get("value"),
                "total_trainings": int(aggs.get("training_count", {}).get("value", 0))
            }
        except Exception as e:
            print(f"Error fetching summary: {e}")
            return {}


# Global instance
_metrics_collector = None

def get_metrics_collector(es_host: str = "localhost:9200") -> MetricsCollector:
    """Get or create global metrics collector"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(es_host)
    return _metrics_collector


if __name__ == "__main__":
    # Test metrics collection
    collector = MetricsCollector()
    
    # Simulate training metrics
    collector.record_training_metrics(
        model_name="input_rf_model",
        mae=2.45,
        rmse=3.12,
        r2=0.92,
        training_time=45.3,
        dataset_size=80000,
        num_trees=100,
        max_depth=10
    )
    
    # Simulate prediction
    collector.record_prediction_metrics(
        model_name="input_rf_model",
        input_tokens=150,
        output_tokens=280,
        prediction_time_ms=12.5
    )
    
    print("✅ Metrics sent to Elasticsearch")
    print("📊 View in Kibana: http://localhost:5601")
