"""
Spark MLlib prediction utilities for token estimation.
Loads trained models and makes predictions on new data.
"""

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from typing import Dict, Tuple

class TokenPredictor:
    """Spark MLlib-based token predictor"""
    
    def __init__(self, model_dir: str = "spark/models"):
        """
        Initialize predictor with trained models.
        
        Args:
            model_dir: Directory containing saved models
        """
        self.spark = SparkSession.builder \
            .appName("TokenPrediction") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        self.model_dir = model_dir
        
        # Load models
        self.model_input = PipelineModel.load(f"{model_dir}/input_rf_model")
        self.model_output = PipelineModel.load(f"{model_dir}/output_rf_model")
        
        self.feature_cols = [
            "context_len", "text_len", "num_words",
            "avg_word_len", "question_flag"
        ]
    
    def predict(self, data: pd.DataFrame) -> Dict[str, float]:
        """
        Predict token counts for input data.
        
        Args:
            data: DataFrame with features
            
        Returns:
            Dictionary with 'input_tokens' and 'output_tokens' predictions
        """
        # Convert to Spark DataFrame
        spark_df = self.spark.createDataFrame(data)
        
        # Make predictions
        input_pred = self.model_input.transform(spark_df)
        output_pred = self.model_output.transform(spark_df)
        
        # Extract values
        input_tokens = input_pred.select("prediction").collect()[0][0]
        output_tokens = output_pred.select("prediction").collect()[0][0]
        
        return {
            "input_tokens": int(round(input_tokens)),
            "output_tokens": int(round(output_tokens))
        }
    
    def batch_predict(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Batch prediction for multiple prompts.
        
        Args:
            data: DataFrame with feature columns
            
        Returns:
            DataFrame with predictions added
        """
        spark_df = self.spark.createDataFrame(data)
        
        input_pred = self.model_input.transform(spark_df)
        output_pred = self.model_output.transform(spark_df)
        
        # Get both predictions
        results = input_pred.select(
            *self.feature_cols,
            "input_tokens"
        ).join(
            output_pred.select("prediction").alias("output_pred"),
            input_pred.rdd.zipWithIndex().map(lambda x: x[1]) ==
            output_pred.rdd.zipWithIndex().map(lambda x: x[1])
        )
        
        return results.toPandas()
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()


if __name__ == "__main__":
    # Example usage
    predictor = TokenPredictor()
    
    # Sample data
    sample_data = pd.DataFrame({
        "context_len": [150],
        "text_len": [150],
        "num_words": [25],
        "avg_word_len": [6.0],
        "question_flag": [1]
    })
    
    result = predictor.predict(sample_data)
    print(f"Predicted input tokens: {result['input_tokens']}")
    print(f"Predicted output tokens: {result['output_tokens']}")
    
    predictor.close()
