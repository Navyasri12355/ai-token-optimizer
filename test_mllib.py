"""
Comprehensive test suite for Spark MLlib training functionality.
Tests Random Forest and Gradient Boosted Tree models for token prediction.
"""

import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GradientBoostedTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline


class MLlibTester:
    """Test suite for MLlib functionality"""
    
    def __init__(self):
        """Initialize Spark session and paths"""
        self.spark = SparkSession.builder \
            .appName("TokenPredictionMLlib_Test") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.data_path = "data/processed.csv"
        self.feature_cols = [
            "context_len",
            "text_len",
            "num_words",
            "avg_word_len",
            "question_flag"
        ]
    
    def create_sample_data(self) -> bool:
        """
        Create sample processed data if it doesn't exist.
        
        Returns:
            True if data created/exists, False otherwise
        """
        print("\n📊 Creating Sample Data...")
        
        if os.path.exists(self.data_path):
            print(f"✅ Data already exists at {self.data_path}")
            return True
        
        try:
            # Create sample data with realistic token prediction values
            np.random.seed(42)
            n_samples = 1000
            
            data = {
                "context_len": np.random.randint(50, 5000, n_samples),
                "text_len": np.random.randint(50, 5000, n_samples),
                "num_words": np.random.randint(5, 500, n_samples),
                "avg_word_len": np.random.uniform(3, 8, n_samples),
                "question_flag": np.random.choice([0, 1], n_samples),
                "input_tokens": np.random.randint(10, 1000, n_samples),
                "output_tokens": np.random.randint(10, 2000, n_samples)
            }
            
            df = pd.DataFrame(data)
            os.makedirs("data", exist_ok=True)
            df.to_csv(self.data_path, index=False)
            
            print(f"✅ Sample data created: {n_samples} samples")
            print(f"   Features: {', '.join(self.feature_cols)}")
            print(f"   Targets: input_tokens, output_tokens")
            return True
            
        except Exception as e:
            print(f"❌ Error creating sample data: {e}")
            return False
    
    def load_and_validate_data(self) -> bool:
        """
        Load data and validate format.
        
        Returns:
            True if data is valid, False otherwise
        """
        print("\n📂 Loading Data...")
        
        try:
            df_pandas = pd.read_csv(self.data_path)
            df = self.spark.createDataFrame(df_pandas)
            
            print(f"✅ Data loaded: {df.count()} rows, {len(df.columns)} columns")
            
            # Validate required columns
            required_cols = self.feature_cols + ["input_tokens", "output_tokens"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"❌ Missing columns: {missing_cols}")
                return False
            
            print(f"✅ All required columns present")
            
            # Show sample data
            print("\n📋 Sample Data:")
            df.show(3, truncate=False)
            
            self.df = df
            return True
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def test_random_forest_input_tokens(self) -> bool:
        """
        Test Random Forest model for input token prediction.
        
        Returns:
            True if model trained and evaluated successfully
        """
        print("\n" + "="*60)
        print("🌲 Testing Random Forest for INPUT TOKEN Prediction")
        print("="*60)
        
        try:
            # Split data
            train_df, test_df = self.df.randomSplit([0.8, 0.2], seed=42)
            print(f"✅ Data split: {train_df.count()} train, {test_df.count()} test")
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=self.feature_cols,
                outputCol="features"
            )
            
            # Create Random Forest model
            rf_model = RandomForestRegressor(
                labelCol="input_tokens",
                featuresCol="features",
                numTrees=50,
                maxDepth=10,
                minInstancesPerNode=1,
                seed=42
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, rf_model])
            
            # Train model
            print("🔄 Training Random Forest model...")
            trained_model = pipeline.fit(train_df)
            print("✅ Model training completed")
            
            # Make predictions
            predictions = trained_model.transform(test_df)
            
            # Evaluate
            evaluator = RegressionEvaluator(
                labelCol="input_tokens",
                predictionCol="prediction",
                metricName="rmse"
            )
            
            rmse = evaluator.evaluate(predictions)
            r2 = evaluator.setMetricName("r2").evaluate(predictions)
            mae = evaluator.setMetricName("mae").evaluate(predictions)
            
            print(f"\n📊 Random Forest - INPUT TOKENS Results:")
            print(f"   RMSE: {rmse:.4f}")
            print(f"   R²: {r2:.4f}")
            print(f"   MAE: {mae:.4f}")
            
            # Show sample predictions
            print(f"\n🔮 Sample Predictions:")
            predictions.select("input_tokens", "prediction").show(5, truncate=False)
            
            self.rf_input_model = trained_model
            return True
            
        except Exception as e:
            print(f"❌ Error in Random Forest training: {e}")
            return False
    
    def test_random_forest_output_tokens(self) -> bool:
        """
        Test Random Forest model for output token prediction.
        
        Returns:
            True if model trained and evaluated successfully
        """
        print("\n" + "="*60)
        print("🌲 Testing Random Forest for OUTPUT TOKEN Prediction")
        print("="*60)
        
        try:
            # Split data
            train_df, test_df = self.df.randomSplit([0.8, 0.2], seed=42)
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=self.feature_cols,
                outputCol="features"
            )
            
            # Create Random Forest model
            rf_model = RandomForestRegressor(
                labelCol="output_tokens",
                featuresCol="features",
                numTrees=50,
                maxDepth=10,
                minInstancesPerNode=1,
                seed=42
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, rf_model])
            
            # Train model
            print("🔄 Training Random Forest model...")
            trained_model = pipeline.fit(train_df)
            print("✅ Model training completed")
            
            # Make predictions
            predictions = trained_model.transform(test_df)
            
            # Evaluate
            evaluator = RegressionEvaluator(
                labelCol="output_tokens",
                predictionCol="prediction",
                metricName="rmse"
            )
            
            rmse = evaluator.evaluate(predictions)
            r2 = evaluator.setMetricName("r2").evaluate(predictions)
            mae = evaluator.setMetricName("mae").evaluate(predictions)
            
            print(f"\n📊 Random Forest - OUTPUT TOKENS Results:")
            print(f"   RMSE: {rmse:.4f}")
            print(f"   R²: {r2:.4f}")
            print(f"   MAE: {mae:.4f}")
            
            # Show sample predictions
            print(f"\n🔮 Sample Predictions:")
            predictions.select("output_tokens", "prediction").show(5, truncate=False)
            
            self.rf_output_model = trained_model
            return True
            
        except Exception as e:
            print(f"❌ Error in Random Forest training: {e}")
            return False
    
    def test_gradient_boosted_trees(self) -> bool:
        """
        Test Gradient Boosted Trees model for token prediction.
        
        Returns:
            True if model trained and evaluated successfully
        """
        print("\n" + "="*60)
        print("🚀 Testing Gradient Boosted Trees for COMBINED Prediction")
        print("="*60)
        
        try:
            # Create combined target (normalized)
            df_with_combined = self.df.withColumn(
                "combined_tokens",
                (self.df.input_tokens + self.df.output_tokens) / 2
            )
            
            # Split data
            train_df, test_df = df_with_combined.randomSplit([0.8, 0.2], seed=42)
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=self.feature_cols,
                outputCol="features"
            )
            
            # Create GBT model
            gbt_model = GradientBoostedTreeRegressor(
                labelCol="combined_tokens",
                featuresCol="features",
                maxIter=100,
                maxDepth=5,
                seed=42
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, gbt_model])
            
            # Train model
            print("🔄 Training Gradient Boosted Trees model...")
            trained_model = pipeline.fit(train_df)
            print("✅ Model training completed")
            
            # Make predictions
            predictions = trained_model.transform(test_df)
            
            # Evaluate
            evaluator = RegressionEvaluator(
                labelCol="combined_tokens",
                predictionCol="prediction",
                metricName="rmse"
            )
            
            rmse = evaluator.evaluate(predictions)
            r2 = evaluator.setMetricName("r2").evaluate(predictions)
            mae = evaluator.setMetricName("mae").evaluate(predictions)
            
            print(f"\n📊 Gradient Boosted Trees Results:")
            print(f"   RMSE: {rmse:.4f}")
            print(f"   R²: {r2:.4f}")
            print(f"   MAE: {mae:.4f}")
            
            # Show sample predictions
            print(f"\n🔮 Sample Predictions:")
            predictions.select("combined_tokens", "prediction").show(5, truncate=False)
            
            self.gbt_model = trained_model
            return True
            
        except Exception as e:
            print(f"❌ Error in Gradient Boosted Trees training: {e}")
            return False
    
    def test_feature_importance(self) -> bool:
        """
        Test feature importance from Random Forest model.
        
        Returns:
            True if feature importance computed successfully
        """
        print("\n" + "="*60)
        print("📊 Feature Importance Analysis")
        print("="*60)
        
        try:
            if not hasattr(self, 'rf_input_model'):
                print("⚠️  No Random Forest model available. Skipping feature importance.")
                return False
            
            # Get feature importances from the model
            model = self.rf_input_model.stages[-1]  # Get the RF model from pipeline
            importances = model.featureImportances.toArray()
            
            # Create importance dataframe
            importance_df = pd.DataFrame({
                'feature': self.feature_cols,
                'importance': importances
            }).sort_values('importance', ascending=False)
            
            print("\n🎯 Feature Importance (INPUT TOKENS Model):")
            for idx, row in importance_df.iterrows():
                bar_length = int(row['importance'] * 50)
                bar = "█" * bar_length
                print(f"   {row['feature']:15s} {bar} {row['importance']:.4f}")
            
            return True
            
        except Exception as e:
            print(f"⚠️  Error computing feature importance: {e}")
            return False
    
    def run_all_tests(self) -> dict:
        """
        Run all MLlib tests.
        
        Returns:
            Dictionary with test results
        """
        print("\n" + "="*60)
        print("🧪 SPARK MLLIB TEST SUITE")
        print("="*60)
        
        results = {}
        
        # Create sample data
        results['data_creation'] = self.create_sample_data()
        if not results['data_creation']:
            return results
        
        # Load and validate
        results['data_loading'] = self.load_and_validate_data()
        if not results['data_loading']:
            return results
        
        # Test models
        results['rf_input_tokens'] = self.test_random_forest_input_tokens()
        results['rf_output_tokens'] = self.test_random_forest_output_tokens()
        results['gbt_model'] = self.test_gradient_boosted_trees()
        results['feature_importance'] = self.test_feature_importance()
        
        # Print summary
        self.print_summary(results)
        
        return results
    
    def print_summary(self, results: dict):
        """Print test summary"""
        print("\n" + "="*60)
        print("✅ TEST SUMMARY")
        print("="*60)
        
        for test_name, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"{status} - {test_name}")
        
        total = len(results)
        passed = sum(1 for v in results.values() if v)
        print(f"\n📈 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("\n🎉 All MLlib tests passed!")
        else:
            print(f"\n⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    tester = MLlibTester()
    results = tester.run_all_tests()
