#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
🔮 Model Loader for Real Estate Prediction
==========================================

Utility class để load và sử dụng trained models cho prediction.
Hỗ trợ cả Spark ML models và sklearn models.

Author: ML Team
Date: June 2025
"""

import os
import json
import joblib
import logging
from typing import Dict, List, Tuple, Any, Optional, Union
from datetime import datetime
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, lit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealEstateModelLoader:
    """🏠 Model loader cho real estate prediction"""

    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize Model Loader

        Args:
            spark_session: Spark session (optional)
        """
        self.spark = spark_session
        if self.spark is None and self._needs_spark():
            self.spark = self._create_spark_session()

        logger.info("🔮 Real Estate Model Loader initialized")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for model loading"""
        return (
            SparkSession.builder.appName("RealEstate_Model_Inference")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

    def _needs_spark(self) -> bool:
        """Check if Spark is needed for current operations"""
        # Simple heuristic - can be enhanced
        return True

    def load_model_registry(self, model_path: str) -> Dict[str, Any]:
        """
        📖 Load model registry để biết model nào là best

        Args:
            model_path: Path to model directory

        Returns:
            Dict: Model registry information
        """
        logger.info(f"📖 Loading model registry from {model_path}")

        registry_path = f"{model_path}/model_registry.json"

        try:
            if self.spark:
                # Read từ Spark (HDFS compatible)
                registry_df = self.spark.read.json(registry_path)
                registry_json = registry_df.collect()[0]["registry"]
                return json.loads(registry_json)
            else:
                # Read từ local filesystem
                with open(registry_path, "r") as f:
                    return json.load(f)

        except Exception as e:
            logger.error(f"❌ Failed to load model registry: {e}")
            raise

    def load_best_model(self, model_path: str) -> Tuple[Any, Dict[str, Any]]:
        """
        🏆 Load best model dựa trên registry

        Args:
            model_path: Path to model directory

        Returns:
            Tuple[model, metadata]: Loaded model and metadata
        """
        logger.info(f"🏆 Loading best model from {model_path}")

        # Load registry to identify best model
        registry = self.load_model_registry(model_path)
        best_model_info = registry["best_model"]

        logger.info(
            f"Best model: {best_model_info['name']} (R²={best_model_info['r2']:.3f})"
        )

        # Load model based on type
        if best_model_info["model_type"] == "spark":
            model = self._load_spark_model(model_path, best_model_info["name"])
        elif best_model_info["model_type"] == "sklearn":
            model = self._load_sklearn_model(model_path, best_model_info["name"])
        else:
            raise ValueError(f"Unsupported model type: {best_model_info['model_type']}")

        return model, best_model_info

    def _load_spark_model(self, model_path: str, model_name: str) -> PipelineModel:
        """Load Spark ML model"""
        spark_model_path = f"{model_path}/spark_models/{model_name}"

        if not self.spark:
            raise RuntimeError("Spark session required for loading Spark models")

        return PipelineModel.load(spark_model_path)

    def _load_sklearn_model(self, model_path: str, model_name: str) -> Any:
        """Load sklearn model from HDFS or local filesystem"""
        sklearn_model_path = f"{model_path}/sklearn_models/{model_name}_model.pkl"

        # For HDFS paths, copy to local temp first, then load
        if model_path.startswith("/data/"):
            try:
                # Copy from HDFS to local temp file
                import tempfile

                local_temp_dir = tempfile.mkdtemp(prefix=f"load_sklearn_{model_name}_")
                local_model_file = f"{local_temp_dir}/{model_name}_model.pkl"

                # Use Hadoop filesystem to copy from HDFS to local
                if self.spark:
                    hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
                    hadoop_fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                        hadoop_config
                    )

                    hdfs_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                        sklearn_model_path
                    )
                    local_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                        f"file://{local_model_file}"
                    )

                    # Copy from HDFS to local
                    hadoop_fs.copyToLocalFile(hdfs_path, local_path)

                    # Load using joblib
                    model = joblib.load(local_model_file)

                    # Clean up temp file
                    import os

                    os.remove(local_model_file)
                    os.rmdir(local_temp_dir)

                    return model
                else:
                    raise RuntimeError(
                        "Spark session required for loading models from HDFS"
                    )

            except Exception as e:
                logger.error(f"Failed to load sklearn model from HDFS: {e}")
                raise
        else:
            # For local paths, load directly
            return joblib.load(sklearn_model_path)

    def load_preprocessing_pipeline(self, model_path: str) -> PipelineModel:
        """
        🔧 Load preprocessing pipeline

        Args:
            model_path: Path to model directory

        Returns:
            PipelineModel: Preprocessing pipeline
        """
        preprocessing_path = f"{model_path}/preprocessing_pipeline"

        if not self.spark:
            raise RuntimeError(
                "Spark session required for loading preprocessing pipeline"
            )

        return PipelineModel.load(preprocessing_path)

    def load_ensemble_config(self, model_path: str) -> Dict[str, Any]:
        """
        🎯 Load ensemble configuration

        Args:
            model_path: Path to model directory

        Returns:
            Dict: Ensemble configuration
        """
        registry = self.load_model_registry(model_path)
        return registry.get("ensemble", {})

    def get_latest_model_path(
        self,
        property_type: str = "house",
        base_path: str = "/data/realestate/processed/ml/models",
    ) -> str:
        """
        📅 Get path to latest trained model

        Args:
            property_type: Type of property (house, apartment, land)
            base_path: Base model storage path

        Returns:
            str: Path to latest model
        """
        property_path = f"{base_path}/{property_type}"

        try:
            if self.spark:
                # List directories using Spark
                hadoop_fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark._jsc.hadoopConfiguration()
                )
                path = self.spark._jvm.org.apache.hadoop.fs.Path(property_path)

                if hadoop_fs.exists(path):
                    statuses = hadoop_fs.listStatus(path)
                    dirs = [
                        str(status.getPath().getName())
                        for status in statuses
                        if status.isDirectory()
                    ]

                    if dirs:
                        # Sort by date (assuming YYYY-MM-DD format)
                        latest_date = max(dirs)
                        return f"{property_path}/{latest_date}"
            else:
                # Local filesystem
                if os.path.exists(property_path):
                    dirs = [
                        d
                        for d in os.listdir(property_path)
                        if os.path.isdir(os.path.join(property_path, d))
                    ]

                    if dirs:
                        latest_date = max(dirs)
                        return f"{property_path}/{latest_date}"

        except Exception as e:
            logger.error(f"❌ Failed to find latest model: {e}")

        raise FileNotFoundError(f"No trained models found for {property_type}")


class RealEstatePredictionPipeline:
    """🔮 Complete prediction pipeline cho real estate"""

    def __init__(self, model_path: str, spark_session: Optional[SparkSession] = None):
        """
        Initialize prediction pipeline

        Args:
            model_path: Path to trained model
            spark_session: Spark session (optional)
        """
        self.model_path = model_path
        self.loader = RealEstateModelLoader(spark_session)

        # Load components
        self.model, self.model_metadata = self.loader.load_best_model(model_path)
        self.preprocessing_pipeline = self.loader.load_preprocessing_pipeline(
            model_path
        )
        self.ensemble_config = self.loader.load_ensemble_config(model_path)

        logger.info(f"🔮 Prediction pipeline loaded: {self.model_metadata['name']}")

    def predict_single(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        🏠 Predict price cho single property

        Args:
            property_data: Property features as dictionary

        Returns:
            Dict: Prediction result
        """
        # Convert to DataFrame
        if self.loader.spark:
            # Spark DataFrame
            df = self.loader.spark.createDataFrame([property_data])
            return self._predict_spark_batch(df)
        else:
            # Pandas DataFrame
            df = pd.DataFrame([property_data])
            return self._predict_pandas_batch(df)

    def predict_batch(
        self, properties_data: Union[DataFrame, pd.DataFrame]
    ) -> Union[DataFrame, pd.DataFrame]:
        """
        📊 Predict prices cho batch of properties

        Args:
            properties_data: Batch of property data

        Returns:
            DataFrame: Predictions with original data
        """
        if isinstance(properties_data, DataFrame):
            return self._predict_spark_batch(properties_data)
        else:
            return self._predict_pandas_batch(properties_data)

    def _predict_spark_batch(self, df: DataFrame) -> DataFrame:
        """Predict using Spark models"""
        # Apply preprocessing
        processed_df = self.preprocessing_pipeline.transform(df)

        # Apply model prediction
        if self.model_metadata["model_type"] == "spark":
            predictions = self.model.transform(processed_df)
        else:
            # Convert to pandas for sklearn model
            pandas_df = processed_df.toPandas()
            pred_results = self._predict_sklearn(pandas_df)

            # Convert back to Spark DataFrame
            predictions = self.loader.spark.createDataFrame(pred_results)

        return predictions

    def _predict_pandas_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Predict using pandas/sklearn models"""
        # For sklearn models, need to handle preprocessing manually
        # This is a simplified version - full implementation would need
        # to replicate Spark preprocessing logic

        predictions = self._predict_sklearn(df)
        return predictions

    def _predict_sklearn(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply sklearn model prediction"""
        # Extract features (simplified - needs proper feature engineering)
        feature_columns = self.model_metadata.get("feature_columns", [])

        if feature_columns:
            X = df[feature_columns]
        else:
            # Use numeric columns as fallback
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            X = df[numeric_columns]

        # Make predictions
        predictions = self.model.predict(X)

        # Add predictions to DataFrame
        result = df.copy()
        result["predicted_price"] = predictions
        result["prediction_timestamp"] = datetime.now().isoformat()
        result["model_used"] = self.model_metadata["name"]
        result["model_r2"] = self.model_metadata["r2"]

        return result

    def get_model_info(self) -> Dict[str, Any]:
        """
        ℹ️ Get information about loaded model

        Returns:
            Dict: Model information
        """
        return {
            "model_name": self.model_metadata["name"],
            "model_type": self.model_metadata["model_type"],
            "model_performance": {
                "rmse": self.model_metadata["rmse"],
                "mae": self.model_metadata["mae"],
                "r2": self.model_metadata["r2"],
            },
            "model_path": self.model_path,
            "ensemble_available": bool(self.ensemble_config.get("models", [])),
            "prediction_ready": True,
        }


# Example usage functions
def create_prediction_service(
    property_type: str = "house",
) -> RealEstatePredictionPipeline:
    """
    🚀 Create prediction service cho property type

    Args:
        property_type: Type of property

    Returns:
        RealEstatePredictionPipeline: Ready-to-use prediction service
    """
    loader = RealEstateModelLoader()
    latest_model_path = loader.get_latest_model_path(property_type)

    return RealEstatePredictionPipeline(latest_model_path, loader.spark)


def predict_property_price(
    property_data: Dict[str, Any], property_type: str = "house"
) -> Dict[str, Any]:
    """
    🏠 Simple function để predict property price

    Args:
        property_data: Property features
        property_type: Type of property

    Returns:
        Dict: Prediction result
    """
    pipeline = create_prediction_service(property_type)
    return pipeline.predict_single(property_data)


if __name__ == "__main__":
    # Example usage
    logger.info("🔮 Testing Real Estate Model Loader")

    # Test data
    sample_property = {
        "area": 80.0,
        "latitude": 10.7769,
        "longitude": 106.7009,
        "bedrooms": 2,
        "bathrooms": 2,
        "city": "Ho Chi Minh",
        "district": "District 1",
        "property_type": "apartment",
    }

    try:
        # Create prediction service
        pipeline = create_prediction_service("house")

        # Get model info
        model_info = pipeline.get_model_info()
        logger.info(f"Model loaded: {model_info}")

        # Make prediction
        result = pipeline.predict_single(sample_property)
        logger.info(f"Prediction result: {result}")

    except Exception as e:
        logger.error(f"❌ Prediction test failed: {e}")
