"""
ML Service for loading and using trained models from HDFS
Supports both Spark ML and Sklearn models for price prediction
"""

import os
import json
import joblib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.regression import (
    LinearRegressionModel,
    RandomForestRegressionModel,
    GBTRegressionModel,
)
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
)

# Try to import sklearn models
try:
    import xgboost as xgb
    import lightgbm as lgb
    from catboost import CatBoostRegressor

    SKLEARN_MODELS_AVAILABLE = True
except ImportError:
    SKLEARN_MODELS_AVAILABLE = False
    print("⚠️ Some ML libraries not available (XGBoost, LightGBM, CatBoost)")

logger = logging.getLogger(__name__)


class HDFSModelLoader:
    """Service to load and manage ML models from HDFS"""

    def __init__(self):
        self.spark = self._create_spark_session()
        self.base_model_path = "/data/realestate/processed/ml/models"
        self._loaded_models = {}
        self._preprocessing_pipeline = None

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for model loading"""
        try:
            spark = (
                SparkSession.builder.appName("RealEstate_Model_Inference")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config(
                    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                )
                .getOrCreate()
            )

            logger.info("✅ Spark session created for model inference")
            return spark
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            raise

    def find_latest_model_path(self, property_type: str = "house") -> Optional[str]:
        """Find the latest trained model path on HDFS"""
        try:
            # Get Hadoop FileSystem
            hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_config
            )

            base_path = f"{self.base_model_path}/{property_type}"
            base_hadoop_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                base_path
            )

            if not fs.exists(base_hadoop_path):
                logger.error(f"❌ Model base path not found: {base_path}")
                return None

            # List all date directories
            date_dirs = []
            date_iterator = fs.listStatus(base_hadoop_path)

            for i in range(len(date_iterator)):
                status = date_iterator[i]
                if status.isDirectory():
                    dir_name = status.getPath().getName()
                    # Try to parse as date (YYYY-MM-DD format)
                    try:
                        date_obj = datetime.strptime(dir_name, "%Y-%m-%d")
                        date_dirs.append((dir_name, date_obj))
                    except ValueError:
                        continue

            if not date_dirs:
                logger.error(f"❌ No valid date directories found in {base_path}")
                return None

            # Sort by date and get the latest
            date_dirs.sort(key=lambda x: x[1], reverse=True)
            latest_date = date_dirs[0][0]
            latest_path = f"{base_path}/{latest_date}"

            logger.info(f"✅ Found latest model path: {latest_path}")
            return latest_path

        except Exception as e:
            logger.error(f"❌ Error finding latest model path: {e}")
            return None

    def load_model_registry(self, model_path: str) -> Optional[Dict]:
        """Load model registry information"""
        try:
            registry_path = f"{model_path}/model_registry.json"

            # Check if registry exists
            hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_config
            )
            registry_hadoop_path = (
                self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(registry_path)
            )

            if not fs.exists(registry_hadoop_path):
                logger.warning(f"⚠️ Model registry not found: {registry_path}")
                return None

            # Read registry using Spark
            registry_df = self.spark.read.json(registry_path)
            registry_data = registry_df.collect()[0]["registry"]

            # Parse JSON string if needed
            if isinstance(registry_data, str):
                registry = json.loads(registry_data)
            else:
                registry = registry_data

            logger.info(
                f"✅ Model registry loaded: {registry.get('model_version', 'unknown')}"
            )
            return registry

        except Exception as e:
            logger.error(f"❌ Error loading model registry: {e}")
            return None

    def load_preprocessing_pipeline(self, model_path: str) -> Optional[PipelineModel]:
        """Load preprocessing pipeline (VectorAssembler + StandardScaler)"""
        try:
            pipeline_path = f"{model_path}/preprocessing_pipeline"

            # Check if pipeline exists
            hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_config
            )
            pipeline_hadoop_path = (
                self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(pipeline_path)
            )

            if not fs.exists(pipeline_hadoop_path):
                logger.error(f"❌ Preprocessing pipeline not found: {pipeline_path}")
                return None

            # Load pipeline model
            pipeline_model = PipelineModel.load(pipeline_path)
            logger.info("✅ Preprocessing pipeline loaded")
            return pipeline_model

        except Exception as e:
            logger.error(f"❌ Error loading preprocessing pipeline: {e}")
            return None

    def load_spark_model(self, model_path: str, model_name: str) -> Optional[Any]:
        """Load Spark ML model"""
        try:
            spark_model_path = f"{model_path}/spark_models/{model_name}"

            # Check if model exists
            hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_config
            )
            model_hadoop_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                spark_model_path
            )

            if not fs.exists(model_hadoop_path):
                logger.warning(f"⚠️ Spark model not found: {spark_model_path}")
                return None

            # Load appropriate model type
            if "linear_regression" in model_name:
                model = LinearRegressionModel.load(spark_model_path)
            elif "random_forest" in model_name:
                model = RandomForestRegressionModel.load(spark_model_path)
            elif "gbt" in model_name:
                model = GBTRegressionModel.load(spark_model_path)
            else:
                logger.error(f"❌ Unknown Spark model type: {model_name}")
                return None

            logger.info(f"✅ Spark model loaded: {model_name}")
            return model

        except Exception as e:
            logger.error(f"❌ Error loading Spark model {model_name}: {e}")
            return None

    def load_sklearn_model(self, model_path: str, model_name: str) -> Optional[Any]:
        """Load sklearn model using joblib"""
        if not SKLEARN_MODELS_AVAILABLE:
            logger.warning("⚠️ Sklearn models not available")
            return None

        try:
            sklearn_model_path = f"{model_path}/sklearn_models/{model_name}.joblib"

            # Check if model exists
            hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_config
            )
            model_hadoop_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                sklearn_model_path
            )

            if not fs.exists(model_hadoop_path):
                logger.warning(f"⚠️ Sklearn model not found: {sklearn_model_path}")
                return None

            # Download model to local temp file
            input_stream = fs.open(model_hadoop_path)
            local_temp_path = (
                f"/tmp/{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"
            )

            with open(local_temp_path, "wb") as f:
                # Read from HDFS and write to local
                buffer_size = 8192
                while True:
                    data = input_stream.read(buffer_size)
                    if len(data) == 0:
                        break
                    f.write(data)

            input_stream.close()

            # Load model using joblib
            model = joblib.load(local_temp_path)

            # Clean up temp file
            os.remove(local_temp_path)

            logger.info(f"✅ Sklearn model loaded: {model_name}")
            return model

        except Exception as e:
            logger.error(f"❌ Error loading sklearn model {model_name}: {e}")
            return None

    def load_all_models(self, property_type: str = "house") -> Dict[str, Any]:
        """Load all available models for a property type"""
        try:
            # Find latest model path
            model_path = self.find_latest_model_path(property_type)
            if not model_path:
                return {}

            # Load model registry
            registry = self.load_model_registry(model_path)
            if not registry:
                logger.warning("⚠️ No model registry found, loading models blindly")
                registry = {"all_models": {}}

            # Load preprocessing pipeline
            self._preprocessing_pipeline = self.load_preprocessing_pipeline(model_path)
            if not self._preprocessing_pipeline:
                logger.error("❌ Failed to load preprocessing pipeline")
                return {}

            loaded_models = {}
            all_models = registry.get("all_models", {})

            # Load Spark models
            for model_name, model_info in all_models.items():
                if model_info.get("model_type") == "spark":
                    model = self.load_spark_model(model_path, model_name)
                    if model:
                        loaded_models[model_name] = {
                            "model": model,
                            "type": "spark",
                            "metrics": model_info,
                        }

            # Load sklearn models
            if SKLEARN_MODELS_AVAILABLE:
                for model_name, model_info in all_models.items():
                    if model_info.get("model_type") == "sklearn":
                        model = self.load_sklearn_model(model_path, model_name)
                        if model:
                            loaded_models[model_name] = {
                                "model": model,
                                "type": "sklearn",
                                "metrics": model_info,
                            }

            self._loaded_models = loaded_models
            logger.info(
                f"✅ Loaded {len(loaded_models)} models: {list(loaded_models.keys())}"
            )

            return loaded_models

        except Exception as e:
            logger.error(f"❌ Error loading models: {e}")
            return {}

    def get_best_model(self) -> Optional[Tuple[str, Dict]]:
        """Get the best performing model"""
        if not self._loaded_models:
            return None

        # Find model with highest R2 score
        best_name = None
        best_r2 = -1
        best_model = None

        for name, model_info in self._loaded_models.items():
            r2 = model_info.get("metrics", {}).get("r2", 0)
            if r2 > best_r2:
                best_r2 = r2
                best_name = name
                best_model = model_info

        return (best_name, best_model) if best_model else None

    def prepare_features(self, input_data: Dict) -> Optional[Any]:
        """Prepare features using the preprocessing pipeline"""
        if not self._preprocessing_pipeline:
            logger.error("❌ No preprocessing pipeline available")
            return None

        try:
            # Expected features from training
            expected_features = [
                # Core features (4)
                "area",
                "latitude",
                "longitude",
                # Room characteristics (3)
                "bedroom",
                "bathroom",
                "floor_count",
                # House characteristics (3)
                "house_direction_code",
                "legal_status_code",
                "interior_code",
                # Administrative (3)
                "province_id",
                "district_id",
                "ward_id",
                # Engineered features (4) - will be created by preprocessing
                "total_rooms",
                "area_per_room",
                "bedroom_bathroom_ratio",
                "population_density",
            ]

            # Create base features from input
            base_features = {}
            for feature in expected_features[:12]:  # First 12 are provided by user
                if feature in input_data:
                    base_features[feature] = float(input_data[feature])
                else:
                    logger.warning(f"⚠️ Missing feature: {feature}")
                    base_features[feature] = 0.0

            # Add engineered features (these should be calculated)
            base_features["total_rooms"] = base_features.get(
                "bedroom", 0
            ) + base_features.get("bathroom", 0)
            base_features["area_per_room"] = base_features.get("area", 0) / max(
                base_features["total_rooms"], 1
            )
            base_features["bedroom_bathroom_ratio"] = base_features.get(
                "bedroom", 0
            ) / max(base_features.get("bathroom", 1), 1)
            base_features["population_density"] = (
                1000.0  # Default value - should be looked up by location
            )

            # Create Spark DataFrame
            schema = StructType(
                [StructField(name, DoubleType(), True) for name in expected_features]
            )

            row_data = [
                tuple(base_features.get(name, 0.0) for name in expected_features)
            ]
            df = self.spark.createDataFrame(row_data, schema)

            # Apply preprocessing pipeline
            processed_df = self._preprocessing_pipeline.transform(df)

            return processed_df

        except Exception as e:
            logger.error(f"❌ Error preparing features: {e}")
            return None

    def predict_price(
        self, input_data: Dict, model_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Predict price using specified models or all available models"""
        try:
            if not self._loaded_models:
                self.load_all_models()

            if not self._loaded_models:
                return {"error": "No models available"}

            # Prepare features
            processed_df = self.prepare_features(input_data)
            if processed_df is None:
                return {"error": "Failed to prepare features"}

            # Get models to use
            models_to_use = (
                model_names if model_names else list(self._loaded_models.keys())
            )

            predictions = {}

            for model_name in models_to_use:
                if model_name not in self._loaded_models:
                    continue

                model_info = self._loaded_models[model_name]
                model = model_info["model"]
                model_type = model_info["type"]

                try:
                    if model_type == "spark":
                        # Spark model prediction
                        pred_df = model.transform(processed_df)
                        prediction = pred_df.select("prediction").collect()[0][
                            "prediction"
                        ]

                    elif model_type == "sklearn":
                        # Convert Spark DataFrame to numpy array for sklearn
                        features_row = processed_df.select("features").collect()[0]
                        features_array = np.array(
                            features_row["features"].toArray()
                        ).reshape(1, -1)
                        prediction = model.predict(features_array)[0]

                    predictions[model_name] = {
                        "predicted_price": float(prediction),
                        "model_type": model_type,
                        "model_metrics": model_info.get("metrics", {}),
                    }

                except Exception as model_error:
                    logger.error(f"❌ Error with model {model_name}: {model_error}")
                    predictions[model_name] = {"error": str(model_error)}

            # Calculate ensemble prediction if multiple models
            if len(predictions) > 1:
                valid_predictions = [
                    pred["predicted_price"]
                    for pred in predictions.values()
                    if "predicted_price" in pred
                ]
                if valid_predictions:
                    ensemble_prediction = np.mean(valid_predictions)
                    predictions["ensemble"] = {
                        "predicted_price": float(ensemble_prediction),
                        "model_type": "ensemble",
                        "individual_predictions": len(valid_predictions),
                    }

            return {
                "success": True,
                "predictions": predictions,
                "input_features": input_data,
                "models_used": len(predictions),
            }

        except Exception as e:
            logger.error(f"❌ Error in price prediction: {e}")
            return {"error": str(e)}

    def cleanup(self):
        """Clean up resources"""
        try:
            if self.spark:
                self.spark.stop()
                logger.info("✅ Spark session stopped")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


# Global model loader instance
_model_loader = None


def get_model_loader() -> HDFSModelLoader:
    """Get singleton model loader instance"""
    global _model_loader
    if _model_loader is None:
        _model_loader = HDFSModelLoader()
    return _model_loader
