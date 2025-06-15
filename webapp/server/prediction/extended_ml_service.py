"""
Extended ML Service for price prediction with Spark model support
Supports both sklearn models (XGBoost, LightGBM) and Spark ML models (RandomForest, LinearRegression)
"""

import os
import json
import joblib
import logging
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Any
import numpy as np
import pandas as pd

# Try to import HDFS client
try:
    from hdfs import InsecureClient

    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    print("⚠️ HDFS client not available")

# Try to import sklearn models
try:
    import xgboost as xgb
    import lightgbm as lgb

    SKLEARN_MODELS_AVAILABLE = True
except ImportError:
    SKLEARN_MODELS_AVAILABLE = False
    print("⚠️ Some ML libraries not available (XGBoost, LightGBM)")

# Try to import PySpark for Spark model loading
try:
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import RandomForestRegressor, LinearRegression
    from pyspark.ml import Pipeline, PipelineModel

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️ PySpark not available - Spark models will not be supported")

# Import base service
try:
    from .simple_ml_service import SimpleModelLoader

    BASE_SERVICE_AVAILABLE = True
except ImportError:
    BASE_SERVICE_AVAILABLE = False
    print("⚠️ Base SimpleModelLoader not available")

logger = logging.getLogger(__name__)


class ExtendedModelLoader:
    """Extended service to load both sklearn and Spark ML models"""

    def __init__(self):
        self.base_loader = SimpleModelLoader() if BASE_SERVICE_AVAILABLE else None
        self.spark_session = None
        self.spark_available = SPARK_AVAILABLE
        self._spark_models = {}

        # Initialize Spark session if available
        if self.spark_available:
            self._init_spark_session()

        logger.info(f"🚀 Initializing ExtendedModelLoader...")
        logger.info(f"📦 HDFS Available: {HDFS_AVAILABLE}")
        logger.info(f"🧠 Sklearn Models Available: {SKLEARN_MODELS_AVAILABLE}")
        logger.info(f"⚡ Spark Available: {self.spark_available}")

    def _init_spark_session(self):
        """Initialize Spark session for loading Spark models"""
        try:
            self.spark_session = (
                SparkSession.builder.appName("ExtendedMLService")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.maxResultSize", "1g")
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .getOrCreate()
            )

            # Set log level to reduce noise
            self.spark_session.sparkContext.setLogLevel("WARN")

            logger.info("✅ Spark session initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark session: {e}")
            self.spark_available = False
            self.spark_session = None

    def load_spark_model(self, model_name: str, model_path: str) -> Optional[Dict]:
        """Load Spark ML model from HDFS"""
        if not self.spark_available or not self.spark_session:
            logger.warning("⚠️ Spark not available, cannot load Spark models")
            return None

        try:
            spark_model_path = f"{model_path}/spark_models/{model_name}"
            logger.info(f"🔄 Loading Spark model from: {spark_model_path}")

            # Load the Spark model
            if model_name == "random_forest":
                from pyspark.ml.regression import RandomForestRegressionModel

                model = RandomForestRegressionModel.load(spark_model_path)
            elif model_name == "linear_regression":
                from pyspark.ml.regression import LinearRegressionModel

                model = LinearRegressionModel.load(spark_model_path)
            else:
                logger.error(f"❌ Unknown Spark model type: {model_name}")
                return None

            # Get metrics from registry if available
            model_metrics = {}
            if self.base_loader and self.base_loader._model_registry:
                registry = self.base_loader._model_registry
                if model_name in registry.get("all_models", {}):
                    registry_model = registry["all_models"][model_name]
                    model_metrics = {
                        "rmse": registry_model.get("rmse", 0),
                        "mae": registry_model.get("mae", 0),
                        "r2": registry_model.get("r2", 0),
                    }

            logger.info(f"✅ Successfully loaded Spark model: {model_name}")

            return {
                "model": model,
                "type": "spark",
                "name": model_name,
                "metrics": model_metrics,
                "loaded_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"❌ Error loading Spark model {model_name}: {e}")
            return None

    def load_all_models(self) -> Dict[str, bool]:
        """Load all available models (sklearn + Spark)"""
        results = {}

        # Load sklearn models using base loader
        if self.base_loader:
            sklearn_results = self.base_loader.load_all_models()
            results.update(sklearn_results)

            # Copy loaded sklearn models
            for name, model_data in self.base_loader._loaded_models.items():
                self._spark_models[name] = model_data

        # Load Spark models
        if (
            self.spark_available
            and self.base_loader
            and self.base_loader._latest_model_path
        ):
            spark_model_names = ["random_forest", "linear_regression"]

            for model_name in spark_model_names:
                try:
                    logger.info(f"🔄 Loading Spark {model_name} model...")
                    model_data = self.load_spark_model(
                        model_name, self.base_loader._latest_model_path
                    )

                    if model_data:
                        self._spark_models[model_name] = model_data
                        results[model_name] = True
                        logger.info(f"✅ Successfully loaded Spark {model_name}")
                    else:
                        results[model_name] = False
                        logger.error(f"❌ Failed to load Spark {model_name}")

                except Exception as e:
                    logger.error(f"❌ Exception loading Spark {model_name}: {e}")
                    results[model_name] = False

        successful_loads = sum(results.values())
        total_attempts = len(results)
        logger.info(
            f"📊 Loaded {successful_loads}/{total_attempts} models successfully"
        )

        return results

    def predict_with_spark_model(
        self, model_name: str, features: List[float]
    ) -> Optional[float]:
        """Make prediction using Spark model"""
        if (
            model_name not in self._spark_models
            or self._spark_models[model_name]["type"] != "spark"
        ):
            logger.error(f"❌ Spark model {model_name} not loaded")
            return None

        if not self.spark_session:
            logger.error("❌ Spark session not available")
            return None

        try:
            model_data = self._spark_models[model_name]
            model = model_data["model"]

            # Convert features to Spark DataFrame - use the same 16 features as sklearn
            feature_names = [
                "area",
                "latitude",
                "longitude",
                "bedroom",
                "bathroom",
                "floor_count",
                "house_direction_code",
                "legal_status_code",
                "interior_code",
                "province_id",
                "district_id",
                "ward_id",
                "total_rooms",
                "area_per_room",
                "bedroom_bathroom_ratio",
                "population_density",
            ]

            # Ensure we have the right number of features
            if len(features) != len(feature_names):
                logger.error(
                    f"❌ Feature count mismatch: expected {len(feature_names)}, got {len(features)}"
                )
                return None

            # Create DataFrame with proper column names
            data = dict(zip(feature_names, features))
            df = self.spark_session.createDataFrame([data])

            # Create feature vector using VectorAssembler
            assembler = VectorAssembler(inputCols=feature_names, outputCol="features")

            feature_df = assembler.transform(df)

            # Make prediction
            prediction_df = model.transform(feature_df)

            # Extract prediction value
            prediction_row = prediction_df.select("prediction").collect()[0]
            prediction_value = float(prediction_row["prediction"])

            logger.info(f"✅ Spark {model_name} prediction: {prediction_value:.2f}")

            return prediction_value

        except Exception as e:
            logger.error(f"❌ Error making prediction with Spark {model_name}: {e}")
            return None

    def predict_with_sklearn_model(
        self, model_name: str, features: List[float]
    ) -> Optional[float]:
        """Make prediction using sklearn model (delegate to base loader)"""
        if (
            model_name not in self._spark_models
            or self._spark_models[model_name]["type"] != "sklearn"
        ):
            logger.error(f"❌ Sklearn model {model_name} not loaded")
            return None

        try:
            model_data = self._spark_models[model_name]
            model = model_data["model"]

            # Convert features to numpy array and reshape for prediction
            feature_array = np.array(features).reshape(1, -1)

            # Make prediction
            prediction = model.predict(feature_array)[0]

            logger.info(f"✅ Sklearn {model_name} prediction: {prediction:.2f}")

            return float(prediction)

        except Exception as e:
            logger.error(f"❌ Error making prediction with sklearn {model_name}: {e}")
            return None

    def predict_all_models(self, features: List[float]) -> Dict[str, Any]:
        """Make predictions using all loaded models"""
        predictions = {}

        for model_name, model_data in self._spark_models.items():
            model_type = model_data["type"]

            try:
                if model_type == "sklearn":
                    prediction = self.predict_with_sklearn_model(model_name, features)
                elif model_type == "spark":
                    prediction = self.predict_with_spark_model(model_name, features)
                else:
                    logger.warning(f"⚠️ Unknown model type: {model_type}")
                    continue

                if prediction is not None:
                    predictions[model_name] = {
                        "prediction": prediction,
                        "type": model_type,
                        "metrics": model_data.get("metrics", {}),
                    }
                else:
                    predictions[model_name] = {
                        "prediction": None,
                        "type": model_type,
                        "error": "Prediction failed",
                    }

            except Exception as e:
                logger.error(f"❌ Error predicting with {model_name}: {e}")
                predictions[model_name] = {
                    "prediction": None,
                    "type": model_type,
                    "error": str(e),
                }

        return predictions

    def get_loaded_models(self) -> Dict[str, Dict]:
        """Get information about loaded models"""
        model_info = {}

        for model_name, model_data in self._spark_models.items():
            model_info[model_name] = {
                "type": model_data["type"],
                "metrics": model_data.get("metrics", {}),
                "loaded_at": model_data.get("loaded_at"),
            }

        return model_info

    def close(self):
        """Clean up resources"""
        if self.spark_session:
            try:
                self.spark_session.stop()
                logger.info("✅ Spark session stopped")
            except Exception as e:
                logger.error(f"❌ Error stopping Spark session: {e}")

    def preprocess_input_data(self, input_data: Dict) -> Dict[str, Any]:
        """Process input data and convert to feature vector"""
        if self.base_loader:
            # Use base loader's preprocessing logic
            return self.base_loader._preprocess_input_data(input_data)
        else:
            # Fallback: manual preprocessing
            try:
                # Same logic as simple_ml_service.py
                area = float(input_data.get("area", 80))
                latitude = float(input_data.get("latitude", 10.762622))
                longitude = float(input_data.get("longitude", 106.660172))
                bedroom = int(input_data.get("bedroom", 3))
                bathroom = int(input_data.get("bathroom", 2))
                floor_count = int(input_data.get("floor_count", 3))
                house_direction_code = int(input_data.get("house_direction_code", 3))
                legal_status_code = int(input_data.get("legal_status_code", 1))
                interior_code = int(input_data.get("interior_code", 2))
                province_id = int(input_data.get("province_id", 79))
                district_id = int(input_data.get("district_id", 769))
                ward_id = int(input_data.get("ward_id", 27000))

                # Engineered features
                total_rooms = bedroom + bathroom
                area_per_room = area / max(total_rooms, 1)
                bedroom_bathroom_ratio = bedroom / max(bathroom, 1)

                # Population density lookup (same as simple_ml_service.py)
                population_density_map = {
                    1: 4513.1,
                    79: 4513.1,
                    2: 2555.8,
                    48: 969.2,
                    # Add more as needed
                }
                population_density = population_density_map.get(province_id, 800.0)

                # Feature vector
                features = [
                    area,
                    latitude,
                    longitude,
                    bedroom,
                    bathroom,
                    floor_count,
                    house_direction_code,
                    legal_status_code,
                    interior_code,
                    province_id,
                    district_id,
                    ward_id,
                    total_rooms,
                    area_per_room,
                    bedroom_bathroom_ratio,
                    population_density,
                ]

                return {
                    "success": True,
                    "features": features,
                    "input_features": {
                        "raw_input": input_data,
                        "processed_features": dict(
                            zip(
                                [
                                    "area",
                                    "latitude",
                                    "longitude",
                                    "bedroom",
                                    "bathroom",
                                    "floor_count",
                                    "house_direction_code",
                                    "legal_status_code",
                                    "interior_code",
                                    "province_id",
                                    "district_id",
                                    "ward_id",
                                    "total_rooms",
                                    "area_per_room",
                                    "bedroom_bathroom_ratio",
                                    "population_density",
                                ],
                                features,
                            )
                        ),
                        "feature_vector": features,
                        "feature_count": len(features),
                    },
                }

            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to preprocess input data: {e}",
                }


# Create global instance
extended_loader = None


def get_extended_loader() -> ExtendedModelLoader:
    """Get or create global extended loader instance"""
    global extended_loader
    if extended_loader is None:
        extended_loader = ExtendedModelLoader()
    return extended_loader
