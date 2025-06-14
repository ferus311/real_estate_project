"""
ML Service for Real Estate Price Prediction
Load models from HDFS and perform predictions
"""

import os
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from pathlib import Path

# HDFS and Spark imports
try:
    from hdfs import InsecureClient
    from pyspark.sql import SparkSession
    from pyspark.ml import Pipeline

    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False

logger = logging.getLogger(__name__)


class MLModelService:
    """Service để load và sử dụng ML models từ HDFS"""

    def __init__(self):
        self.hdfs_client = None
        self.spark = None
        self.models = {}
        self.scaler = None
        self.feature_columns = [
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
        ]
        self._init_hdfs_client()
        self._init_spark_session()

    def _init_hdfs_client(self):
        """Khởi tạo HDFS client"""
        if not HDFS_AVAILABLE:
            logger.warning("HDFS not available, using local fallback")
            return

        try:
            # Thử kết nối HDFS (namenode thường chạy port 9870 cho web, 9000 cho API)
            self.hdfs_client = InsecureClient("http://namenode:9870")
            logger.info("HDFS client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize HDFS client: {e}")

    def _init_spark_session(self):
        """Khởi tạo Spark session cho load models"""
        if not HDFS_AVAILABLE:
            return

        try:
            self.spark = (
                SparkSession.builder.appName("RealEstate_ML_Service")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )
            logger.info("Spark session initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")

    def find_latest_model_path(self, property_type: str = "house") -> Optional[str]:
        """Tìm model mới nhất trên HDFS"""
        if not self.hdfs_client:
            return None

        try:
            base_path = f"/data/realestate/processed/ml/models/{property_type}"

            # List tất cả directories theo năm
            years = []
            try:
                for item in self.hdfs_client.list(base_path):
                    if item.isdigit() and len(item) == 4:  # Year format YYYY
                        years.append(item)
            except Exception:
                logger.error(f"Path {base_path} not found on HDFS")
                return None

            if not years:
                return None

            # Sắp xếp years giảm dần để lấy năm mới nhất
            years.sort(reverse=True)

            # Tìm model mới nhất
            for year in years:
                year_path = f"{base_path}/{year}"
                months = []

                try:
                    for item in self.hdfs_client.list(year_path):
                        if item.isdigit() and len(item) == 2:  # Month format MM
                            months.append(item)
                except Exception:
                    continue

                if not months:
                    continue

                months.sort(reverse=True)

                for month in months:
                    month_path = f"{year_path}/{month}"
                    days = []

                    try:
                        for item in self.hdfs_client.list(month_path):
                            if item.isdigit() and len(item) == 2:  # Day format DD
                                days.append(item)
                    except Exception:
                        continue

                    if not days:
                        continue

                    days.sort(reverse=True)

                    for day in days:
                        model_path = f"{month_path}/{day}"
                        # Check if models exist in this path
                        try:
                            files = self.hdfs_client.list(model_path)
                            if any("model" in f.lower() for f in files):
                                logger.info(f"Found latest model path: {model_path}")
                                return model_path
                        except Exception:
                            continue

            return None

        except Exception as e:
            logger.error(f"Error finding latest model path: {e}")
            return None

    def load_models_from_hdfs(self, property_type: str = "house") -> bool:
        """Load tất cả models từ HDFS path mới nhất"""
        model_path = self.find_latest_model_path(property_type)
        if not model_path:
            logger.error("No model path found on HDFS")
            return False

        try:
            # Model names theo định dạng trong training pipeline
            model_names = {
                "random_forest": "spark_random_forest_model",
                "gradient_boosting": "spark_gradient_boosting_model",
                "xgboost": "sklearn_xgboost_model.joblib",
                "lightgbm": "sklearn_lightgbm_model.joblib",
            }

            # Load từng model
            for model_key, model_filename in model_names.items():
                full_path = f"{model_path}/{model_filename}"

                try:
                    if model_filename.endswith(".joblib"):
                        # Sklearn models
                        local_path = f"/tmp/{model_filename}"
                        self.hdfs_client.download(full_path, local_path)
                        model = joblib.load(local_path)
                        os.remove(local_path)  # Cleanup
                        self.models[model_key] = model
                        logger.info(f"Loaded sklearn model: {model_key}")

                    else:
                        # Spark ML models
                        if self.spark:
                            from pyspark.ml.regression import (
                                RandomForestRegressor,
                                GBTRegressor,
                            )

                            if "random_forest" in model_filename:
                                model = RandomForestRegressor.load(full_path)
                            elif "gradient_boosting" in model_filename:
                                model = GBTRegressor.load(full_path)
                            else:
                                continue

                            self.models[model_key] = model
                            logger.info(f"Loaded Spark model: {model_key}")

                except Exception as e:
                    logger.warning(f"Failed to load model {model_key}: {e}")
                    continue

            # Load scaler
            try:
                scaler_path = f"{model_path}/preprocessing_scaler.joblib"
                local_scaler_path = "/tmp/preprocessing_scaler.joblib"
                self.hdfs_client.download(scaler_path, local_scaler_path)
                self.scaler = joblib.load(local_scaler_path)
                os.remove(local_scaler_path)
                logger.info("Loaded preprocessing scaler")
            except Exception as e:
                logger.warning(f"Failed to load scaler: {e}")

            logger.info(f"Successfully loaded {len(self.models)} models from HDFS")
            return len(self.models) > 0

        except Exception as e:
            logger.error(f"Error loading models from HDFS: {e}")
            return False

    def prepare_features(self, input_data: Dict[str, Any]) -> Optional[np.ndarray]:
        """Chuẩn bị features cho prediction"""
        try:
            # Tạo DataFrame từ input
            df = pd.DataFrame([input_data])

            # Đảm bảo có đủ tất cả features
            for col in self.feature_columns:
                if col not in df.columns:
                    df[col] = 0  # Default value

            # Sắp xếp theo thứ tự features
            df = df[self.feature_columns]

            # Apply scaler nếu có
            if self.scaler:
                features = self.scaler.transform(df.values)
            else:
                features = df.values

            return features

        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return None

    def predict_with_model(
        self, model_name: str, features: np.ndarray
    ) -> Optional[float]:
        """Predict với một model cụ thể"""
        if model_name not in self.models:
            return None

        try:
            model = self.models[model_name]

            if model_name in ["xgboost", "lightgbm"]:
                # Sklearn models
                prediction = model.predict(features)[0]
            else:
                # Spark ML models
                if self.spark:
                    # Convert to Spark DataFrame
                    columns = [f"feature_{i}" for i in range(features.shape[1])]
                    df = self.spark.createDataFrame([tuple(features[0])], columns)

                    # Tạo feature vector
                    from pyspark.ml.feature import VectorAssembler

                    assembler = VectorAssembler(inputCols=columns, outputCol="features")
                    df_features = assembler.transform(df)

                    # Predict
                    predictions = model.transform(df_features)
                    prediction = predictions.select("prediction").collect()[0][0]
                else:
                    return None

            return float(prediction)

        except Exception as e:
            logger.error(f"Error predicting with {model_name}: {e}")
            return None

    def predict_all_models(
        self, input_data: Dict[str, Any]
    ) -> Dict[str, Optional[float]]:
        """Predict với tất cả models"""
        # Prepare features
        features = self.prepare_features(input_data)
        if features is None:
            return {
                model: None
                for model in [
                    "random_forest",
                    "gradient_boosting",
                    "xgboost",
                    "lightgbm",
                ]
            }

        results = {}
        for model_name in ["random_forest", "gradient_boosting", "xgboost", "lightgbm"]:
            results[model_name] = self.predict_with_model(model_name, features)

        return results

    def get_model_info(self) -> Dict[str, Any]:
        """Lấy thông tin về models đã load"""
        return {
            "models_loaded": list(self.models.keys()),
            "total_models": len(self.models),
            "scaler_loaded": self.scaler is not None,
            "hdfs_available": self.hdfs_client is not None,
            "spark_available": self.spark is not None,
        }


# Khởi tạo global instance
ml_service = MLModelService()

# Load models khi import module
try:
    ml_service.load_models_from_hdfs()
    logger.info("ML models loaded successfully on startup")
except Exception as e:
    logger.error(f"Failed to load ML models on startup: {e}")
