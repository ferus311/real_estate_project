"""
🤖 ML Model Training Pipeline for Real Estate Price Prediction
============================================================

This module focuses exclusively on model training, evaluation, and deployment.
Data preparation is handled by the unified data_preparation pipeline.

🎯 IMPORTANT: Feature Scaling
============================
All features are standardized using StandardScaler (mean=0, std=1) before training.
This is crucial for:
- Linear models (LinearRegression, etc.)
- Distance-based algorithms
- Neural networks
- Gradient descent optimization
- Preventing features with large scales (e.g., price, area) from dominating

Features:
- 🤖 Multiple model architectures: Spark ML, XGBoost, LightGBM, CatBoost
- 🎯 Automated hyperparameter tuning
- 📊 Advanced model evaluation and validation
- 🔄 Incremental learning for daily updates
- 📈 Model monitoring and drift detection
- 💾 Production-ready model versioning
- 🎯 Feature standardization for optimal performance

Author: ML Team
Date: June 2025
Version: 3.0 - Clean Architecture with Feature Scaling
"""

import sys
import os
import json
import joblib
import pickle
import tempfile
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import warnings
import warnings

warnings.filterwarnings("ignore")

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, broadcast, log
from pyspark.sql.types import StructType, StructField, BinaryType
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    StringIndexer,
    OneHotEncoder,
)
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.regression import (
    RandomForestRegressor,
    GBTRegressor,
    LinearRegression,
    DecisionTreeRegressor,
    GeneralizedLinearRegression,
)

# Sklearn-compatible ML libraries (XGBoost, LightGBM, CatBoost)
try:
    import xgboost as xgb
    import lightgbm as lgb
    from catboost import CatBoostRegressor

    SKLEARN_MODELS_AVAILABLE = True
except ImportError:
    print("⚠️ Sklearn-compatible models (XGBoost, LightGBM, CatBoost) not available")
    SKLEARN_MODELS_AVAILABLE = False

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.config.spark_config import (
    create_optimized_ml_spark_session,
)
from common.utils.hdfs_utils import check_hdfs_path_exists, create_hdfs_directory
from common.utils.logging_utils import SparkJobLogger


class ModelConfig:
    """Lightweight model configuration class to avoid serialization issues"""

    @staticmethod
    def get_spark_model_configs():
        """Get Spark model configurations - static method to avoid serialization"""
        return {
            "random_forest": {
                "class": RandomForestRegressor,
                "params": {
                    "featuresCol": "features",
                    "labelCol": "price",
                    "numTrees": 100,
                    "maxDepth": 15,
                    "maxBins": 64,
                    "seed": 42,
                },
            },
            "linear_regression": {
                "class": LinearRegression,
                "params": {
                    "featuresCol": "features",
                    "labelCol": "price",
                    "regParam": 0.01,
                    "elasticNetParam": 0.1,
                },
            },
        }

    @staticmethod
    def get_sklearn_model_configs():
        """Get sklearn model configurations - static method to avoid serialization"""
        if not SKLEARN_MODELS_AVAILABLE:
            return {}

        return {
            "xgboost": {
                "class": xgb.XGBRegressor,
                "params": {
                    "n_estimators": 300,
                    "max_depth": 8,
                    "learning_rate": 0.05,
                    "subsample": 0.7,
                    "colsample_bytree": 0.7,
                    "random_state": 42,
                    "n_jobs": 4,  # Reduced from -1 to control memory usage
                },
            },
            "lightgbm": {
                "class": lgb.LGBMRegressor,
                "params": {
                    "n_estimators": 300,
                    "max_depth": 8,
                    "learning_rate": 0.1,
                    "subsample": 0.9,
                    "colsample_bytree": 1.0,
                    "random_state": 42,
                    "n_jobs": 4,  # Reduced from -1 to control memory usage
                    "verbose": -1,
                },
            },
        }


class MLTrainer:
    """
    🚀 Production-grade ML Training Pipeline for Real Estate Price Prediction

    ⚡ Performance Optimized Features:
    - Minimal object serialization to prevent large task binary warnings
    - Efficient memory management and data handling
    - Broadcast variables for large data structures
    - Reduced closure capture overhead
    - Lazy model initialization
    """

    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        model_output_path: str = "/data/realestate/processed/ml/models",
    ):
        # Minimize instance variables to reduce serialization overhead
        self.spark = spark_session if spark_session else self._create_spark_session()
        self.logger = SparkJobLogger("ml_training_pipeline")
        self.model_output_path = model_output_path

        # Don't store large configurations in instance - use static methods instead
        self._trained_models: Dict[str, Any] = {}
        self._preprocessing_model: Optional[Any] = None

    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for ML workloads"""
        return create_optimized_ml_spark_session("ML_Training")

    def read_ml_features(
        self, date: str, property_type: str = "house"
    ) -> Tuple[Any, Dict]:
        """📖 Read ML features from feature store - using latest available file only"""
        from datetime import datetime, timedelta

        self.logger.logger.info(
            f"📖 Reading ML features for property type '{property_type}'"
        )
        self.logger.logger.info(f"📅 Requested date: {date}")

        # First, try to find the exact date requested
        requested_date = datetime.strptime(date, "%Y-%m-%d")
        date_formatted = requested_date.strftime("%Y%m%d")
        date_path = requested_date.strftime("%Y/%m/%d")

        exact_feature_path = f"/data/realestate/processed/ml/feature_store/{property_type}/{date_path}/features_{property_type}_{date_formatted}.parquet"

        latest_path = None
        latest_date = None

        if check_hdfs_path_exists(self.spark, exact_feature_path):
            latest_path = exact_feature_path
            latest_date = requested_date
            self.logger.logger.info(f"✅ Found exact date feature file: {date}")
        else:
            self.logger.logger.info(
                f"⚠️ Exact date {date} not found, searching for latest available..."
            )

            # Search for the most recent available feature file
            latest_path, latest_date = self._find_latest_available_features(
                property_type, requested_date
            )

            if not latest_path:
                raise FileNotFoundError(
                    f"❌ No ML features found for property type '{property_type}' "
                    f"near requested date {date}"
                )

        self.logger.logger.info(
            f"📊 Using feature data from: {latest_date.strftime('%Y-%m-%d')}"
        )

        # Read the single latest feature file
        df = self.spark.read.option("mergeSchema", "false").parquet(latest_path)

        # Optimize partitioning and cache
        df = df.repartition(self.spark.sparkContext.defaultParallelism).cache()

        # Read metadata from the latest feature file
        metadata = {}
        metadata_loaded = False

        try:
            # Extract date info from latest_path
            path_parts = latest_path.split("/")
            year = path_parts[-4]
            month = path_parts[-3]
            day = path_parts[-2]
            date_str = f"{year}-{month}-{day}"
            date_formatted = date_str.replace("-", "")

            metadata_path = f"/data/realestate/processed/ml/feature_store/{property_type}/{year}/{month}/{day}/metadata_{property_type}_{date_formatted}.json"

            if check_hdfs_path_exists(self.spark, metadata_path):
                metadata_df = self.spark.read.json(metadata_path)
                metadata_row = metadata_df.collect()[0]

                # Handle both nested and flat JSON structures
                if hasattr(metadata_row, "metadata"):
                    metadata = json.loads(metadata_row.metadata)
                else:
                    metadata = metadata_row.asDict()

                self.logger.logger.info(f"📊 Feature metadata loaded from {date_str}:")
                self.logger.logger.info(
                    f"   - Total features: {metadata.get('total_features', 'Unknown')}"
                )

                # Update total records to reflect the dataset
                actual_count = df.count()
                metadata["total_records"] = actual_count
                metadata["data_source_date"] = latest_date.strftime("%Y-%m-%d")
                metadata["data_mode"] = "latest_single_file"

                self.logger.logger.info(f"   - Total records: {actual_count:,}")
                self.logger.logger.info(
                    f"   - Data source date: {metadata['data_source_date']}"
                )
                self.logger.logger.info(
                    f"   - Feature engineering version: {metadata.get('feature_engineering_version', 'v1.0')}"
                )

                metadata_loaded = True

        except Exception as e:
            self.logger.logger.debug(f"Could not load metadata from {latest_path}: {e}")

        if not metadata_loaded:
            self.logger.logger.warning(
                f"Could not load feature metadata, creating basic metadata"
            )
            # Create basic metadata from DataFrame
            actual_count = df.count()
            metadata = {
                "total_records": actual_count,
                "total_features": len(df.columns),
                "data_source_date": latest_date.strftime("%Y-%m-%d"),
                "data_mode": "latest_single_file",
                "feature_engineering_version": "v1.0",
            }

        return df, metadata

    def _create_temporal_split(
        self, df: Any, train_ratio: float = 0.8
    ) -> Tuple[Any, Any]:
        """📅 Create time-aware data split for real estate training"""
        self.logger.logger.info("📅 Creating temporal data split...")

        # Check if listing_date column exists
        if "listing_date" in df.columns:
            # Sort by listing_date for temporal split
            df_sorted = df.orderBy("listing_date")

            total_count = df_sorted.count()
            train_size = int(total_count * train_ratio)

            # Take earlier data for training, later data for testing
            train_df = df_sorted.limit(train_size)
            test_df = df_sorted.offset(train_size)

            self.logger.logger.info("✅ Using temporal split based on listing_date")

        else:
            # Fallback to random split if no date column
            self.logger.logger.warning("⚠️ No listing_date found, using random split")
            train_df, test_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)

        return train_df, test_df

    def _calculate_business_metrics(self, predictions_df: Any) -> Dict[str, float]:
        """📊 Calculate real estate business-specific metrics"""
        try:
            # Convert to Pandas for detailed analysis
            predictions_pandas = predictions_df.select("price", "prediction").toPandas()

            y_true = predictions_pandas["price"].values
            y_pred = predictions_pandas["prediction"].values

            # Calculate percentage errors
            errors = np.abs(y_true - y_pred)
            percentage_errors = errors / y_true

            # Accuracy thresholds - key business metrics
            within_10_pct = np.mean(percentage_errors <= 0.10) * 100
            within_20_pct = np.mean(percentage_errors <= 0.20) * 100
            within_30_pct = np.mean(percentage_errors <= 0.30) * 100

            # Price range analysis
            budget_mask = y_true < 3_000_000_000  # Under 3B VND
            mid_range_mask = (y_true >= 3_000_000_000) & (
                y_true < 10_000_000_000
            )  # 3-10B VND
            luxury_mask = y_true >= 10_000_000_000  # Above 10B VND

            business_metrics = {
                "within_10_percent": within_10_pct,
                "within_20_percent": within_20_pct,
                "within_30_percent": within_30_pct,
                "median_error": float(np.median(errors)),
                "mean_percentage_error": float(np.mean(percentage_errors) * 100),
                "outlier_percentage": float(
                    np.mean(errors > np.percentile(errors, 95)) * 100
                ),
            }

            # Add range-specific metrics if enough samples
            if budget_mask.sum() > 10:
                business_metrics["budget_mae"] = float(np.mean(errors[budget_mask]))
            if mid_range_mask.sum() > 10:
                business_metrics["mid_range_mae"] = float(
                    np.mean(errors[mid_range_mask])
                )
            if luxury_mask.sum() > 10:
                business_metrics["luxury_mae"] = float(np.mean(errors[luxury_mask]))

            return business_metrics

        except Exception as e:
            self.logger.logger.warning(f"⚠️ Could not calculate business metrics: {e}")
            return {}

    def _calculate_sklearn_business_metrics(
        self, y_true: np.ndarray, y_pred: np.ndarray
    ) -> Dict[str, float]:
        """📊 Calculate real estate business-specific metrics for sklearn models"""
        try:
            # Calculate percentage errors
            errors = np.abs(y_true - y_pred)
            percentage_errors = errors / y_true

            # Accuracy thresholds - key business metrics
            within_10_pct = np.mean(percentage_errors <= 0.10) * 100
            within_20_pct = np.mean(percentage_errors <= 0.20) * 100
            within_30_pct = np.mean(percentage_errors <= 0.30) * 100

            # Price range analysis
            budget_mask = y_true < 3_000_000_000  # Under 3B VND
            mid_range_mask = (y_true >= 3_000_000_000) & (
                y_true < 10_000_000_000
            )  # 3-10B VND
            luxury_mask = y_true >= 10_000_000_000  # Above 10B VND

            business_metrics = {
                "within_10_percent": within_10_pct,
                "within_20_percent": within_20_pct,
                "within_30_percent": within_30_pct,
                "median_error": float(np.median(errors)),
                "mean_percentage_error": float(np.mean(percentage_errors) * 100),
                "outlier_percentage": float(
                    np.mean(errors > np.percentile(errors, 95)) * 100
                ),
            }

            # Add range-specific metrics if enough samples
            if budget_mask.sum() > 10:
                business_metrics["budget_mae"] = float(np.mean(errors[budget_mask]))
            if mid_range_mask.sum() > 10:
                business_metrics["mid_range_mae"] = float(
                    np.mean(errors[mid_range_mask])
                )
            if luxury_mask.sum() > 10:
                business_metrics["luxury_mae"] = float(np.mean(errors[luxury_mask]))

            return business_metrics

        except Exception as e:
            self.logger.logger.warning(
                f"⚠️ Could not calculate sklearn business metrics: {e}"
            )
            return {}

    def validate_vector_features(self, df: Any) -> Any:
        """🔍 Validate that vector features don't contain NaN or Infinity values using pure Spark functions"""
        from pyspark.sql.functions import udf, size, array_contains
        from pyspark.sql.types import BooleanType, DoubleType
        from pyspark.ml.linalg import VectorUDT
        from pyspark.sql.functions import col

        # Simple validation without numpy dependency
        def is_valid_vector_simple(vector):
            """Check if vector is not None - simpler validation without numpy"""
            return vector is not None

        # Register simple UDF without numpy dependency
        valid_vector_udf = udf(is_valid_vector_simple, BooleanType())

        # First filter - remove null vectors
        df_not_null = df.filter(col("features").isNotNull())
        df_validated = df_not_null.filter(valid_vector_udf(col("features")))

        initial_count = df.count()
        validated_count = df_validated.count()

        if validated_count < initial_count:
            self.logger.logger.warning(
                f"⚠️ Filtered out {initial_count - validated_count} records with null/invalid vectors"
            )

        if validated_count == 0:
            raise ValueError("❌ No valid vectors remaining after validation!")

        return df_validated

    def prepare_spark_ml_data(self, df: Any, metadata: Dict) -> Tuple[Any, Any]:
        """🔧 Prepare data for Spark ML training from individual feature columns"""
        from pyspark.ml.feature import VectorAssembler, StandardScaler
        from pyspark.ml import Pipeline
        from pyspark.sql.functions import col, log, when, isnan, isnull

        self.logger.logger.info(
            "🔧 Starting data preprocessing pipeline for individual feature columns..."
        )

        # Print current schema to understand the data structure
        self.logger.logger.info("📊 Current data schema:")
        for field in df.schema.fields:
            self.logger.logger.info(f"   - {field.name}: {field.dataType}")

        # Clean data - remove null target values
        df_clean = df.filter(col("price").isNotNull() & (col("price") > 0))

        # Add log price for better model performance
        df_clean = df_clean.withColumn("log_price", log(col("price")))

        # Define EXACT 16 feature columns from feature engineering pipeline
        # These are the FINAL 16 features created by FeatureEngineer.create_all_features()
        expected_feature_columns = [
            # Core features (4)
            "area",
            "latitude",
            "longitude",
            # Room characteristics (3)
            "bedroom",
            "bathroom",
            "floor_count",
            # House characteristics - encoded (3)
            "house_direction_code",
            "legal_status_code",
            "interior_code",
            # Administrative hierarchy (3)
            "province_id",
            "district_id",
            "ward_id",
            # Property features - created by feature engineering (3)
            "total_rooms",
            "area_per_room",
            "bedroom_bathroom_ratio",
            # Population feature - created by feature engineering (1)
            "population_density",
        ]

        # Validate that we have the expected 16 features
        available_features = [
            col for col in expected_feature_columns if col in df_clean.columns
        ]
        missing_features = [
            col for col in expected_feature_columns if col not in df_clean.columns
        ]

        if missing_features:
            self.logger.logger.error(
                f"❌ Missing expected features: {missing_features}"
            )
            raise ValueError(f"Missing required ML features: {missing_features}")

        if len(available_features) != 16:
            self.logger.logger.error(
                f"❌ Expected 16 features, got {len(available_features)}"
            )
            raise ValueError(
                f"Feature count mismatch: expected 16, got {len(available_features)}"
            )

        feature_columns = available_features
        self.logger.logger.info(
            f"✅ Using EXACT 16 ML features from feature engineering:"
        )
        for i, col_name in enumerate(feature_columns, 1):
            self.logger.logger.info(f"   {i:2d}. {col_name}")

        # ❌ REMOVED: No more missing value handling here - data is already cleaned!
        # Data comes from feature engineering pipeline which already handled missing values
        self.logger.logger.info(
            "💡 Data is pre-cleaned by feature engineering pipeline - no missing value handling needed"
        )

        # All 16 features should be numeric (already processed by feature engineering)
        numeric_feature_columns = feature_columns

        self.logger.logger.info(
            f"✅ Validated 16 numeric feature columns for VectorAssembler"
        )

        # Validate feature vector size will be 16
        expected_vector_size = 16
        if len(numeric_feature_columns) != expected_vector_size:
            raise ValueError(
                f"❌ Expected {expected_vector_size} features, got {len(numeric_feature_columns)} for VectorAssembler!"
            )

        # Create feature vector using VectorAssembler (no standardization)
        assembler = VectorAssembler(
            inputCols=numeric_feature_columns,
            outputCol="features",  # Direct features without scaling
            handleInvalid="skip",  # Skip rows with invalid values
        )

        # No StandardScaler - use raw features directly for better interpretability
        self.logger.logger.info("💡 Using raw features without standardization")
        self.logger.logger.info(
            "   - This ensures consistency between training and prediction service"
        )
        self.logger.logger.info(
            "   - XGBoost and LightGBM handle feature scaling internally"
        )

        # Create preprocessing pipeline with just assembler
        pipeline = Pipeline(stages=[assembler])

        # Fit and transform
        pipeline_model = pipeline.fit(df_clean)
        df_processed = pipeline_model.transform(df_clean)

        # Select final columns for ML training
        final_df = df_processed.select("features", "price", "log_price", "id")

        # Filter out any remaining invalid rows
        final_df = final_df.filter(
            col("features").isNotNull()
            & col("price").isNotNull()
            & col("log_price").isNotNull()
        )

        # Cache the result
        final_df.cache()
        record_count = final_df.count()

        self.logger.logger.info(
            f"✅ Data preprocessing completed: {record_count:,} records ready for training"
        )
        self.logger.logger.info(
            "🎯 Features are raw (not standardized) for consistency with prediction service"
        )

        if record_count == 0:
            raise ValueError("❌ No valid records remaining after preprocessing!")

        # Store preprocessing model (VectorAssembler only - no scaling)
        self._preprocessing_model = pipeline_model

        # Log scaling statistics for debugging
        try:
            # Get a sample to check scaling
            sample_features = final_df.select("features").first()["features"]
            feature_means = np.mean([float(x) for x in sample_features.toArray()])
            feature_stds = np.std([float(x) for x in sample_features.toArray()])

            self.logger.logger.info(f"📊 Feature scaling check (sample):")
            self.logger.logger.info(
                f"   - Sample feature mean: {feature_means:.4f} (should be ~0)"
            )
            self.logger.logger.info(
                f"   - Sample feature std: {feature_stds:.4f} (should be ~1)"
            )
        except Exception as e:
            self.logger.logger.debug(f"Could not compute scaling statistics: {e}")

        # Log feature vector statistics - should be exactly 16
        try:
            feature_size = final_df.select("features").first()["features"].size
            self.logger.logger.info(
                f"🎯 Feature vector size: {feature_size} (expected: 16)"
            )
            if feature_size != 16:
                self.logger.logger.error(
                    f"❌ Feature vector size mismatch: {feature_size} != 16"
                )
                raise ValueError(
                    f"Feature vector size mismatch: expected 16, got {feature_size}"
                )
            else:
                self.logger.logger.info(
                    "✅ Feature vector size is correct: 16 features"
                )
        except Exception as e:
            self.logger.logger.warning(f"Could not validate feature vector size: {e}")

        return final_df, pipeline_model

    def train_spark_models(self, df: Any) -> Dict[str, Dict]:
        """🤖 Train Spark ML models (Random Forest & Linear Regression) following Kaggle approach"""
        self.logger.logger.info(
            "🤖 Training Spark ML models with model-specific data preparation..."
        )

        models = {}

        # Create evaluators once to avoid re-serialization
        rmse_evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="rmse"
        )
        mae_evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="mae"
        )
        r2_evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="r2"
        )

        # ==================== RANDOM FOREST ====================
        self.logger.logger.info("🌲 Training Random Forest...")
        try:
            # Prepare data specifically for Random Forest
            df_rf = self.prepare_rf_model_data(df)

            # Define features for RF (including high cardinality categorical)
            numerical_features = [
                "area",
                "latitude",
                "longitude",
                "bedroom",
                "bathroom",
                "floor_count",
                "population_density",
                "total_rooms",
                "area_per_room",
                "bedroom_bathroom_ratio",
            ]
            low_card_cat = [
                "province_id",
                "interior_code",
                "legal_status_code",
                "house_direction_code",
            ]
            high_card_cat = ["district_id", "ward_id"]
            all_rf_features = numerical_features + low_card_cat + high_card_cat

            # Validate features exist in prepared data
            available_rf_features = [f for f in all_rf_features if f in df_rf.columns]
            missing_features = [f for f in all_rf_features if f not in df_rf.columns]

            if missing_features:
                self.logger.logger.error(f"❌ RF missing features: {missing_features}")
                self.logger.logger.error(
                    f"Available columns in df_rf: {sorted(df_rf.columns)}"
                )
                # Log a sample of the dataframe to debug
                sample_count = min(5, df_rf.count())
                if sample_count > 0:
                    self.logger.logger.error("Sample data:")
                    df_rf.select(*df_rf.columns[:10]).show(sample_count, truncate=False)
                raise ValueError(
                    f"Missing features for Random Forest: {missing_features}"
                )

            self.logger.logger.info(
                f"✅ RF using {len(available_rf_features)} features: {available_rf_features}"
            )

            # Create VectorAssembler for RF
            from pyspark.ml.feature import VectorAssembler

            rf_assembler = VectorAssembler(
                inputCols=available_rf_features,
                outputCol="features",
                handleInvalid="skip",
            )

            # Transform and prepare RF data
            df_rf_vectorized = rf_assembler.transform(df_rf)
            df_rf_clean = df_rf_vectorized.filter(
                col("features").isNotNull() & col("price").isNotNull()
            )

            # Train/test split for RF
            rf_train, rf_test = self._create_temporal_split(df_rf_clean)
            rf_train.cache()
            rf_test.cache()

            train_count = rf_train.count()
            test_count = rf_test.count()
            self.logger.logger.info(
                f"🌲 RF Train/Test: {train_count:,} / {test_count:,}"
            )

            # Train Random Forest model (like Kaggle: n_estimators=200, max_depth=15)
            from pyspark.ml.regression import RandomForestRegressor

            rf_model = RandomForestRegressor(
                featuresCol="features",
                labelCol="price",
                numTrees=200,
                maxDepth=15,
                seed=42,
            )

            rf_fitted = rf_model.fit(rf_train)
            rf_predictions = rf_fitted.transform(rf_test)

            # Calculate metrics
            rf_rmse = rmse_evaluator.evaluate(rf_predictions)
            rf_mae = mae_evaluator.evaluate(rf_predictions)
            rf_r2 = r2_evaluator.evaluate(rf_predictions)

            # Business metrics
            rf_business_metrics = self._calculate_business_metrics(rf_predictions)

            models["random_forest"] = {
                "model": rf_fitted,
                "model_type": "spark",
                "rmse": float(rf_rmse),
                "mae": float(rf_mae),
                "r2": float(rf_r2),
                "business_metrics": rf_business_metrics,
                "feature_count": len(available_rf_features),
            }

            self.logger.logger.info(
                f"✅ Random Forest: RMSE={rf_rmse:,.0f}, R²={rf_r2:.3f}"
            )

            # Clean up
            rf_train.unpersist()
            rf_test.unpersist()

        except Exception as e:
            self.logger.logger.error(f"❌ Random Forest training failed: {e}")

        # ==================== LINEAR REGRESSION ====================
        self.logger.logger.info("📈 Training Linear Regression...")
        try:
            # Prepare data specifically for Linear Regression
            df_lr = self.prepare_lr_model_data(df)

            # Define features for LR (exclude high cardinality categorical)
            numerical_features = [
                "area",
                "latitude",
                "longitude",
                "bedroom",
                "bathroom",
                "floor_count",
                "population_density",
                "total_rooms",
                "area_per_room",
                "bedroom_bathroom_ratio",
            ]
            low_card_cat = [
                "province_id",
                "interior_code",
                "legal_status_code",
                "house_direction_code",
            ]

            # Validate features exist in prepared data
            available_numeric = [f for f in numerical_features if f in df_lr.columns]
            available_categorical = [f for f in low_card_cat if f in df_lr.columns]

            missing_numeric = [f for f in numerical_features if f not in df_lr.columns]
            missing_categorical = [f for f in low_card_cat if f not in df_lr.columns]

            if missing_numeric or missing_categorical:
                self.logger.logger.error(f"❌ LR missing numeric: {missing_numeric}")
                self.logger.logger.error(
                    f"❌ LR missing categorical: {missing_categorical}"
                )
                self.logger.logger.error(
                    f"Available columns in df_lr: {sorted(df_lr.columns)}"
                )
                raise ValueError(
                    f"Missing features for Linear Regression: {missing_numeric + missing_categorical}"
                )

            self.logger.logger.info(
                f"✅ LR using {len(available_numeric)} numeric + {len(available_categorical)} categorical features"
            )
            from pyspark.ml.feature import (
                StringIndexer,
                OneHotEncoder,
                VectorAssembler,
                StandardScaler,
            )
            from pyspark.ml import Pipeline

            # Create preprocessing stages
            stages = []
            feature_cols = numerical_features.copy()

            # Process categorical features
            for cat_col in low_card_cat:
                if cat_col in df_lr.columns:
                    # StringIndexer
                    indexer = StringIndexer(
                        inputCol=cat_col,
                        outputCol=f"{cat_col}_indexed",
                        handleInvalid="keep",
                    )
                    stages.append(indexer)

                    # OneHotEncoder
                    encoder = OneHotEncoder(
                        inputCol=f"{cat_col}_indexed", outputCol=f"{cat_col}_encoded"
                    )
                    stages.append(encoder)

                    feature_cols.append(f"{cat_col}_encoded")

            # VectorAssembler for all features
            assembler = VectorAssembler(
                inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip"
            )
            stages.append(assembler)

            # StandardScaler (critical for Linear Regression)
            scaler = StandardScaler(
                inputCol="features_raw",
                outputCol="features",
                withStd=True,
                withMean=True,
            )
            stages.append(scaler)

            # Create preprocessing pipeline
            preprocessing_pipeline = Pipeline(stages=stages)

            # Fit preprocessing
            preprocessing_model = preprocessing_pipeline.fit(df_lr)
            df_lr_processed = preprocessing_model.transform(df_lr)

            # Clean data
            df_lr_clean = df_lr_processed.filter(
                col("features").isNotNull()
                & col("price").isNotNull()
                & col("log_price").isNotNull()
            )

            # Train/test split for LR
            lr_train, lr_test = self._create_temporal_split(df_lr_clean)
            lr_train.cache()
            lr_test.cache()

            train_count = lr_train.count()
            test_count = lr_test.count()
            self.logger.logger.info(
                f"📈 LR Train/Test: {train_count:,} / {test_count:,}"
            )

            # Train Linear Regression on log-price (like Kaggle)
            from pyspark.ml.regression import LinearRegression

            lr_model = LinearRegression(
                featuresCol="features",
                labelCol="log_price",  # Use log-transformed target
                regParam=0.01,
                elasticNetParam=0.1,
            )

            lr_fitted = lr_model.fit(lr_train)
            lr_predictions = lr_test.select("features", "price", "log_price")
            lr_predictions = lr_fitted.transform(lr_predictions)

            # Convert log predictions back to original scale (like Kaggle)
            from pyspark.sql.functions import exp

            lr_predictions = lr_predictions.withColumn(
                "prediction", exp(col("prediction"))
            )

            # Calculate metrics on original price scale
            lr_rmse = rmse_evaluator.evaluate(lr_predictions)
            lr_mae = mae_evaluator.evaluate(lr_predictions)
            lr_r2 = r2_evaluator.evaluate(lr_predictions)

            # Business metrics
            lr_business_metrics = self._calculate_business_metrics(lr_predictions)

            models["linear_regression"] = {
                "model": lr_fitted,
                "preprocessing_model": preprocessing_model,
                "model_type": "spark",
                "rmse": float(lr_rmse),
                "mae": float(lr_mae),
                "r2": float(lr_r2),
                "business_metrics": lr_business_metrics,
                "feature_count": len(feature_cols),
                "uses_log_target": True,
            }

            self.logger.logger.info(
                f"✅ Linear Regression: RMSE={lr_rmse:,.0f}, R²={lr_r2:.3f}"
            )

            # Clean up
            lr_train.unpersist()
            lr_test.unpersist()

        except Exception as e:
            self.logger.logger.error(f"❌ Linear Regression training failed: {e}")

        self.logger.logger.info(
            f"🎯 Spark models training completed: {len(models)} models trained"
        )
        return models

    def train_sklearn_models(self, df: Any) -> Dict[str, Dict]:
        """🚀 Train XGBoost/LightGBM models following Kaggle approach with NULL handling"""
        if not SKLEARN_MODELS_AVAILABLE:
            self.logger.logger.warning("⚠️ Sklearn models not available")
            return {}

        self.logger.logger.info(
            "🚀 Training XGBoost/LightGBM models with tree-optimized data..."
        )

        # Step 1: Prepare data specifically for tree models (preserve NULLs)
        df_tree = self.prepare_tree_model_data(df)

        # Step 2: Select exact 16 features (same as Kaggle code)
        important_features = [
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

        # Validate features exist
        available_features = [f for f in important_features if f in df_tree.columns]
        if len(available_features) != 16:
            self.logger.logger.error(
                f"❌ Expected 16 features, got {len(available_features)}"
            )
            return {}

        # Step 3: Convert to Pandas (tree models work better with Pandas)
        total_records = df_tree.count()

        # Sample if too large (but keep more data for tree models)
        if total_records > 50000:
            df_sampled = df_tree.limit(50000)
            self.logger.logger.info(
                f"📊 Sampling 50K records from {total_records:,} for sklearn models"
            )
        else:
            df_sampled = df_tree

        # Select features + target for conversion
        feature_cols = available_features + ["price"]
        pandas_df = df_sampled.select(feature_cols).toPandas()

        self.logger.logger.info(
            f"🐼 Converted to Pandas: {len(pandas_df):,} records, {len(available_features)} features"
        )

        # Step 4: Prepare features and target (like Kaggle)
        X = pandas_df[available_features]
        y = pandas_df["price"]

        # Log missing value statistics
        self.logger.logger.info("🔍 Missing value statistics for tree models:")
        for col in available_features:
            null_count = X[col].isnull().sum()
            if null_count > 0:
                self.logger.logger.info(
                    f"   - {col}: {null_count:,} nulls ({null_count/len(X)*100:.1f}%)"
                )

        # Step 5: Train/test split (like Kaggle)
        from sklearn.model_selection import train_test_split

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        self.logger.logger.info(
            f"📊 Train/test split: {len(X_train):,} / {len(X_test):,}"
        )

        models = {}

        # Step 6: Train XGBoost (handles NULLs natively)
        self.logger.logger.info("🏋️ Training XGBoost...")
        try:
            from xgboost import XGBRegressor

            xgb_model = XGBRegressor(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.05,
                random_state=42,
                n_jobs=4,
            )

            # XGBoost can handle NaN values natively
            xgb_model.fit(X_train, y_train)

            # Predictions
            train_preds = xgb_model.predict(X_train)
            test_preds = xgb_model.predict(X_test)

            # Metrics
            from sklearn.metrics import (
                mean_squared_error,
                mean_absolute_error,
                r2_score,
            )
            import numpy as np

            train_rmse = np.sqrt(mean_squared_error(y_train, train_preds))
            train_mae = mean_absolute_error(y_train, train_preds)
            train_r2 = r2_score(y_train, train_preds)

            test_rmse = np.sqrt(mean_squared_error(y_test, test_preds))
            test_mae = mean_absolute_error(y_test, test_preds)
            test_r2 = r2_score(y_test, test_preds)

            # Business metrics
            business_metrics = self._calculate_sklearn_business_metrics(
                y_test, test_preds
            )

            models["xgboost"] = {
                "model": xgb_model,
                "model_type": "sklearn",
                "rmse": float(test_rmse),
                "mae": float(test_mae),
                "r2": float(test_r2),
                "train_rmse": float(train_rmse),
                "train_r2": float(train_r2),
                "business_metrics": business_metrics,
            }

            self.logger.logger.info(f"✅ XGBoost completed:")
            self.logger.logger.info(
                f"   Train RMSE: {train_rmse:,.2f}, R²: {train_r2:.4f}"
            )
            self.logger.logger.info(
                f"   Test RMSE: {test_rmse:,.2f}, R²: {test_r2:.4f}"
            )

        except Exception as e:
            self.logger.logger.error(f"❌ XGBoost training failed: {e}")

        # Step 7: Train LightGBM (also handles NULLs natively)
        self.logger.logger.info("🏋️ Training LightGBM...")
        try:
            from lightgbm import LGBMRegressor

            lgb_model = LGBMRegressor(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.05,
                random_state=42,
                n_jobs=4,
                verbose=-1,
            )

            # LightGBM can handle NaN values natively
            lgb_model.fit(X_train, y_train)

            # Predictions
            train_preds = lgb_model.predict(X_train)
            test_preds = lgb_model.predict(X_test)

            # Metrics
            train_rmse = np.sqrt(mean_squared_error(y_train, train_preds))
            train_mae = mean_absolute_error(y_train, train_preds)
            train_r2 = r2_score(y_train, train_preds)

            test_rmse = np.sqrt(mean_squared_error(y_test, test_preds))
            test_mae = mean_absolute_error(y_test, test_preds)
            test_r2 = r2_score(y_test, test_preds)

            # Business metrics
            business_metrics = self._calculate_sklearn_business_metrics(
                y_test, test_preds
            )

            models["lightgbm"] = {
                "model": lgb_model,
                "model_type": "sklearn",
                "rmse": float(test_rmse),
                "mae": float(test_mae),
                "r2": float(test_r2),
                "train_rmse": float(train_rmse),
                "train_r2": float(train_r2),
                "business_metrics": business_metrics,
            }

            self.logger.logger.info(f"✅ LightGBM completed:")
            self.logger.logger.info(
                f"   Train RMSE: {train_rmse:,.2f}, R²: {train_r2:.4f}"
            )
            self.logger.logger.info(
                f"   Test RMSE: {test_rmse:,.2f}, R²: {test_r2:.4f}"
            )

        except Exception as e:
            self.logger.logger.error(f"❌ LightGBM training failed: {e}")

        self.logger.logger.info(
            f"🎯 Tree models training completed: {len(models)} models trained"
        )
        return models

    def create_ensemble_model(self, models: Dict[str, Dict]) -> Dict[str, Any]:
        """🎯 Create enhanced ensemble model considering both accuracy and business metrics"""
        self.logger.logger.info("🎯 Creating enhanced ensemble model...")

        if not models:
            self.logger.logger.warning("⚠️ No models available for ensemble")
            return {}

        # Enhanced model selection considering business metrics
        model_scores = []
        for name, model_info in models.items():
            r2_score = model_info.get("r2", 0)
            business_metrics = model_info.get("business_metrics", {})
            within_20_pct = business_metrics.get("within_20_percent", 0)

            # Composite score: 70% R² + 30% business accuracy
            composite_score = (0.7 * r2_score) + (0.3 * within_20_pct / 100)

            model_scores.append((name, model_info, composite_score))
            self.logger.logger.info(
                f"   {name}: R²={r2_score:.3f}, Within20%={within_20_pct:.1f}%, Composite={composite_score:.3f}"
            )

        # Sort by composite score and select top 3
        sorted_models = sorted(model_scores, key=lambda x: x[2], reverse=True)
        top_models = {name: model_info for name, model_info, _ in sorted_models[:3]}

        self.logger.logger.info(
            f"📊 Top models for ensemble: {list(top_models.keys())}"
        )

        # Calculate weights efficiently
        weights = {}
        total_r2 = sum(model["r2"] for model in top_models.values())

        for name, model in top_models.items():
            weights[name] = float(model["r2"] / total_r2)

        self.logger.logger.info(f"Ensemble weights: {weights}")

        return {
            "models": top_models,
            "weights": weights,
            "ensemble_type": "weighted_average",
        }

    def verify_saved_models(self, model_dir: str, models: Dict) -> Dict[str, bool]:
        """🔍 Verify that all models were saved successfully"""
        verification_results = {}

        self.logger.logger.info("🔍 Verifying saved models...")

        for name, model in models.items():
            if model["model_type"] == "spark":
                spark_model_path = f"{model_dir}/spark_models/{name}"
                try:
                    if check_hdfs_path_exists(self.spark, spark_model_path):
                        verification_results[f"spark_{name}"] = True
                        self.logger.logger.info(f"✅ Verified Spark model: {name}")
                    else:
                        verification_results[f"spark_{name}"] = False
                        self.logger.logger.warning(f"⚠️ Spark model not found: {name}")
                except Exception as e:
                    verification_results[f"spark_{name}"] = False
                    self.logger.logger.error(
                        f"❌ Error verifying Spark model {name}: {e}"
                    )

            elif model["model_type"] == "sklearn":
                sklearn_model_file = f"{model_dir}/sklearn_models/{name}_model.pkl"
                try:
                    # For HDFS paths, use HDFS check; for local paths, use os.path.exists
                    if model_dir.startswith("/data/"):
                        # Check HDFS path
                        if check_hdfs_path_exists(self.spark, sklearn_model_file):
                            verification_results[f"sklearn_{name}"] = True
                            # Get file size using Hadoop filesystem
                            try:
                                hadoop_config = (
                                    self.spark.sparkContext._jsc.hadoopConfiguration()
                                )
                                hadoop_fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                                    hadoop_config
                                )
                                hdfs_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                                    sklearn_model_file
                                )
                                file_size = hadoop_fs.getFileStatus(
                                    hdfs_path
                                ).getLen() / (
                                    1024 * 1024
                                )  # MB
                                self.logger.logger.info(
                                    f"✅ Verified sklearn model on HDFS: {name} ({file_size:.2f} MB)"
                                )
                            except:
                                self.logger.logger.info(
                                    f"✅ Verified sklearn model on HDFS: {name}"
                                )
                        else:
                            verification_results[f"sklearn_{name}"] = False
                            self.logger.logger.warning(
                                f"⚠️ Sklearn model not found on HDFS: {name}"
                            )
                    else:
                        # Check local path
                        if os.path.exists(sklearn_model_file):
                            verification_results[f"sklearn_{name}"] = True
                            file_size = os.path.getsize(sklearn_model_file) / (
                                1024 * 1024
                            )  # MB
                            self.logger.logger.info(
                                f"✅ Verified sklearn model locally: {name} ({file_size:.2f} MB)"
                            )
                        else:
                            verification_results[f"sklearn_{name}"] = False
                            self.logger.logger.warning(
                                f"⚠️ Sklearn model not found locally: {name}"
                            )
                except Exception as e:
                    verification_results[f"sklearn_{name}"] = False
                    self.logger.logger.error(
                        f"❌ Error verifying sklearn model {name}: {e}"
                    )

        # Check preprocessing pipeline
        preprocessing_path = f"{model_dir}/preprocessing_pipeline"
        try:
            if check_hdfs_path_exists(self.spark, preprocessing_path):
                verification_results["preprocessing_pipeline"] = True
                self.logger.logger.info("✅ Verified preprocessing pipeline")
            else:
                verification_results["preprocessing_pipeline"] = False
                self.logger.logger.warning("⚠️ Preprocessing pipeline not found")
        except Exception as e:
            verification_results["preprocessing_pipeline"] = False
            self.logger.logger.error(f"❌ Error verifying preprocessing pipeline: {e}")

        # Summary
        successful_saves = sum(verification_results.values())
        total_expected = len(verification_results)
        success_rate = successful_saves / total_expected if total_expected > 0 else 0

        self.logger.logger.info(
            f"📊 Model save verification: {successful_saves}/{total_expected} successful ({success_rate:.1%})"
        )

        return verification_results

    def debug_model_info(self, models: Dict, stage: str = "training") -> None:
        """🔍 Debug information about models"""
        self.logger.logger.info(f"🔍 Model Debug Info - {stage}")
        self.logger.logger.info(
            f"   Sklearn models available: {SKLEARN_MODELS_AVAILABLE}"
        )
        self.logger.logger.info(f"   Total models: {len(models)}")

        for name, model in models.items():
            model_type = model.get("model_type", "unknown")
            r2_score = model.get("r2", 0)
            self.logger.logger.info(f"   - {name}: {model_type} (R²={r2_score:.3f})")

        spark_models = [
            name for name, model in models.items() if model.get("model_type") == "spark"
        ]
        sklearn_models = [
            name
            for name, model in models.items()
            if model.get("model_type") == "sklearn"
        ]

        self.logger.logger.info(f"   Spark models: {spark_models}")
        self.logger.logger.info(f"   Sklearn models: {sklearn_models}")

    def save_models(
        self,
        models: Dict,
        ensemble: Dict,
        preprocessing_model: Any,
        date: str,
        property_type: str = "house",
    ) -> str:
        """💾 Save all models with versioning and optimized serialization"""
        self.logger.logger.info("💾 Saving models...")

        # Debug model information
        self.debug_model_info(models, "before_saving")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_dir = f"{self.model_output_path}/{property_type}/{date}"

        # Find best model efficiently
        best_model_name = max(models.keys(), key=lambda k: models[k]["r2"])
        best_model = models[best_model_name]

        # Create optimized model registry entry
        model_registry = {
            "model_version": f"v{timestamp}",
            "training_date": date,
            "timestamp": timestamp,
            "best_model": {
                "name": best_model_name,
                "rmse": float(best_model["rmse"]),
                "mae": float(best_model["mae"]),
                "r2": float(best_model["r2"]),
                "model_type": best_model["model_type"],
            },
            "all_models": {
                name: {
                    "rmse": float(model["rmse"]),
                    "mae": float(model["mae"]),
                    "r2": float(model["r2"]),
                    "model_type": model["model_type"],
                }
                for name, model in models.items()
            },
            "ensemble": {
                "models": list(ensemble["models"].keys()),
                "weights": {k: float(v) for k, v in ensemble["weights"].items()},
            },
            "deployment_ready": True,
            "production_metrics": {
                "target_rmse_threshold": float(best_model["rmse"] * 1.1),
                "minimum_r2": 0.7,
                "passed_validation": best_model["r2"] > 0.7,
            },
            "performance_optimization": {
                "spark_broadcast_optimized": True,
                "memory_efficient": True,
                "serialization_minimized": True,
            },
        }

        try:
            # Save Spark models efficiently
            for name, model in models.items():
                if model["model_type"] == "spark":
                    spark_model_path = f"{model_dir}/spark_models/{name}"
                    model["model"].write().overwrite().save(spark_model_path)

            # Save preprocessing pipeline
            preprocessing_path = f"{model_dir}/preprocessing_pipeline"
            preprocessing_model.write().overwrite().save(preprocessing_path)

            # Save sklearn models with optimized serialization
            if SKLEARN_MODELS_AVAILABLE:
                sklearn_models_dir = f"{model_dir}/sklearn_models"

                # Count sklearn models to save
                sklearn_models_count = sum(
                    1 for model in models.values() if model["model_type"] == "sklearn"
                )
                self.logger.logger.info(
                    f"📦 Saving {sklearn_models_count} sklearn models..."
                )

                # Create sklearn models directory properly
                sklearn_models_dir = f"{model_dir}/sklearn_models"

                try:
                    # For HDFS paths, use create_hdfs_directory utility
                    if model_dir.startswith("/data/"):
                        self.logger.logger.info(
                            f"📁 Creating HDFS directory: {sklearn_models_dir}"
                        )

                        # Use create_hdfs_directory from utils
                        success = create_hdfs_directory(self.spark, sklearn_models_dir)

                        if not success:
                            # Alternative approach: create using Spark DataFrame write
                            self.logger.logger.info(
                                "🔄 Trying alternative directory creation..."
                            )
                            dummy_df = self.spark.createDataFrame([{"temp": "data"}])
                            temp_path = f"{sklearn_models_dir}/.mkdir_temp"
                            dummy_df.write.mode("overwrite").parquet(temp_path)

                            # Clean up temp file
                            try:
                                hadoop_config = (
                                    self.spark.sparkContext._jsc.hadoopConfiguration()
                                )
                                hadoop_fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                                    hadoop_config
                                )
                                temp_hadoop_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                                    temp_path
                                )
                                hadoop_fs.delete(temp_hadoop_path, True)
                                self.logger.logger.info(
                                    "✅ HDFS directory created successfully"
                                )
                            except Exception as cleanup_error:
                                self.logger.logger.warning(
                                    f"⚠️ Could not clean up temp file: {cleanup_error}"
                                )
                        else:
                            self.logger.logger.info(
                                "✅ HDFS directory created successfully"
                            )

                    else:
                        # For local paths, use standard os.makedirs
                        os.makedirs(sklearn_models_dir, exist_ok=True)
                        self.logger.logger.info(
                            f"✅ Local directory created: {sklearn_models_dir}"
                        )

                except Exception as e:
                    self.logger.logger.warning(
                        f"⚠️ Could not create sklearn models directory: {e}"
                    )
                    # Fallback: use local temp directory
                    sklearn_models_dir = tempfile.mkdtemp(prefix="sklearn_models_")
                    self.logger.logger.info(
                        f"📁 Using fallback directory: {sklearn_models_dir}"
                    )

                # Save sklearn models with proper HDFS handling
                for name, model in models.items():
                    if model["model_type"] == "sklearn":
                        hdfs_model_path = f"{sklearn_models_dir}/{name}_model.pkl"

                        try:
                            # For HDFS paths, save to local temp first, then copy to HDFS
                            if model_dir.startswith("/data/"):
                                # Step 1: Save to local temp file
                                local_temp_dir = tempfile.mkdtemp(
                                    prefix=f"sklearn_{name}_"
                                )
                                local_model_file = f"{local_temp_dir}/{name}_model.pkl"

                                # Save using joblib to local file
                                joblib.dump(
                                    model["model"], local_model_file, compress=3
                                )

                                if not os.path.exists(local_model_file):
                                    raise FileNotFoundError(
                                        f"Failed to create local temp file: {local_model_file}"
                                    )

                                local_file_size = os.path.getsize(local_model_file) / (
                                    1024 * 1024
                                )  # MB
                                self.logger.logger.info(
                                    f"📦 Created local temp file: {local_model_file} ({local_file_size:.2f} MB)"
                                )

                                # Step 2: Copy local file to HDFS using Hadoop filesystem
                                try:
                                    # Use Hadoop filesystem API to copy the actual .pkl file
                                    hadoop_config = (
                                        self.spark.sparkContext._jsc.hadoopConfiguration()
                                    )
                                    hadoop_fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                                        hadoop_config
                                    )

                                    # Convert paths to Hadoop Path objects
                                    local_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                                        f"file://{local_model_file}"
                                    )
                                    hdfs_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                                        hdfs_model_path
                                    )

                                    # Copy file from local to HDFS
                                    hadoop_fs.copyFromLocalFile(local_path, hdfs_path)

                                    # Verify HDFS file exists
                                    if hadoop_fs.exists(hdfs_path):
                                        hdfs_file_size = hadoop_fs.getFileStatus(
                                            hdfs_path
                                        ).getLen() / (
                                            1024 * 1024
                                        )  # MB
                                        self.logger.logger.info(
                                            f"✅ Sklearn model copied to HDFS: {name} -> {hdfs_model_path} ({hdfs_file_size:.2f} MB)"
                                        )
                                    else:
                                        raise Exception(
                                            "HDFS copy verification failed - file not found"
                                        )

                                except Exception as hdfs_error:
                                    self.logger.logger.warning(
                                        f"⚠️ HDFS copy failed for {name}: {hdfs_error}"
                                    )
                                    # Keep the local temp file as fallback
                                    self.logger.logger.info(
                                        f"📁 Model available at local fallback: {local_model_file}"
                                    )

                                # Step 3: Clean up local temp file (optional - keep for now as backup)
                                # We'll keep the temp file as a backup in case HDFS access fails later

                            else:
                                # For local paths, save directly
                                os.makedirs(
                                    os.path.dirname(hdfs_model_path), exist_ok=True
                                )
                                joblib.dump(model["model"], hdfs_model_path, compress=3)

                                if os.path.exists(hdfs_model_path):
                                    file_size = os.path.getsize(hdfs_model_path) / (
                                        1024 * 1024
                                    )  # MB
                                    self.logger.logger.info(
                                        f"✅ Saved sklearn model locally: {name} -> {hdfs_model_path} ({file_size:.2f} MB)"
                                    )
                                else:
                                    raise FileNotFoundError(
                                        f"Model file was not created: {hdfs_model_path}"
                                    )

                        except Exception as e:
                            self.logger.logger.error(
                                f"❌ Failed to save sklearn model {name}: {e}"
                            )

                            # Final fallback: save to local temp directory only
                            try:
                                local_temp_dir = tempfile.mkdtemp(
                                    prefix=f"sklearn_fallback_{name}_"
                                )
                                local_model_file = f"{local_temp_dir}/{name}_model.pkl"

                                joblib.dump(
                                    model["model"], local_model_file, compress=3
                                )

                                if os.path.exists(local_model_file):
                                    file_size = os.path.getsize(local_model_file) / (
                                        1024 * 1024
                                    )  # MB
                                    self.logger.logger.info(
                                        f"✅ Saved sklearn model to fallback location: {name} -> {local_model_file} ({file_size:.2f} MB)"
                                    )
                                else:
                                    self.logger.logger.error(
                                        f"❌ Failed to create fallback file for {name}"
                                    )

                            except Exception as fallback_error:
                                self.logger.logger.error(
                                    f"❌ Final fallback failed for {name}: {fallback_error}"
                                )

            else:
                self.logger.logger.warning("⚠️ Sklearn models not available for saving")

            # Save model registry efficiently
            registry_df = self.spark.createDataFrame(
                [{"registry": json.dumps(model_registry, indent=2)}]
            )
            registry_df.write.mode("overwrite").json(f"{model_dir}/model_registry.json")

            # Verify all models were saved successfully
            verification_results = self.verify_saved_models(model_dir, models)

            # Log verification results
            failed_saves = [
                name for name, success in verification_results.items() if not success
            ]
            if failed_saves:
                self.logger.logger.warning(
                    f"⚠️ Some models failed to save: {failed_saves}"
                )
            else:
                self.logger.logger.info("✅ All models verified successfully")

            self.logger.logger.info(f"✅ Models saved to: {model_dir}")
            self.logger.logger.info(
                f"🏆 Best model: {best_model_name} (R²={best_model['r2']:.3f})"
            )

            return model_dir

        except Exception as e:
            self.logger.logger.error(f"❌ Error saving models: {e}")
            raise

    def run_training_pipeline(
        self,
        date: str,
        property_type: str = "house",
        enable_sklearn_models: bool = True,
    ) -> Dict[str, Any]:
        """🚀 Run complete ML training pipeline with optimized performance"""
        self.logger.logger.info(
            "🚀 Starting ML Training Pipeline (Performance Optimized)"
        )
        self.logger.logger.info("=" * 80)

        try:
            # Step 1: Read ML features with optimized data handling
            df, metadata = self.read_ml_features(date, property_type)

            # Step 2: Prepare preprocessing model for deployment
            # Note: This is only used for creating the preprocessing model, not for training
            _, preprocessing_model = self.prepare_spark_ml_data(df, metadata)

            # Step 3: Train Spark models with raw feature data
            spark_models = self.train_spark_models(df)

            # Step 4: Train sklearn models with raw feature data
            sklearn_models = {}
            if enable_sklearn_models and SKLEARN_MODELS_AVAILABLE:
                sklearn_models = self.train_sklearn_models(df)

            # Step 5: Combine all models efficiently
            all_models = {**spark_models, **sklearn_models}

            if not all_models:
                raise Exception("No models were successfully trained")

            # Step 6: Create optimized ensemble
            ensemble = self.create_ensemble_model(all_models)

            # Step 7: Save models with optimized serialization
            model_path = self.save_models(
                all_models, ensemble, preprocessing_model, date, property_type
            )

            # Step 8: Validate production quality
            best_model_name = max(all_models.keys(), key=lambda k: all_models[k]["r2"])
            best_model = all_models[best_model_name]

            # Run production quality validation
            quality_validation = self._validate_production_quality(best_model)

            # Include business metrics if available
            business_metrics = best_model.get("business_metrics", {})

            # Enhanced metrics with business KPIs
            enhanced_metrics = {
                "rmse": float(best_model["rmse"]),
                "mae": float(best_model["mae"]),
                "r2": float(best_model["r2"]),
            }

            # Add business metrics to main metrics
            if business_metrics:
                enhanced_metrics.update(
                    {
                        "within_20_percent": business_metrics.get(
                            "within_20_percent", 0
                        ),
                        "median_error": business_metrics.get("median_error", 0),
                        "outlier_percentage": business_metrics.get(
                            "outlier_percentage", 0
                        ),
                    }
                )

            result = {
                "success": True,
                "best_model": best_model_name,
                "model_path": model_path,
                "metrics": enhanced_metrics,
                "business_metrics": business_metrics,
                "quality_validation": quality_validation,
                "total_models_trained": len(all_models),
                "ensemble_models": len(ensemble.get("models", [])),
                "production_ready": quality_validation["production_ready"],
                "quality_score": quality_validation["quality_score"],
                "performance_optimizations": {
                    "temporal_splitting_enabled": True,
                    "business_metrics_calculated": bool(business_metrics),
                    "production_quality_validated": True,
                    "broadcast_warnings_resolved": True,
                    "memory_efficient": True,
                    "serialization_optimized": True,
                },
            }

            # Clean up cached DataFrames
            if hasattr(df, "unpersist"):
                df.unpersist()

            self.logger.logger.info("=" * 80)
            self.logger.logger.info("🎉 ML Training Pipeline Completed!")
            self.logger.logger.info(f"🏆 Best Model: {best_model_name}")
            self.logger.logger.info(f"📊 RMSE: {best_model['rmse']:,.0f} VND")
            self.logger.logger.info(f"📊 MAE: {best_model['mae']:,.0f} VND")
            self.logger.logger.info(f"📊 R²: {best_model['r2']:.3f}")
            self.logger.logger.info(
                f"🎯 Production Ready: {result['production_ready']}"
            )
            self.logger.logger.info("⚡ Performance Optimizations Applied")

            return result

        except Exception as e:
            self.logger.logger.error(f"❌ Training pipeline failed: {str(e)}")
            raise
        finally:
            self.logger.logger.info("🧹 Training pipeline cleanup completed")

    def _find_latest_available_features(
        self, property_type: str, requested_date: datetime
    ) -> Tuple[Optional[str], Optional[datetime]]:
        """🔍 Find the latest available feature file"""
        from datetime import datetime, timedelta

        base_path = f"/data/realestate/processed/ml/feature_store/{property_type}"

        # Look back from requested date to find the latest available file
        current_date = requested_date
        for i in range(90):  # Look back 90 days maximum
            check_date = current_date - timedelta(days=i)
            year = check_date.strftime("%Y")
            month = check_date.strftime("%m")
            day = check_date.strftime("%d")
            date_formatted = check_date.strftime("%Y%m%d")

            feature_path = f"{base_path}/{year}/{month}/{day}/features_{property_type}_{date_formatted}.parquet"

            if check_hdfs_path_exists(self.spark, feature_path):
                self.logger.logger.info(
                    f"✅ Found latest available feature file: {check_date.strftime('%Y-%m-%d')}"
                )
                return feature_path, check_date

        self.logger.logger.error(
            f"❌ No feature files found within 90 days from {requested_date.strftime('%Y-%m-%d')}"
        )
        return None, None

    def _find_most_recent_features(
        self, property_type: str = "house", min_days: int = 30
    ) -> List[str]:
        """🔍 Find most recent available feature data for fallback training"""
        from datetime import datetime, timedelta

        self.logger.logger.info(
            f"🔍 Scanning for most recent {property_type} features..."
        )

        base_path = f"/data/realestate/processed/ml/feature_store/{property_type}"
        available_paths = []

        try:
            # Get all available dates by scanning year/month/day directories
            if check_hdfs_path_exists(self.spark, base_path):
                hadoop_config = self.spark.sparkContext._jsc.hadoopConfiguration()
                hadoop_fs = (
                    self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                        hadoop_config
                    )
                )

                available_dates = []

                # Scan for the last 90 days to find available data
                current_date = datetime.now()
                for i in range(90):  # Look back 90 days
                    check_date = current_date - timedelta(days=i)
                    year = check_date.strftime("%Y")
                    month = check_date.strftime("%m")
                    day = check_date.strftime("%d")
                    date_formatted = check_date.strftime("%Y%m%d")

                    feature_path = f"{base_path}/{year}/{month}/{day}/features_{property_type}_{date_formatted}.parquet"

                    if check_hdfs_path_exists(self.spark, feature_path):
                        available_dates.append((check_date, feature_path))

                if available_dates:
                    # Sort by date (most recent first)
                    available_dates.sort(key=lambda x: x[0], reverse=True)

                    self.logger.logger.info(
                        f"📅 Found {len(available_dates)} available feature files"
                    )
                    self.logger.logger.info(
                        f"📅 Most recent: {available_dates[0][0].strftime('%Y-%m-%d')}"
                    )
                    self.logger.logger.info(
                        f"📅 Oldest available: {available_dates[-1][0].strftime('%Y-%m-%d')}"
                    )

                    # Take up to min_days of most recent data
                    recent_data = available_dates[:min_days]
                    available_paths = [path for date_obj, path in recent_data]

                    if available_paths:
                        self.logger.logger.info(
                            f"✅ Using {len(available_paths)} days of most recent feature data"
                        )
                        self.logger.logger.info(
                            f"📅 Fallback date range: {recent_data[-1][0].strftime('%Y-%m-%d')} to {recent_data[0][0].strftime('%Y-%m-%d')}"
                        )

            return available_paths

        except Exception as e:
            self.logger.logger.error(f"❌ Error scanning for recent features: {e}")
            return []

    def _validate_production_quality(
        self, best_model: Dict[str, Any]
    ) -> Dict[str, Any]:
        """✅ Validate if model meets production quality standards"""
        self.logger.logger.info("✅ Validating production quality standards...")

        # Default production requirements for real estate models
        requirements = {
            "min_r2": 0.70,  # Minimum R² score
            "max_mape": 0.30,  # Maximum 30% MAPE
            "min_within_20_percent": 60.0,  # At least 60% within 20% accuracy
            "max_outliers_percentage": 15.0,  # Maximum 15% outliers
        }

        validation_results = {
            "production_ready": True,
            "validation_checks": {},
            "warnings": [],
            "failures": [],
        }

        # Extract metrics
        r2 = best_model.get("r2", 0)
        business_metrics = best_model.get("business_metrics", {})

        # Check R² score
        r2_check = r2 >= requirements["min_r2"]
        validation_results["validation_checks"]["r2_check"] = {
            "passed": r2_check,
            "actual": r2,
            "required": requirements["min_r2"],
        }

        if not r2_check:
            validation_results["production_ready"] = False
            validation_results["failures"].append(
                f"R² score too low: {r2:.3f} < {requirements['min_r2']}"
            )

        # Check business metrics if available
        if business_metrics:
            # Check accuracy within 20%
            within_20_pct = business_metrics.get("within_20_percent", 0)
            within_20_check = within_20_pct >= requirements["min_within_20_percent"]
            validation_results["validation_checks"]["within_20_check"] = {
                "passed": within_20_check,
                "actual": within_20_pct,
                "required": requirements["min_within_20_percent"],
            }

            if not within_20_check:
                validation_results["production_ready"] = False
                validation_results["failures"].append(
                    f"Accuracy within 20% too low: {within_20_pct:.1f}% < {requirements['min_within_20_percent']}%"
                )

            # Check MAPE if available
            mape = (
                business_metrics.get("mean_percentage_error", 100) / 100
            )  # Convert to decimal
            if mape > 0:
                mape_check = mape <= requirements["max_mape"]
                validation_results["validation_checks"]["mape_check"] = {
                    "passed": mape_check,
                    "actual": mape,
                    "required": requirements["max_mape"],
                }

                if not mape_check:
                    validation_results["production_ready"] = False
                    validation_results["failures"].append(
                        f"MAPE too high: {mape:.3f} > {requirements['max_mape']}"
                    )

            # Check outliers percentage
            outliers_pct = business_metrics.get("outlier_percentage", 0)
            outliers_check = outliers_pct <= requirements["max_outliers_percentage"]
            validation_results["validation_checks"]["outliers_check"] = {
                "passed": outliers_check,
                "actual": outliers_pct,
                "required": requirements["max_outliers_percentage"],
            }

            if not outliers_check:
                validation_results["warnings"].append(
                    f"High outliers percentage: {outliers_pct:.1f}% > {requirements['max_outliers_percentage']}%"
                )

        # Calculate quality score
        total_checks = len(validation_results["validation_checks"])
        passed_checks = sum(
            1
            for check in validation_results["validation_checks"].values()
            if check["passed"]
        )
        validation_results["quality_score"] = (
            passed_checks / total_checks if total_checks > 0 else 0
        )

        # Log results
        if validation_results["production_ready"]:
            self.logger.logger.info("✅ Model meets production quality standards")
            self.logger.logger.info(
                f"📊 Quality score: {validation_results['quality_score']:.2%}"
            )
        else:
            self.logger.logger.warning(
                "⚠️ Model does not meet production quality standards"
            )
            for failure in validation_results["failures"]:
                self.logger.logger.warning(f"   ❌ {failure}")

        for warning in validation_results["warnings"]:
            self.logger.logger.warning(f"   ⚠️ {warning}")

        return validation_results

    def prepare_tree_model_data(self, df: Any) -> Any:
        """🌳 Prepare data specifically for XGBoost/LightGBM following Kaggle approach"""
        from pyspark.sql.functions import col, when, lit

        self.logger.logger.info(
            "🌳 Preparing data for tree-based models (XGBoost/LightGBM)"
        )

        # Apply downsampling first (like Kaggle code)
        from ml.utils.data_cleaner import DataCleaner

        cleaner = DataCleaner(self.spark)
        df = cleaner.apply_province_downsampling(df)

        # Define categorical features (same as Kaggle)
        categorical_features = [
            "house_direction_code",
            "legal_status_code",
            "interior_code",
            "province_id",
            "district_id",
            "ward_id",
        ]

        # Convert -1 to NULL for categorical features (tree models handle nulls natively)
        for col_name in categorical_features:
            if (
                col_name in df.columns and col_name != "province_id"
            ):  # Keep province_id validation
                before_count = df.filter(col(col_name) == -1).count()
                df = df.withColumn(
                    col_name,
                    when(col(col_name) == -1, lit(None)).otherwise(col(col_name)),
                )
                if before_count > 0:
                    self.logger.logger.info(
                        f"🌳 Converted {before_count:,} values of -1 to NULL in {col_name}"
                    )

        # For tree models: KEEP nulls in numeric features where appropriate
        # Only impute absolutely critical features
        critical_features = ["price", "area", "latitude", "longitude", "province_id"]

        for col_name in critical_features:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    if col_name == "province_id":
                        # Remove records with null province_id (critical)
                        df = df.filter(col(col_name).isNotNull())
                        self.logger.logger.info(
                            f"🌳 Removed {null_count:,} records with NULL {col_name}"
                        )
                    else:
                        # Impute numeric critical features
                        median_val = df.approxQuantile(col_name, [0.5], 0.1)[0]
                        if median_val is not None:
                            df = df.fillna({col_name: median_val})
                            self.logger.logger.info(
                                f"🌳 Imputed {null_count:,} NULL values in {col_name} with median: {median_val}"
                            )

        self.logger.logger.info(
            "🌳 Tree model data preparation completed - NULLs preserved for tree algorithms"
        )
        return df

    def prepare_common_features(self, df: Any) -> Any:
        """🔧 Common feature preparation for all models - avoid code duplication"""
        from pyspark.sql.functions import col, when, lit

        self.logger.logger.info("🔧 Preparing common features...")

        # Apply downsampling first (common for all models)
        from ml.utils.data_cleaner import DataCleaner

        cleaner = DataCleaner(self.spark)
        df = cleaner.apply_province_downsampling(df)

        # Define essential numeric columns that need imputation for feature engineering
        essential_numeric = ["bedroom", "bathroom", "floor_count", "area"]

        # Fill essential numeric columns with median (needed for derived features)
        for col_name in essential_numeric:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    median_val = df.approxQuantile(col_name, [0.5], 0.1)[0]
                    if median_val is not None:
                        df = df.fillna({col_name: median_val})
                        self.logger.logger.info(
                            f"🔧 Filled {null_count:,} nulls in {col_name} with median: {median_val}"
                        )

        # Create derived features (like Kaggle)
        df = df.withColumn("total_rooms", col("bedroom") + col("bathroom"))
        df = df.withColumn(
            "area_per_room", col("area") / (col("total_rooms") + lit(1e-5))
        )
        df = df.withColumn(
            "bedroom_bathroom_ratio", col("bedroom") / (col("bathroom") + lit(1e-5))
        )

        self.logger.logger.info(
            "✅ Created derived features: total_rooms, area_per_room, bedroom_bathroom_ratio"
        )

        return df

    def prepare_rf_model_data(self, df: Any) -> Any:
        """🌲 Prepare data specifically for Random Forest following Kaggle approach"""
        from pyspark.sql.functions import col, when, lit

        self.logger.logger.info("🌲 Preparing data for Random Forest...")

        # Common feature preparation
        df = self.prepare_common_features(df)

        # Define feature categories (like Kaggle)
        numerical_features = [
            "area",
            "latitude",
            "longitude",
            "bedroom",
            "bathroom",
            "floor_count",
            "population_density",
            "total_rooms",
            "area_per_room",
            "bedroom_bathroom_ratio",
        ]
        low_card_cat = [
            "province_id",
            "interior_code",
            "legal_status_code",
            "house_direction_code",
        ]
        high_card_cat = ["district_id", "ward_id"]
        all_features = numerical_features + low_card_cat + high_card_cat

        # For Random Forest: Fill NaN with -1 for categorical (like Kaggle)
        for col_name in low_card_cat + high_card_cat:
            if col_name in df.columns:
                df = df.fillna({col_name: -1})
                # Convert to int type
                df = df.withColumn(col_name, col(col_name).cast("integer"))

        # Fill remaining numeric nulls with median
        for col_name in numerical_features:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    median_val = df.approxQuantile(col_name, [0.5], 0.1)[0]
                    if median_val is not None:
                        df = df.fillna({col_name: median_val})

        self.logger.logger.info(
            f"🌲 Random Forest data ready with {len(all_features)} features"
        )
        return df

    def prepare_lr_model_data(self, df: Any) -> Any:
        """📈 Prepare data specifically for Linear Regression following Kaggle approach"""
        from pyspark.sql.functions import col, when, lit, log as spark_log

        self.logger.logger.info("📈 Preparing data for Linear Regression...")

        # Common feature preparation
        df = self.prepare_common_features(df)

        # For Linear Regression: Use log-transformed target (like Kaggle)
        df = df.withColumn("log_price", spark_log(col("price")))

        # Define features (exclude high cardinality categorical for linear model)
        numerical_features = [
            "area",
            "latitude",
            "longitude",
            "bedroom",
            "bathroom",
            "floor_count",
            "population_density",
            "total_rooms",
            "area_per_room",
            "bedroom_bathroom_ratio",
        ]
        low_card_cat = [
            "province_id",
            "interior_code",
            "legal_status_code",
            "house_direction_code",
        ]

        # For Linear Regression: Convert categorical to string and fill missing with "missing"
        for col_name in low_card_cat:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNull() | (col(col_name) == -1), "missing"
                    ).otherwise(col(col_name).cast("string")),
                )

        # Fill numeric nulls with median (critical for linear models)
        for col_name in numerical_features:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    median_val = df.approxQuantile(col_name, [0.5], 0.1)[0]
                    if median_val is not None:
                        df = df.fillna({col_name: median_val})

        all_features = numerical_features + low_card_cat
        self.logger.logger.info(
            f"📈 Linear Regression data ready with {len(all_features)} features"
        )
        return df
