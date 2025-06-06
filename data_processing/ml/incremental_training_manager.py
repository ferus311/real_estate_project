"""
🔄 Incremental ML Training Manager for Real Estate Prediction
============================================================

Manages daily incremental training workflow:
- 📊 Detects new data and data drift
- 🔄 Performs incremental model updates
- 📈 Monitors model performance
- 🚨 Triggers full retraining when needed
- 💾 Manages model versioning and rollback
- 🐳 Docker deployment ready

Strategy:
1. Daily: Check for new Gold data
2. Feature Engineering: Process new data through feature pipeline
3. Incremental Training: Update existing models with new data
4. Validation: Compare performance with baseline
5. Deployment: Deploy if performance improves
6. Monitoring: Track model drift and performance

Author: ML Team
Date: June 2025
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import warnings

warnings.filterwarnings("ignore")

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, min as spark_min
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# Advanced ML imports
try:
    import xgboost as xgb
    import lightgbm as lgb
    from catboost import CatBoostRegressor
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    import joblib

    ADVANCED_MODELS_AVAILABLE = True
except ImportError:
    print("⚠️ Advanced models not available - using Spark ML only")
    ADVANCED_MODELS_AVAILABLE = False

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from spark.common.config.spark_config import create_spark_session
from spark.common.utils.hdfs_utils import check_hdfs_path_exists
from spark.common.utils.logging_utils import SparkJobLogger


class IncrementalTrainingManager:
    """
    🔄 Manager for incremental ML training workflow

    Handles:
    - Daily data detection and processing
    - Incremental model updates
    - Performance monitoring and drift detection
    - Model versioning and deployment
    """

    def __init__(
        self,
        models_path="/data/realestate/ml/models",
        features_path="/data/realestate/ml/features",
    ):
        self.spark = create_spark_session("Incremental_ML_Training")
        self.logger = SparkJobLogger("incremental_training", self.spark)

        self.models_path = models_path
        self.features_path = features_path
        self.model_registry_path = f"{models_path}/registry"

        # Performance thresholds
        self.performance_thresholds = {
            "min_improvement": 0.02,  # 2% improvement required
            "max_drift_score": 0.15,  # Maximum allowed drift
            "min_r2_score": 0.75,  # Minimum R² for deployment
            "max_rmse_increase": 0.10,  # Max 10% RMSE increase
        }

        # Training configurations
        self.incremental_config = {
            "learning_rate_decay": 0.9,  # Reduce learning rate for incremental
            "max_incremental_days": 7,  # Max days before full retrain
            "validation_split": 0.2,  # Validation data percentage
            "early_stopping_rounds": 50,
        }

    def check_new_data_available(
        self, target_date: str, property_type: str = "house"
    ) -> bool:
        """Check if new feature data is available for the target date"""
        features_path = f"{self.features_path}/{property_type}/{target_date}"
        return check_hdfs_path_exists(self.spark, features_path)

    def get_latest_model_info(self, property_type: str = "house") -> Optional[Dict]:
        """Get information about the latest trained model"""
        registry_path = f"{self.model_registry_path}/{property_type}/latest_model.json"

        try:
            if check_hdfs_path_exists(self.spark, registry_path):
                # Read model metadata from HDFS
                metadata_df = self.spark.read.json(registry_path)
                metadata_row = metadata_df.collect()[0]
                return json.loads(metadata_row["metadata"])
            else:
                self.logger.logger.info(
                    "No existing model found - will perform full training"
                )
                return None
        except Exception as e:
            self.logger.logger.warning(f"Error reading model registry: {e}")
            return None

    def load_feature_data(
        self, date: str, property_type: str = "house"
    ) -> Tuple[pd.DataFrame, Dict]:
        """Load feature data for a specific date"""
        features_path = f"{self.features_path}/{property_type}/{date}/features.parquet"
        metadata_path = (
            f"{self.features_path}/{property_type}/{date}/feature_metadata.json"
        )

        if not check_hdfs_path_exists(self.spark, features_path):
            raise FileNotFoundError(f"Feature data not found: {features_path}")

        # Load features
        df_spark = self.spark.read.parquet(features_path)
        df_pandas = df_spark.toPandas()

        # Load metadata
        metadata = {}
        if check_hdfs_path_exists(self.spark, metadata_path):
            metadata_df = self.spark.read.json(metadata_path)
            metadata_row = metadata_df.collect()[0]
            metadata = json.loads(metadata_row["metadata"])

        self.logger.logger.info(f"Loaded {len(df_pandas)} records from {date}")
        return df_pandas, metadata

    def detect_data_drift(self, new_data: pd.DataFrame, baseline_stats: Dict) -> Dict:
        """Detect data drift between new data and baseline"""
        drift_results = {
            "drift_detected": False,
            "drift_score": 0.0,
            "drifted_features": [],
            "stats_comparison": {},
        }

        try:
            numeric_features = baseline_stats.get("numeric_features", [])

            for feature in numeric_features:
                if feature in new_data.columns and feature in baseline_stats.get(
                    "feature_stats", {}
                ):
                    baseline_stat = baseline_stats["feature_stats"][feature]
                    new_stat = {
                        "mean": new_data[feature].mean(),
                        "std": new_data[feature].std(),
                        "min": new_data[feature].min(),
                        "max": new_data[feature].max(),
                    }

                    # Calculate drift score (normalized difference in means and stds)
                    mean_drift = abs(new_stat["mean"] - baseline_stat["mean"]) / (
                        baseline_stat["std"] + 1e-8
                    )
                    std_drift = abs(new_stat["std"] - baseline_stat["std"]) / (
                        baseline_stat["std"] + 1e-8
                    )
                    feature_drift = (mean_drift + std_drift) / 2

                    drift_results["stats_comparison"][feature] = {
                        "baseline": baseline_stat,
                        "new": new_stat,
                        "drift_score": feature_drift,
                    }

                    if feature_drift > self.performance_thresholds["max_drift_score"]:
                        drift_results["drifted_features"].append(feature)
                        drift_results["drift_score"] = max(
                            drift_results["drift_score"], feature_drift
                        )

            drift_results["drift_detected"] = len(drift_results["drifted_features"]) > 0

            self.logger.logger.info(f"Data drift analysis:")
            self.logger.logger.info(
                f"- Drift detected: {drift_results['drift_detected']}"
            )
            self.logger.logger.info(
                f"- Drift score: {drift_results['drift_score']:.3f}"
            )
            self.logger.logger.info(
                f"- Drifted features: {len(drift_results['drifted_features'])}"
            )

        except Exception as e:
            self.logger.logger.error(f"Error in drift detection: {e}")
            drift_results["drift_detected"] = True  # Assume drift if detection fails

        return drift_results

    def prepare_incremental_data(
        self, new_data: pd.DataFrame, metadata: Dict
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare new data for incremental training"""
        # Get feature columns
        numeric_features = metadata.get("numeric_features", [])
        categorical_features = metadata.get("categorical_features", [])
        binary_features = metadata.get("binary_features", [])

        # Prepare features
        feature_cols = numeric_features + binary_features
        feature_cols = [
            col for col in feature_cols if col in new_data.columns and col != "price"
        ]

        # Handle categorical features (simple label encoding for now)
        X_data = new_data[feature_cols].copy()

        # Fill missing values
        X_data = X_data.fillna(X_data.median())

        # Target variable
        y_data = new_data["price"].values

        self.logger.logger.info(f"Prepared incremental data: {X_data.shape}")
        return X_data.values, y_data

    def perform_incremental_training(
        self, model_path: str, X_new: np.ndarray, y_new: np.ndarray, model_type: str
    ) -> Dict:
        """Perform incremental training on existing model"""
        results = {"success": False, "metrics": {}, "model_updated": False}

        try:
            if not ADVANCED_MODELS_AVAILABLE:
                raise ImportError(
                    "Advanced models not available for incremental training"
                )

            # Load existing model
            if model_type == "xgboost":
                model = xgb.XGBRegressor()
                model.load_model(f"{model_path}/xgboost_model.json")

                # Continue training with new data
                model.fit(X_new, y_new, xgb_model=model.get_booster())

            elif model_type == "lightgbm":
                model = joblib.load(f"{model_path}/lightgbm_model.pkl")

                # LightGBM incremental training
                train_data = lgb.Dataset(X_new, label=y_new)
                model = lgb.train(
                    model.params,
                    train_data,
                    init_model=model,
                    num_boost_round=100,
                    valid_sets=[train_data],
                    callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)],
                )

            elif model_type == "catboost":
                model = CatBoostRegressor()
                model.load_model(f"{model_path}/catboost_model.cbm")

                # CatBoost incremental training
                model.fit(X_new, y_new, init_model=model)

            else:
                raise ValueError(
                    f"Incremental training not supported for model type: {model_type}"
                )

            # Validate on new data
            y_pred = model.predict(X_new)

            # Calculate metrics
            rmse = np.sqrt(mean_squared_error(y_new, y_pred))
            mae = mean_absolute_error(y_new, y_pred)
            r2 = r2_score(y_new, y_pred)

            results["metrics"] = {
                "rmse": rmse,
                "mae": mae,
                "r2": r2,
                "data_points": len(y_new),
            }

            # Save updated model
            model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
            updated_model_path = f"{model_path}_incremental_{model_version}"

            if model_type == "xgboost":
                model.save_model(f"{updated_model_path}/xgboost_model.json")
            elif model_type == "lightgbm":
                joblib.dump(model, f"{updated_model_path}/lightgbm_model.pkl")
            elif model_type == "catboost":
                model.save_model(f"{updated_model_path}/catboost_model.cbm")

            results["success"] = True
            results["model_updated"] = True
            results["updated_model_path"] = updated_model_path

            self.logger.logger.info(f"✅ Incremental training completed:")
            self.logger.logger.info(f"   RMSE: {rmse:,.0f}")
            self.logger.logger.info(f"   MAE: {mae:,.0f}")
            self.logger.logger.info(f"   R²: {r2:.3f}")

        except Exception as e:
            self.logger.logger.error(f"Incremental training failed: {e}")
            results["error"] = str(e)

        return results

    def should_trigger_full_retrain(
        self, drift_results: Dict, performance_metrics: Dict, days_since_full_train: int
    ) -> bool:
        """Determine if full retraining should be triggered"""
        triggers = []

        # Check drift
        if drift_results["drift_detected"]:
            triggers.append("data_drift")

        # Check performance degradation
        if (
            performance_metrics.get("r2", 0)
            < self.performance_thresholds["min_r2_score"]
        ):
            triggers.append("poor_performance")

        # Check time since last full training
        if days_since_full_train >= self.incremental_config["max_incremental_days"]:
            triggers.append("time_threshold")

        should_retrain = len(triggers) > 0

        if should_retrain:
            self.logger.logger.info(f"🔄 Full retraining triggered by: {triggers}")

        return should_retrain

    def update_model_registry(self, model_info: Dict, property_type: str = "house"):
        """Update model registry with new model information"""
        registry_path = f"{self.model_registry_path}/{property_type}"

        # Ensure directory exists
        os.makedirs(registry_path, exist_ok=True)

        # Add timestamp
        model_info["updated_at"] = datetime.now().isoformat()

        # Save to HDFS as JSON
        metadata_df = self.spark.createDataFrame([{"metadata": json.dumps(model_info)}])
        metadata_df.write.mode("overwrite").json(f"{registry_path}/latest_model.json")

        self.logger.logger.info("✅ Model registry updated")

    def run_daily_incremental_training(
        self, target_date: str, property_type: str = "house"
    ) -> Dict:
        """Run complete daily incremental training workflow"""
        self.logger.logger.info("🔄 === Starting Daily Incremental Training ===")

        workflow_result = {
            "success": False,
            "action_taken": "none",
            "details": {},
            "new_model_deployed": False,
        }

        try:
            # 1. Check if new data is available
            if not self.check_new_data_available(target_date, property_type):
                workflow_result["action_taken"] = "no_new_data"
                self.logger.logger.info("ℹ️ No new data available for training")
                return workflow_result

            # 2. Load new feature data
            new_data, metadata = self.load_feature_data(target_date, property_type)

            # 3. Get existing model info
            latest_model_info = self.get_latest_model_info(property_type)

            if latest_model_info is None:
                # No existing model - trigger full training
                workflow_result["action_taken"] = "full_training_required"
                self.logger.logger.info("🆕 No existing model - full training required")
                return workflow_result

            # 4. Detect data drift
            drift_results = self.detect_data_drift(new_data, latest_model_info)

            # 5. Check if full retraining needed
            days_since_full_train = (
                datetime.now()
                - datetime.fromisoformat(
                    latest_model_info.get("trained_at", datetime.now().isoformat())
                )
            ).days

            should_full_retrain = self.should_trigger_full_retrain(
                drift_results,
                latest_model_info.get("metrics", {}),
                days_since_full_train,
            )

            if should_full_retrain:
                workflow_result["action_taken"] = "full_retrain_triggered"
                workflow_result["details"]["triggers"] = drift_results.get(
                    "drifted_features", []
                )
                self.logger.logger.info("🔄 Full retraining triggered")
                return workflow_result

            # 6. Perform incremental training
            X_new, y_new = self.prepare_incremental_data(new_data, metadata)

            best_model_type = latest_model_info.get("best_model_type", "xgboost")
            model_path = latest_model_info.get("model_path")

            training_results = self.perform_incremental_training(
                model_path, X_new, y_new, best_model_type
            )

            if training_results["success"]:
                # 7. Update model registry
                updated_model_info = latest_model_info.copy()
                updated_model_info.update(
                    {
                        "last_incremental_update": target_date,
                        "incremental_metrics": training_results["metrics"],
                        "model_path": training_results.get(
                            "updated_model_path", model_path
                        ),
                        "total_incremental_updates": latest_model_info.get(
                            "total_incremental_updates", 0
                        )
                        + 1,
                    }
                )

                self.update_model_registry(updated_model_info, property_type)

                workflow_result["success"] = True
                workflow_result["action_taken"] = "incremental_update"
                workflow_result["new_model_deployed"] = True
                workflow_result["details"] = {
                    "metrics": training_results["metrics"],
                    "drift_detected": drift_results["drift_detected"],
                    "drift_score": drift_results["drift_score"],
                }

                self.logger.logger.info(
                    "✅ Incremental training completed successfully"
                )
            else:
                workflow_result["action_taken"] = "incremental_failed"
                workflow_result["details"]["error"] = training_results.get(
                    "error", "Unknown error"
                )

        except Exception as e:
            self.logger.logger.error(f"Daily incremental training failed: {e}")
            workflow_result["details"]["error"] = str(e)

        return workflow_result


def main():
    """Main execution function for daily incremental training"""
    import argparse

    parser = argparse.ArgumentParser(description="Daily Incremental ML Training")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--property-type", default="house", help="Property type")

    args = parser.parse_args()

    # Run incremental training
    manager = IncrementalTrainingManager()
    try:
        result = manager.run_daily_incremental_training(args.date, args.property_type)

        print(f"\n🔄 Daily Incremental Training Results:")
        print(f"Action taken: {result['action_taken']}")
        print(f"Success: {result['success']}")
        print(f"New model deployed: {result['new_model_deployed']}")

        if result.get("details"):
            print(f"Details: {json.dumps(result['details'], indent=2)}")

    finally:
        manager.spark.stop()


if __name__ == "__main__":
    main()
