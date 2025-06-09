# filepath: /home/fer/data/real_estate_project/data_processing/ml/utils/model_utils.py
"""
🤖 ML Model Utilities
====================

Model management, evaluation, and deployment utilities
for real estate ML pipelines.

Author: ML Team
Date: June 2025
"""

import os
import json
import pickle
import joblib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Union
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    mean_absolute_percentage_error,
)
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelMetrics:
    """📊 Model performance metrics container"""

    def __init__(self):
        self.mae: float = 0.0
        self.mse: float = 0.0
        self.rmse: float = 0.0
        self.r2: float = 0.0
        self.mape: float = 0.0
        self.median_ae: float = 0.0
        self.max_error: float = 0.0
        self.predictions_count: int = 0
        self.calculated_at: str = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            "mae": self.mae,
            "mse": self.mse,
            "rmse": self.rmse,
            "r2": self.r2,
            "mape": self.mape,
            "median_ae": self.median_ae,
            "max_error": self.max_error,
            "predictions_count": self.predictions_count,
            "calculated_at": self.calculated_at,
        }


class ModelEvaluator:
    """📊 Model evaluation and metrics calculation"""

    def __init__(self):
        logger.info("📊 Model Evaluator initialized")

    def evaluate_regression(
        self, y_true: np.ndarray, y_pred: np.ndarray
    ) -> ModelMetrics:
        """
        📈 Comprehensive regression evaluation

        Args:
            y_true: Actual values
            y_pred: Predicted values

        Returns:
            ModelMetrics: Complete evaluation metrics
        """
        logger.info("📈 Evaluating regression model")

        metrics = ModelMetrics()

        try:
            # Basic metrics
            metrics.mae = float(mean_absolute_error(y_true, y_pred))
            metrics.mse = float(mean_squared_error(y_true, y_pred))
            metrics.rmse = float(np.sqrt(metrics.mse))
            metrics.r2 = float(r2_score(y_true, y_pred))

            # MAPE (handle division by zero)
            mape_values = np.abs((y_true - y_pred) / np.maximum(y_true, 1e-8))
            metrics.mape = float(np.mean(mape_values))

            # Additional metrics
            abs_errors = np.abs(y_true - y_pred)
            metrics.median_ae = float(np.median(abs_errors))
            metrics.max_error = float(np.max(abs_errors))
            metrics.predictions_count = len(y_true)

            # Log metrics
            logger.info(f"📊 Evaluation Results:")
            logger.info(f"  MAE: {metrics.mae:,.0f} VND")
            logger.info(f"  RMSE: {metrics.rmse:,.0f} VND")
            logger.info(f"  R²: {metrics.r2:.3f}")
            logger.info(f"  MAPE: {metrics.mape:.3f}")

        except Exception as e:
            logger.error(f"❌ Evaluation failed: {str(e)}")
            raise

        return metrics

    def evaluate_cross_validation(
        self,
        model,
        X: np.ndarray,
        y: np.ndarray,
        cv: int = 5,
        scoring: str = "neg_mean_absolute_error",
    ) -> Dict[str, float]:
        """
        🔄 Cross-validation evaluation

        Args:
            model: Trained model
            X: Features
            y: Target values
            cv: Number of folds
            scoring: Scoring metric

        Returns:
            Dict: Cross-validation results
        """
        logger.info(f"🔄 Running {cv}-fold cross-validation")

        try:
            scores = cross_val_score(model, X, y, cv=cv, scoring=scoring)

            results = {
                "mean_score": float(np.mean(scores)),
                "std_score": float(np.std(scores)),
                "min_score": float(np.min(scores)),
                "max_score": float(np.max(scores)),
                "cv_folds": cv,
                "scoring": scoring,
            }

            logger.info(
                f"📊 CV Results: {results['mean_score']:.3f} ± {results['std_score']:.3f}"
            )

            return results

        except Exception as e:
            logger.error(f"❌ Cross-validation failed: {str(e)}")
            raise

    def compare_models(self, model_results: Dict[str, ModelMetrics]) -> Dict[str, Any]:
        """
        🏆 Compare multiple models and rank them

        Args:
            model_results: Dictionary of model_name -> ModelMetrics

        Returns:
            Dict: Comparison results with rankings
        """
        logger.info("🏆 Comparing models")

        if not model_results:
            return {}

        # Create comparison DataFrame
        comparison_data = []
        for model_name, metrics in model_results.items():
            comparison_data.append(
                {
                    "model": model_name,
                    "mae": metrics.mae,
                    "rmse": metrics.rmse,
                    "r2": metrics.r2,
                    "mape": metrics.mape,
                }
            )

        df = pd.DataFrame(comparison_data)

        # Rank models (lower is better for MAE, RMSE, MAPE; higher is better for R²)
        df["mae_rank"] = df["mae"].rank()
        df["rmse_rank"] = df["rmse"].rank()
        df["mape_rank"] = df["mape"].rank()
        df["r2_rank"] = df["r2"].rank(ascending=False)

        # Calculate overall rank (average of individual ranks)
        df["overall_rank"] = (
            df["mae_rank"] + df["rmse_rank"] + df["mape_rank"] + df["r2_rank"]
        ) / 4
        df = df.sort_values("overall_rank")

        # Best model
        best_model = df.iloc[0]["model"]

        results = {
            "best_model": best_model,
            "rankings": df.to_dict("records"),
            "comparison_summary": {
                "total_models": len(model_results),
                "best_mae": float(df["mae"].min()),
                "best_rmse": float(df["rmse"].min()),
                "best_r2": float(df["r2"].max()),
                "best_mape": float(df["mape"].min()),
            },
        }

        logger.info(f"🏆 Best model: {best_model}")

        return results


class ModelManager:
    """🔧 Model persistence and versioning"""

    def __init__(self, model_store_path: str = "/data/realestate/models"):
        self.model_store_path = model_store_path
        self.ensure_directories()
        logger.info(f"🔧 Model Manager initialized at {model_store_path}")

    def ensure_directories(self):
        """Create required directories"""
        os.makedirs(self.model_store_path, exist_ok=True)
        os.makedirs(f"{self.model_store_path}/models", exist_ok=True)
        os.makedirs(f"{self.model_store_path}/metadata", exist_ok=True)
        os.makedirs(f"{self.model_store_path}/artifacts", exist_ok=True)

    def save_model(
        self,
        model: Any,
        model_name: str,
        version: str = None,
        metadata: Dict[str, Any] = None,
        format: str = "joblib",
    ) -> str:
        """
        💾 Save model with metadata and versioning

        Args:
            model: Trained model object
            model_name: Name of the model
            version: Model version (auto-generated if None)
            metadata: Additional metadata
            format: Serialization format ('joblib', 'pickle')

        Returns:
            str: Path where model was saved
        """
        if version is None:
            version = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info(f"💾 Saving model {model_name} v{version}")

        # Create version directory
        version_path = f"{self.model_store_path}/models/{model_name}/{version}"
        os.makedirs(version_path, exist_ok=True)

        # Save model
        model_path = f"{version_path}/model.{format}"
        if format == "joblib":
            joblib.dump(model, model_path)
        elif format == "pickle":
            with open(model_path, "wb") as f:
                pickle.dump(model, f)
        else:
            raise ValueError(f"Unsupported format: {format}")

        # Save metadata
        model_metadata = {
            "model_name": model_name,
            "version": version,
            "format": format,
            "saved_at": datetime.now().isoformat(),
            "model_type": type(model).__name__,
            "file_size": os.path.getsize(model_path),
            "file_path": model_path,
        }

        if metadata:
            model_metadata.update(metadata)

        metadata_path = f"{version_path}/metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(model_metadata, f, indent=2, default=str)

        # Update latest symlink
        latest_path = f"{self.model_store_path}/models/{model_name}/latest"
        if os.path.exists(latest_path):
            os.remove(latest_path)
        os.symlink(version_path, latest_path)

        logger.info(f"💾 Model saved to {version_path}")

        return version_path

    def load_model(
        self, model_name: str, version: str = "latest"
    ) -> Tuple[Any, Dict[str, Any]]:
        """
        📖 Load model and metadata

        Args:
            model_name: Name of the model
            version: Model version or "latest"

        Returns:
            Tuple[model, metadata]: Loaded model and metadata
        """
        logger.info(f"📖 Loading model {model_name} v{version}")

        model_path = f"{self.model_store_path}/models/{model_name}/{version}"

        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")

        # Load metadata
        metadata_path = f"{model_path}/metadata.json"
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        # Load model
        format = metadata.get("format", "joblib")
        model_file = f"{model_path}/model.{format}"

        if format == "joblib":
            model = joblib.load(model_file)
        elif format == "pickle":
            with open(model_file, "rb") as f:
                model = pickle.load(f)
        else:
            raise ValueError(f"Unsupported format: {format}")

        logger.info(f"📖 Model loaded successfully")

        return model, metadata

    def list_models(self, model_name: str = None) -> List[Dict[str, Any]]:
        """📋 List available models and versions"""
        models = []

        models_dir = f"{self.model_store_path}/models"
        if not os.path.exists(models_dir):
            return models

        for model_dir in os.listdir(models_dir):
            if model_name and model_dir != model_name:
                continue

            model_path = f"{models_dir}/{model_dir}"
            if not os.path.isdir(model_path):
                continue

            for version_dir in os.listdir(model_path):
                if version_dir == "latest":
                    continue

                version_path = f"{model_path}/{version_dir}"
                metadata_path = f"{version_path}/metadata.json"

                if os.path.exists(metadata_path):
                    with open(metadata_path, "r") as f:
                        metadata = json.load(f)
                    models.append(metadata)

        return sorted(models, key=lambda x: x.get("saved_at", ""), reverse=True)

    def delete_model(self, model_name: str, version: str) -> bool:
        """🗑️ Delete a specific model version"""
        if version == "latest":
            raise ValueError("Cannot delete 'latest' version directly")

        model_path = f"{self.model_store_path}/models/{model_name}/{version}"

        if not os.path.exists(model_path):
            logger.warning(f"⚠️ Model not found: {model_path}")
            return False

        import shutil

        shutil.rmtree(model_path)

        logger.info(f"🗑️ Deleted model {model_name} v{version}")

        return True


class ModelDeployment:
    """🚀 Model deployment utilities"""

    def __init__(self, model_manager: ModelManager):
        self.model_manager = model_manager
        logger.info("🚀 Model Deployment initialized")

    def create_prediction_pipeline(
        self, model_name: str, version: str = "latest"
    ) -> Dict[str, Any]:
        """
        🔧 Create prediction pipeline with preprocessing

        Args:
            model_name: Name of the model
            version: Model version

        Returns:
            Dict: Pipeline configuration
        """
        logger.info(f"🔧 Creating prediction pipeline for {model_name}")

        model, metadata = self.model_manager.load_model(model_name, version)

        pipeline = {
            "model": model,
            "metadata": metadata,
            "preprocessing_steps": [],
            "feature_columns": metadata.get("feature_columns", []),
            "target_column": metadata.get("target_column", "price"),
            "created_at": datetime.now().isoformat(),
        }

        return pipeline

    def batch_predict(
        self, pipeline: Dict[str, Any], data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        📊 Batch prediction with pipeline

        Args:
            pipeline: Prediction pipeline
            data: Input data

        Returns:
            DataFrame: Data with predictions
        """
        logger.info(f"📊 Running batch prediction on {len(data)} records")

        try:
            model = pipeline["model"]
            feature_columns = pipeline["feature_columns"]

            # Select and order features
            if feature_columns:
                missing_cols = [
                    col for col in feature_columns if col not in data.columns
                ]
                if missing_cols:
                    raise ValueError(f"Missing required columns: {missing_cols}")

                X = data[feature_columns]
            else:
                # Use all numeric columns except target
                target_col = pipeline.get("target_column", "price")
                numeric_cols = data.select_dtypes(include=[np.number]).columns
                X = data[numeric_cols.drop(target_col, errors="ignore")]

            # Make predictions
            predictions = model.predict(X)

            # Add predictions to DataFrame
            result = data.copy()
            result["predicted_price"] = predictions
            result["prediction_date"] = datetime.now().isoformat()

            logger.info(f"📊 Batch prediction completed")

            return result

        except Exception as e:
            logger.error(f"❌ Batch prediction failed: {str(e)}")
            raise

    def export_model_for_serving(
        self, model_name: str, version: str = "latest", output_format: str = "onnx"
    ) -> str:
        """
        📦 Export model for production serving

        Args:
            model_name: Name of the model
            version: Model version
            output_format: Export format ('onnx', 'tflite', 'torchscript')

        Returns:
            str: Path to exported model
        """
        logger.info(f"📦 Exporting model {model_name} to {output_format}")

        model, metadata = self.model_manager.load_model(model_name, version)

        # Export path
        export_path = f"{self.model_manager.model_store_path}/exports/{model_name}_{version}.{output_format}"
        os.makedirs(os.path.dirname(export_path), exist_ok=True)

        if output_format == "onnx":
            # ONNX export (requires skl2onnx for sklearn models)
            try:
                from skl2onnx import convert_sklearn
                from skl2onnx.common.data_types import FloatTensorType

                # Determine input shape from metadata
                feature_count = len(metadata.get("feature_columns", []))
                initial_type = [("float_input", FloatTensorType([None, feature_count]))]

                onx = convert_sklearn(model, initial_types=initial_type)
                with open(export_path, "wb") as f:
                    f.write(onx.SerializeToString())

                logger.info(f"📦 Model exported to {export_path}")

            except ImportError:
                logger.error("❌ skl2onnx not installed. Cannot export to ONNX")
                raise
        else:
            raise ValueError(f"Unsupported export format: {output_format}")

        return export_path


def calculate_business_metrics(
    y_true: np.ndarray, y_pred: np.ndarray
) -> Dict[str, float]:
    """
    💰 Calculate business-specific metrics for real estate predictions

    Args:
        y_true: Actual prices
        y_pred: Predicted prices

    Returns:
        Dict: Business metrics
    """
    logger.info("💰 Calculating business metrics")

    # Price ranges for analysis
    ranges = {
        "budget": (0, 3_000_000_000),  # < 3B VND
        "mid_range": (3_000_000_000, 8_000_000_000),  # 3-8B VND
        "premium": (8_000_000_000, float("inf")),  # > 8B VND
    }

    metrics = {}

    for range_name, (min_price, max_price) in ranges.items():
        mask = (y_true >= min_price) & (y_true < max_price)

        if mask.sum() > 0:
            range_true = y_true[mask]
            range_pred = y_pred[mask]

            metrics[f"{range_name}_mae"] = float(
                mean_absolute_error(range_true, range_pred)
            )
            metrics[f"{range_name}_mape"] = float(
                np.mean(np.abs((range_true - range_pred) / range_true))
            )
            metrics[f"{range_name}_count"] = int(mask.sum())

    # Overall business metrics
    errors = np.abs(y_true - y_pred)
    percentage_errors = errors / y_true

    metrics.update(
        {
            "within_10_percent": float(
                np.mean(percentage_errors <= 0.10)
            ),  # % within 10%
            "within_20_percent": float(
                np.mean(percentage_errors <= 0.20)
            ),  # % within 20%
            "median_percentage_error": float(np.median(percentage_errors)),
            "avg_absolute_error_billion": float(np.mean(errors))
            / 1_000_000_000,  # In billions VND
        }
    )

    return metrics


if __name__ == "__main__":
    # Example usage

    # Create sample data for testing
    np.random.seed(42)
    y_true = np.random.normal(5_000_000_000, 2_000_000_000, 1000)  # 5B ± 2B VND
    y_pred = y_true + np.random.normal(0, 500_000_000, 1000)  # Add some noise

    # Test evaluator
    evaluator = ModelEvaluator()
    metrics = evaluator.evaluate_regression(y_true, y_pred)

    print("Evaluation Results:")
    for key, value in metrics.to_dict().items():
        if isinstance(value, float) and key in ["mae", "mse", "rmse"]:
            print(f"{key}: {value:,.0f} VND")
        else:
            print(f"{key}: {value}")

    # Test business metrics
    business_metrics = calculate_business_metrics(y_true, y_pred)
    print("\nBusiness Metrics:")
    for key, value in business_metrics.items():
        print(f"{key}: {value}")
