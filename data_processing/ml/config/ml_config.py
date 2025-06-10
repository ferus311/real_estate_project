# filepath: /home/fer/data/real_estate_project/data_processing/ml/config/ml_config.py
"""
🎛️ ML Configuration Management
=============================

Centralized configuration for all ML components including models,
features, training parameters, and deployment settings.

Author: ML Team
Date: June 2025
"""

import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DataConfig:
    """📊 Data processing configuration"""

    # Data paths - using unified path configuration
    gold_data_path: str = "/data/realestate/processed/gold"
    feature_store_path: str = "/data/realestate/ml/features"
    model_store_path: str = "/data/realestate/ml/models"

    # Data quality thresholds
    missing_threshold: float = 0.3  # Drop features with >30% missing
    outlier_threshold: float = 3.0  # Z-score threshold for outliers
    min_samples: int = 100  # Minimum samples required for training

    # Feature engineering
    lookback_days: int = 30  # Days to include in training data
    correlation_threshold: float = 0.95  # Drop highly correlated features
    variance_threshold: float = 0.01  # Drop low variance features

    # Validation
    test_size: float = 0.2
    validation_size: float = 0.15
    random_state: int = 42


@dataclass
class ModelConfig:
    """🤖 Model training configuration"""

    # XGBoost parameters
    xgboost_params: Dict[str, Any] = field(
        default_factory=lambda: {
            "n_estimators": 1000,
            "max_depth": 8,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
            "n_jobs": -1,
            "early_stopping_rounds": 50,
            "eval_metric": "rmse",
        }
    )

    # LightGBM parameters
    lightgbm_params: Dict[str, Any] = field(
        default_factory=lambda: {
            "n_estimators": 1000,
            "max_depth": 8,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
            "n_jobs": -1,
            "early_stopping_rounds": 50,
            "metric": "rmse",
            "verbose": -1,
        }
    )

    # Neural Network parameters
    neural_net_params: Dict[str, Any] = field(
        default_factory=lambda: {
            "hidden_layers": [256, 128, 64, 32],
            "dropout_rate": 0.3,
            "batch_size": 512,
            "epochs": 100,
            "learning_rate": 0.001,
            "early_stopping_patience": 10,
            "validation_split": 0.15,
        }
    )

    # Ensemble parameters
    ensemble_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "xgboost": 0.3,
            "lightgbm": 0.3,
            "catboost": 0.25,
            "neural_net": 0.15,
        }
    )


@dataclass
class FeatureConfig:
    """🔧 Feature engineering configuration"""

    # Target variable
    target_column: str = "price"

    # Key features
    key_features: List[str] = field(
        default_factory=lambda: [
            "area",
            "bedrooms",
            "bathrooms",
            "floors",
            "district",
            "ward",
            "lat",
            "lng",
            "property_type",
            "legal_status",
            "furniture",
        ]
    )

    # Categorical features
    categorical_features: List[str] = field(
        default_factory=lambda: [
            "district",
            "ward",
            "property_type",
            "legal_status",
            "furniture",
            "direction",
            "balcony_direction",
            "structure_type",
        ]
    )

    # Numerical features
    numerical_features: List[str] = field(
        default_factory=lambda: [
            "area",
            "bedrooms",
            "bathrooms",
            "floors",
            "lat",
            "lng",
            "frontage",
            "entrance_width",
            "floor_number",
            "total_floors",
        ]
    )

    # Text features
    text_features: List[str] = field(
        default_factory=lambda: ["title", "description", "address"]
    )

    # Features to exclude from training
    exclude_features: List[str] = field(
        default_factory=lambda: [
            "id",
            "url",
            "crawl_date",
            "data_date",
            "created_at",
            "updated_at",
            "source",
            "raw_data",
            "images",
        ]
    )

    # Feature selection parameters
    feature_selection_k: int = 50  # Top K features to select
    feature_selection_method: str = "correlation"  # correlation, mutual_info, chi2

    # Scaling method
    scaling_method: str = "standard"  # standard, minmax, robust


@dataclass
class TrainingConfig:
    """🏋️ Training configuration"""

    # Cross-validation
    cv_folds: int = 5
    cv_scoring: str = "neg_mean_absolute_error"

    # Hyperparameter tuning
    hyperopt_max_evals: int = 100
    hyperopt_trials_path: str = "/tmp/hyperopt_trials"

    # Model persistence
    model_save_format: str = "joblib"  # joblib, pickle, mlflow
    save_best_only: bool = True

    # Training schedule
    retrain_frequency_days: int = 7
    incremental_learning: bool = True

    # Performance thresholds
    min_r2_score: float = 0.7
    max_mae_threshold: float = 500_000_000  # 500M VND
    max_mape_threshold: float = 0.25  # 25%


@dataclass
class DeploymentConfig:
    """🚀 Deployment configuration"""

    # Model serving
    model_registry_path: str = "/data/realestate/models/registry"
    model_api_port: int = 8080
    batch_prediction_schedule: str = "0 2 * * *"  # Daily at 2 AM

    # Monitoring
    drift_detection_threshold: float = 0.1
    performance_monitoring_window: int = 7  # days
    alert_thresholds: Dict[str, float] = field(
        default_factory=lambda: {
            "mae_increase": 0.2,  # 20% increase in MAE
            "prediction_drift": 0.15,  # 15% drift in predictions
            "data_drift": 0.1,  # 10% drift in input data
        }
    )

    # A/B testing
    ab_test_traffic_split: float = 0.1  # 10% for new model
    ab_test_duration_days: int = 14

    # Rollback conditions
    auto_rollback_enabled: bool = True
    rollback_mae_threshold: float = 600_000_000  # 600M VND


class MLConfig:
    """🎛️ Main ML configuration class"""

    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.data = DataConfig()
        self.model = ModelConfig()
        self.features = FeatureConfig()
        self.training = TrainingConfig()
        self.deployment = DeploymentConfig()

        # Environment-specific overrides
        self._apply_environment_config()

    def _apply_environment_config(self):
        """Apply environment-specific configuration overrides"""
        if self.environment == "production":
            # Production settings
            self.data.min_samples = 1000
            self.training.cv_folds = 10
            self.training.hyperopt_max_evals = 200
            self.deployment.auto_rollback_enabled = True

        elif self.environment == "development":
            # Development settings
            self.data.min_samples = 50
            self.training.cv_folds = 3
            self.training.hyperopt_max_evals = 20
            self.deployment.auto_rollback_enabled = False

        elif self.environment == "testing":
            # Testing settings
            self.data.min_samples = 20
            self.training.cv_folds = 2
            self.training.hyperopt_max_evals = 5

    def get_model_params(self, model_name: str) -> Dict[str, Any]:
        """Get parameters for a specific model"""
        params_map = {
            "xgboost": self.model.xgboost_params,
            "lightgbm": self.model.lightgbm_params,
            "catboost": self.model.catboost_params,
            "neural_net": self.model.neural_net_params,
        }
        return params_map.get(model_name, {})

    def get_feature_columns(self, feature_type: str = "all") -> List[str]:
        """Get feature columns by type"""
        if feature_type == "categorical":
            return self.features.categorical_features
        elif feature_type == "numerical":
            return self.features.numerical_features
        elif feature_type == "text":
            return self.features.text_features
        elif feature_type == "key":
            return self.features.key_features
        else:
            # Return all features except excluded ones
            all_features = (
                self.features.key_features
                + self.features.categorical_features
                + self.features.numerical_features
                + self.features.text_features
            )
            return [f for f in all_features if f not in self.features.exclude_features]

    def validate_config(self) -> bool:
        """Validate configuration consistency"""
        errors = []

        # Check data paths exist
        required_paths = [
            self.data.gold_data_path,
            self.data.feature_store_path,
            self.data.model_store_path,
        ]

        for path in required_paths:
            if not os.path.exists(path):
                errors.append(f"Path does not exist: {path}")

        # Check threshold values
        if not 0 < self.data.test_size < 1:
            errors.append("test_size must be between 0 and 1")

        if not 0 < self.data.validation_size < 1:
            errors.append("validation_size must be between 0 and 1")

        # Check ensemble weights sum to 1
        weight_sum = sum(self.model.ensemble_weights.values())
        if abs(weight_sum - 1.0) > 0.01:
            errors.append(f"Ensemble weights sum to {weight_sum}, should be 1.0")

        if errors:
            print("❌ Configuration validation errors:")
            for error in errors:
                print(f"  - {error}")
            return False

        print("✅ Configuration validation passed")
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "environment": self.environment,
            "data": self.data.__dict__,
            "model": self.model.__dict__,
            "features": self.features.__dict__,
            "training": self.training.__dict__,
            "deployment": self.deployment.__dict__,
        }

    def save_config(self, path: str):
        """Save configuration to JSON file"""
        import json

        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        print(f"💾 Configuration saved to {path}")


# Global configuration instance
config = MLConfig(environment=os.getenv("ML_ENVIRONMENT", "development"))


def get_config(environment: str = None) -> MLConfig:
    """Get ML configuration instance"""
    if environment:
        return MLConfig(environment=environment)
    return config


if __name__ == "__main__":
    # Test configuration
    test_config = get_config("development")
    test_config.validate_config()
    print(f"🎛️ ML Configuration loaded for {test_config.environment} environment")
