#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Enhanced Model Factory với configuration support
"""

import os
import sys
from typing import Dict, Any, Optional, List
import logging

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from ml.base_model import BaseModel
from ml.price_prediction_model import PricePredictionModel
from ml.config_manager import MLConfig

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EnhancedModelFactory:
    """
    Enhanced Model Factory với configuration management
    """

    _model_registry = {"price_prediction": PricePredictionModel}

    @classmethod
    def register_model(cls, model_name: str, model_class: type):
        """
        Đăng ký model class mới

        Args:
            model_name: Tên model
            model_class: Class của model
        """
        cls._model_registry[model_name] = model_class
        logger.info(f"✅ Registered model: {model_name}")

    @classmethod
    def list_available_models(cls) -> List[str]:
        """
        Liệt kê các models có sẵn

        Returns:
            List[str]: Danh sách model names
        """
        return list(cls._model_registry.keys())

    @classmethod
    def create_model(
        cls,
        model_name: str,
        config: Optional[MLConfig] = None,
        model_type: Optional[str] = None,
        categorical_cols: Optional[List[str]] = None,
        numerical_cols: Optional[List[str]] = None,
        **kwargs,
    ) -> BaseModel:
        """
        Tạo model instance với configuration

        Args:
            model_name: Tên model
            config: Configuration object
            model_type: Override model type
            categorical_cols: Override categorical columns
            numerical_cols: Override numerical columns
            **kwargs: Additional parameters

        Returns:
            BaseModel: Model instance
        """
        if model_name not in cls._model_registry:
            available_models = ", ".join(cls._model_registry.keys())
            raise ValueError(
                f"Model '{model_name}' not found. Available: {available_models}"
            )

        model_class = cls._model_registry[model_name]

        # Load configuration if not provided
        if config is None:
            config = MLConfig()

        # Get model configuration
        model_config = config.get_model_config(model_name)

        # Override with provided parameters
        if model_type:
            model_config["type"] = model_type

        # Prepare model parameters
        model_params = {"model_type": model_config["type"]}

        # Add categorical and numerical columns if provided
        if categorical_cols:
            model_params["categorical_cols"] = categorical_cols
        elif config:
            # Get from configuration
            feature_config = config.get("feature_engineering", {})
            model_params["categorical_cols"] = feature_config.get(
                "categorical_features", []
            )

        if numerical_cols:
            model_params["numerical_cols"] = numerical_cols
        elif config:
            # Get from configuration
            feature_config = config.get("feature_engineering", {})
            model_params["numerical_cols"] = feature_config.get(
                "numerical_features", []
            )

        # Add any additional kwargs
        model_params.update(kwargs)

        # Create model instance
        try:
            model = model_class(**model_params)

            # Apply hyperparameters from config if available
            hyperparams = model_config.get("hyperparameters", {})
            if hasattr(model, "apply_hyperparameters"):
                model.apply_hyperparameters(hyperparams)

            logger.info(
                f"✅ Created {model_name} model with type: {model_config['type']}"
            )
            return model

        except Exception as e:
            logger.error(f"❌ Failed to create {model_name} model: {str(e)}")
            raise

    @classmethod
    def create_model_with_auto_config(
        cls,
        model_name: str,
        training_data_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> BaseModel:
        """
        Tạo model với auto configuration dựa trên data

        Args:
            model_name: Tên model
            training_data_info: Thông tin về training data
            **kwargs: Additional parameters

        Returns:
            BaseModel: Model instance
        """
        config = MLConfig()

        # Auto-adjust configuration based on data
        if training_data_info:
            data_size = training_data_info.get("record_count", 0)

            # Adjust model complexity based on data size
            if data_size < 5000:
                # Small dataset - use simpler model
                if model_name == "price_prediction":
                    config.set(
                        "models.price_prediction.default_type", "linear_regression"
                    )
                    config.set(
                        "models.price_prediction.hyperparameters.linear_regression.maxIter",
                        50,
                    )
            elif data_size < 50000:
                # Medium dataset - use Random Forest with moderate complexity
                if model_name == "price_prediction":
                    config.set("models.price_prediction.default_type", "random_forest")
                    config.set(
                        "models.price_prediction.hyperparameters.random_forest.numTrees",
                        50,
                    )
                    config.set(
                        "models.price_prediction.hyperparameters.random_forest.maxDepth",
                        8,
                    )
            else:
                # Large dataset - use GBT or full Random Forest
                if model_name == "price_prediction":
                    config.set("models.price_prediction.default_type", "gbt")

            # Adjust feature engineering based on data characteristics
            feature_count = training_data_info.get("feature_count", 0)
            if feature_count > 100:
                # Many features - enable more aggressive feature engineering
                config.set(
                    "feature_engineering.derived_features.enable_interaction_features",
                    True,
                )

            logger.info(
                f"📊 Auto-configured model for {data_size:,} records and {feature_count} features"
            )

        return cls.create_model(model_name, config=config, **kwargs)

    @classmethod
    def create_ensemble_model(
        cls,
        model_name: str,
        ensemble_types: List[str],
        config: Optional[MLConfig] = None,
        **kwargs,
    ) -> List[BaseModel]:
        """
        Tạo ensemble của nhiều models

        Args:
            model_name: Base model name
            ensemble_types: List các model types để tạo ensemble
            config: Configuration object
            **kwargs: Additional parameters

        Returns:
            List[BaseModel]: List of model instances
        """
        if config is None:
            config = MLConfig()

        models = []

        for model_type in ensemble_types:
            try:
                model = cls.create_model(
                    model_name=model_name,
                    config=config,
                    model_type=model_type,
                    **kwargs,
                )
                models.append(model)
                logger.info(f"✅ Created ensemble member: {model_type}")

            except Exception as e:
                logger.error(
                    f"❌ Failed to create ensemble member {model_type}: {str(e)}"
                )

        logger.info(f"🎯 Created ensemble with {len(models)} models")
        return models

    @classmethod
    def create_model_for_experiment(
        cls,
        model_name: str,
        experiment_config: Dict[str, Any],
        base_config: Optional[MLConfig] = None,
    ) -> BaseModel:
        """
        Tạo model cho experiments/hyperparameter tuning

        Args:
            model_name: Tên model
            experiment_config: Experiment configuration
            base_config: Base configuration

        Returns:
            BaseModel: Model instance
        """
        if base_config is None:
            base_config = MLConfig()

        # Create experiment-specific config
        exp_config = MLConfig()
        exp_config.config = base_config.config.copy()

        # Apply experiment overrides
        model_type = experiment_config.get("model_type")
        if model_type:
            exp_config.set(f"models.{model_name}.default_type", model_type)

        # Apply hyperparameter overrides
        hyperparams = experiment_config.get("hyperparameters", {})
        for param, value in hyperparams.items():
            exp_config.set(
                f"models.{model_name}.hyperparameters.{model_type}.{param}", value
            )

        # Apply feature engineering overrides
        feature_config = experiment_config.get("feature_engineering", {})
        for key, value in feature_config.items():
            exp_config.set(f"feature_engineering.{key}", value)

        logger.info(f"🧪 Creating experimental model: {experiment_config}")

        return cls.create_model(model_name, config=exp_config)


# Maintain backward compatibility
class ModelFactory(EnhancedModelFactory):
    """Backward compatibility alias"""

    @classmethod
    def create_model(
        cls,
        model_name: str,
        model_type: Optional[str] = None,
        categorical_cols: Optional[List[str]] = None,
        numerical_cols: Optional[List[str]] = None,
        **kwargs,
    ) -> BaseModel:
        """
        Legacy create_model method for backward compatibility
        """
        return super().create_model(
            model_name=model_name,
            model_type=model_type,
            categorical_cols=categorical_cols,
            numerical_cols=numerical_cols,
            **kwargs,
        )


def main():
    """Test model factory"""
    # Test basic model creation
    factory = EnhancedModelFactory()

    print("Available models:", factory.list_available_models())

    # Test with default config
    model1 = factory.create_model("price_prediction")
    print(f"Created model 1: {type(model1).__name__}")

    # Test with custom config
    config = MLConfig()
    config.set("models.price_prediction.default_type", "gbt")

    model2 = factory.create_model("price_prediction", config=config)
    print(f"Created model 2: {type(model2).__name__}")

    # Test auto configuration
    training_info = {"record_count": 75000, "feature_count": 25}

    model3 = factory.create_model_with_auto_config("price_prediction", training_info)
    print(f"Created auto-configured model: {type(model3).__name__}")

    # Test ensemble
    ensemble = factory.create_ensemble_model(
        "price_prediction", ["random_forest", "gbt", "linear_regression"]
    )
    print(f"Created ensemble with {len(ensemble)} models")


if __name__ == "__main__":
    main()
