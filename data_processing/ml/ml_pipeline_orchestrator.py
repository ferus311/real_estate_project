"""
🎯 ML Pipeline Orchestrator - Unified Architecture
================================================

Orchestrates the complete ML pipeline:
Data Preparation → Feature Engineering → Model Training

Features:
- 🔄 Unified data preparation pipeline integration
- 🤖 Advanced model training coordination
- 📊 Comprehensive error handling and logging
- 🏗️ Modular, maintainable design

Author: ML Team
Date: June 2025
Version: 3.0 - Clean Architecture
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add project paths
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_optimized_ml_spark_session


class MLPipelineOrchestrator:
    """
    🎯 ML Pipeline Orchestrator - Coordinates the complete ML workflow

    Integrates:
    1. Unified Data Preparation Pipeline
    2. Advanced ML Model Training
    3. Model Evaluation and Deployment
    """

    def __init__(self, spark_session=None):
        self.spark = (
            spark_session
            if spark_session
            else create_optimized_ml_spark_session("ML_Pipeline_Orchestrator")
        )
        self.logger = SparkJobLogger("ml_pipeline_orchestrator")

    def run_full_pipeline(
        self, input_date: str, property_type: str = "house"
    ) -> Dict[str, Any]:
        """
        🚀 Run complete ML pipeline: Data Prep + Training

        Args:
            input_date: Date to process (YYYY-MM-DD)
            property_type: Property type to process

        Returns:
            Dict containing results from both data preparation and training
        """
        self.logger.logger.info(
            f"🚀 Starting full ML pipeline for {property_type} on {input_date}"
        )

        try:
            # Step 1: Run data preparation
            prep_results = self.run_data_preparation_only(input_date, property_type)

            if not prep_results.get("success", False):
                raise Exception(
                    f"Data preparation failed: {prep_results.get('error', 'Unknown error')}"
                )

            # Step 2: Run model training
            training_results = self.run_training_only(input_date, property_type)

            if not training_results.get("success", False):
                raise Exception(
                    f"Model training failed: {training_results.get('error', 'Unknown error')}"
                )

            # Combine results
            combined_results = {
                "success": True,
                "preparation_results": prep_results,
                "training_results": training_results,
                "pipeline_duration": prep_results.get("duration", 0)
                + training_results.get("duration", 0),
                "total_records": prep_results.get("record_count", 0),
                "best_model": training_results.get("best_model", "unknown"),
                "model_metrics": training_results.get("best_model_metrics", {}),
                "date": input_date,
                "property_type": property_type,
            }

            self.logger.logger.info(f"✅ Full ML pipeline completed successfully!")
            self.logger.logger.info(f"🏆 Best model: {combined_results['best_model']}")
            self.logger.logger.info(
                f"📊 Total records: {combined_results['total_records']:,}"
            )

            return combined_results

        except Exception as e:
            error_msg = f"❌ Full ML pipeline failed: {str(e)}"
            self.logger.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "preparation_results": {},
                "training_results": {},
            }

    def run_data_preparation_only(
        self, input_date: str, property_type: str = "house"
    ) -> Dict[str, Any]:
        """
        📊 Run only data preparation pipeline

        Args:
            input_date: Date to process
            property_type: Property type to process

        Returns:
            Dict containing data preparation results
        """
        self.logger.logger.info(
            f"📊 Starting data preparation for {property_type} on {input_date}"
        )

        try:
            start_time = datetime.now()

            # Import and run unified data preparation
            from ml.pipelines.data_preparation import UnifiedDataPreparator

            preparator = UnifiedDataPreparator(spark_session=self.spark)

            # Run data preparation with 30-day sliding window
            result = preparator.prepare_training_data(
                date=input_date,
                property_type=property_type,
                lookback_days=30,
                target_column="price",
            )

            duration = (datetime.now() - start_time).total_seconds()

            if result["success"]:
                self.logger.logger.info(
                    f"✅ Data preparation completed in {duration:.1f}s"
                )
                self.logger.logger.info(f"📊 Records: {result['total_records']:,}")
                self.logger.logger.info(
                    f"🔧 Features: {result.get('features_count', 'Unknown')}"
                )

            result["duration"] = duration
            return result

        except Exception as e:
            error_msg = f"❌ Data preparation failed: {str(e)}"
            self.logger.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "duration": (
                    (datetime.now() - start_time).total_seconds()
                    if "start_time" in locals()
                    else 0
                ),
            }

    def run_training_only(
        self, input_date: str, property_type: str = "house"
    ) -> Dict[str, Any]:
        """
        🤖 Run only model training pipeline

        Args:
            input_date: Date to process
            property_type: Property type to process

        Returns:
            Dict containing training results
        """
        self.logger.logger.info(
            f"🤖 Starting model training for {property_type} on {input_date}"
        )

        try:
            start_time = datetime.now()

            # Import and run ML model trainer
            from ml.pipelines.model_training import MLModelTrainer

            trainer = MLModelTrainer(spark_session=self.spark)

            # Run training pipeline
            result = trainer.train_all_models(
                date=input_date, property_type=property_type
            )

            duration = (datetime.now() - start_time).total_seconds()

            if result["success"]:
                self.logger.logger.info(
                    f"✅ Model training completed in {duration:.1f}s"
                )
                self.logger.logger.info(f"🏆 Best model: {result['best_model']}")
                if result.get("best_model_metrics", {}):
                    r2_score = result["best_model_metrics"].get("r2", "N/A")
                    self.logger.logger.info(f"📊 R² Score: {r2_score}")

            result["duration"] = duration
            return result

        except Exception as e:
            error_msg = f"❌ Model training failed: {str(e)}"
            self.logger.logger.error(error_msg)
            return {
                "success": False,
                "error": str(e),
                "best_model": "training_failed",
                "models_trained": [],
                "best_model_metrics": {},
                "duration": (
                    (datetime.now() - start_time).total_seconds()
                    if "start_time" in locals()
                    else 0
                ),
            }

    def validate_prerequisites(self, input_date: str, property_type: str) -> bool:
        """
        ✅ Validate that all prerequisites are met for ML pipeline

        Args:
            input_date: Date to validate
            property_type: Property type to validate

        Returns:
            True if all prerequisites are met
        """
        self.logger.logger.info(
            f"✅ Validating prerequisites for {property_type} on {input_date}"
        )

        try:
            from common.utils.hdfs_utils import check_hdfs_path_exists

            # Check if gold data exists
            date_formatted = input_date.replace("-", "")
            gold_path = f"/data/realestate/processed/gold/unified/{property_type}/{input_date.replace('-', '/')}/unified_{property_type}_{date_formatted}.parquet"

            if not check_hdfs_path_exists(self.spark, gold_path):
                self.logger.logger.error(f"❌ Gold data not found: {gold_path}")
                return False

            self.logger.logger.info(f"✅ Gold data validated: {gold_path}")

            # Check for sufficient historical data (at least 5 days)
            valid_days = 0
            for i in range(30):  # Check last 30 days
                check_date = (
                    datetime.strptime(input_date, "%Y-%m-%d") - timedelta(days=i)
                ).strftime("%Y-%m-%d")
                check_date_formatted = check_date.replace("-", "")
                check_path = f"/data/realestate/processed/gold/unified/{property_type}/{check_date.replace('-', '/')}/unified_{property_type}_{check_date_formatted}.parquet"

                if check_hdfs_path_exists(self.spark, check_path):
                    valid_days += 1

                if valid_days >= 5:  # Need at least 5 days of data
                    break

            if valid_days < 5:
                self.logger.logger.error(
                    f"❌ Insufficient historical data: only {valid_days} days found"
                )
                return False

            self.logger.logger.info(
                f"✅ Historical data validated: {valid_days} days available"
            )
            return True

        except Exception as e:
            self.logger.logger.error(f"❌ Prerequisites validation failed: {str(e)}")
            return False

    def cleanup(self):
        """🧹 Cleanup resources"""
        try:
            if self.spark:
                self.spark.catalog.clearCache()
                self.logger.logger.info("🧹 Spark cache cleared")
        except Exception as e:
            self.logger.logger.warning(f"⚠️ Cleanup warning: {str(e)}")
