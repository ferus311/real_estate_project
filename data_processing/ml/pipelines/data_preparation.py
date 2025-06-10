"""
🔄 ML Data Preparation Orchestrator
===================================

This module orchestrates the data preparation pipeline by coordinating
calls to specialized utility modules for cleaning and feature engineering.

Architecture:
- 🧹 DataCleaner: Handles data cleaning, validation, missing values, duplicates
- 🔧 FeatureEngineer: Handles feature engineering and transformations
- 📊 This module: Orchestrates the pipeline and manages data flow

Author: ML Team
Date: June 2025
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

# Import common utilities
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.config.spark_config import create_optimized_ml_spark_session

# Import ML utilities
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils import DataCleaner, FeatureEngineer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLDataPreprocessor:
    """
    🔄 ML Data Preparation Orchestrator

    Coordinates data preparation by calling specialized utilities:
    - DataCleaner for cleaning operations
    - FeatureEngineer for feature transformations
    """

    def __init__(self, spark: SparkSession = None):
        """Initialize the data preparation orchestrator."""
        self.spark = spark or create_optimized_ml_spark_session("MLDataPreparation")
        self.feature_store_path = "/data/realestate/processed/ml/feature_store"
        self.feature_metadata = {}

        # Initialize utility modules
        self.data_cleaner = DataCleaner(self.spark)
        self.feature_engineer = FeatureEngineer(self.spark)

        # Pipeline configuration
        self.config = {
            "outlier_method": "iqr",  # 'iqr', 'zscore', 'isolation', 'none'
            "outlier_threshold": 3.0,
            "iqr_multiplier": 1.5,  # Standard IQR multiplier (can be adjusted: 1.5=strict, 2.0=moderate, 3.0=loose)
            "missing_threshold": 0.3,  # Drop features with >30% missing
            "correlation_threshold": 0.95,  # Drop highly correlated features
            "variance_threshold": 0.01,  # Drop low variance features
            "feature_selection_k": 50,  # Top K features to select
            "lookback_days": 30,  # Days to include for training data
            # Price validation ranges (based on actual data analysis - in VND)
            "price_min": 500000000,  # 500M VND - minimum reasonable price
            "price_max": 100000000000,  # 100B VND - maximum reasonable price
            # Area validation ranges (sqm)
            "area_min": 10,  # 10 sqm - small units
            "area_max": 1000,  # 1000 sqm - very large properties
        }

        logger.info("🔄 ML Data Preparation Orchestrator initialized")

    def run_full_pipeline(self, date: str, property_type: str = "house") -> DataFrame:
        """
        🚀 Run the complete data preparation pipeline

        Args:
            date: Target date for training (YYYY-MM-DD)
            property_type: Type of property to process

        Returns:
            DataFrame: Fully prepared features ready for ML training
        """
        logger.info(
            f"🚀 Starting data preparation orchestration for {date} ({property_type})"
        )

        try:
            # Step 1: Read raw data
            raw_df = self._read_gold_data(date, property_type)
            logger.info(f"📊 Read {raw_df.count():,} raw records")

            # Step 2: Data cleaning pipeline (delegated to DataCleaner)
            clean_df = self.data_cleaner.full_cleaning_pipeline(raw_df, self.config)
            logger.info(f"🧹 After cleaning: {clean_df.count():,} records")

            # Step 3: Feature engineering pipeline (delegated to FeatureEngineer)
            features_df = self.feature_engineer.create_all_features(clean_df)
            logger.info(f"🔧 After feature engineering: {features_df.count():,} records")

            # Step 4: Final validation and feature selection
            final_df = self._select_and_validate_features(features_df)
            logger.info(
                f"✅ Final dataset: {final_df.count():,} records with {len(final_df.columns)} features"
            )

            # Step 5: Save to feature store
            self._save_to_feature_store(final_df, date, property_type)

            return final_df

        except Exception as e:
            logger.error(f"❌ Pipeline orchestration failed: {str(e)}")
            raise

    def _get_training_columns(self) -> list:
        """🎯 Get list of required columns for ML training"""
        core_features = [
            # Target variable
            "price",
            # Core numeric features
            "area",
            "latitude",
            "longitude",
            "price_per_m2",
            # Property characteristics
            "bedroom",
            "bathroom",
            "floor_count",
            "width",
            "length",
            "living_size",
            "facade_width",
            "road_width",
            # Categorical features (keep limited set)
            "district",
            "ward",
            "house_direction",
            "legal_status",
            "interior",
            "house_type",
            "property_type",
            # Quality features
            "data_quality_score",
            # ID for tracking
            "id",
        ]
        return core_features

    def _read_gold_data(self, date: str, property_type: str) -> DataFrame:
        """📖 Read gold layer data with sliding window approach (optimized with early column selection)"""
        logger.info(
            f"📖 Reading gold data with {self.config['lookback_days']}-day window"
        )

        # Import HDFS utility to check file existence
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from common.utils.hdfs_utils import check_hdfs_path_exists

        # Get required columns for training
        required_columns = self._get_training_columns()

        # Calculate date range
        end_date = datetime.strptime(date, "%Y-%m-%d")
        start_date = end_date - timedelta(days=self.config["lookback_days"] - 1)

        # Read data from multiple days (only process dates with actual data)
        all_dfs = []
        current_date = start_date
        missing_dates = []

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            date_formatted = date_str.replace("-", "")
            gold_path = f"/data/realestate/processed/gold/unified/{property_type}/{date_str.replace('-', '/')}/unified_{property_type}_{date_formatted}.parquet"

            # Check if file exists before trying to read it
            if check_hdfs_path_exists(self.spark, gold_path):
                try:
                    # First read just to check available columns
                    temp_df = self.spark.read.parquet(gold_path)
                    available_columns = [
                        col for col in required_columns if col in temp_df.columns
                    ]

                    # Re-read with only required columns (OPTIMIZATION: Save I/O and memory)
                    daily_df = self.spark.read.parquet(gold_path).select(
                        *available_columns
                    )

                    if daily_df.count() > 0:
                        daily_df = daily_df.withColumn("data_date", lit(date_str))
                        all_dfs.append(daily_df)
                        logger.info(
                            f"📅 {date_str}: {daily_df.count():,} records ({len(available_columns)} cols)"
                        )
                    else:
                        missing_dates.append(date_str)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to read {date_str}: {str(e)}")
                    missing_dates.append(date_str)
            else:
                missing_dates.append(date_str)

            current_date += timedelta(days=1)

        # Log missing dates summary only once (reduced noise)
        if missing_dates:
            logger.info(
                f"📊 Found data for {len(all_dfs)} out of {self.config['lookback_days']} days"
            )
            if len(missing_dates) <= 5:
                logger.info(f"⚠️ Missing dates: {', '.join(missing_dates)}")
            else:
                logger.info(
                    f"⚠️ Missing {len(missing_dates)} dates (first 3: {', '.join(missing_dates[:3])}...)"
                )

        if not all_dfs:
            raise ValueError(
                f"❌ No data found for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )

        # Union all DataFrames
        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.union(df)

        logger.info(
            f"📊 Combined {combined_df.count():,} total records from {len(all_dfs)} days"
        )
        logger.info(
            f"🎯 Early column selection optimization: Only {len(required_columns)} columns read from storage"
        )
        return combined_df

    def _select_and_validate_features(self, df: DataFrame) -> DataFrame:
        """✅ Final feature selection and validation"""
        logger.info("✅ Final feature selection and validation")

        # Remove highly correlated features
        df = self._remove_highly_correlated_features(df)

        # Remove low variance features
        df = self._remove_low_variance_features(df)

        # Feature selection based on importance
        df = self._select_top_features(df)

        return df

    def _remove_highly_correlated_features(self, df: DataFrame) -> DataFrame:
        """Remove highly correlated features"""
        logger.info("🔄 Removing highly correlated features")
        # This is a placeholder - implement correlation analysis if needed
        return df

    def _remove_low_variance_features(self, df: DataFrame) -> DataFrame:
        """Remove low variance features"""
        logger.info("🔄 Removing low variance features")
        # This is a placeholder - implement variance analysis if needed
        return df

    def _select_top_features(self, df: DataFrame) -> DataFrame:
        """Select top K features based on importance"""
        logger.info("🔄 Selecting top features")
        # This is a placeholder - implement feature selection if needed
        return df

    def _save_to_feature_store(self, df: DataFrame, date: str, property_type: str) -> None:
        """💾 Save processed features to feature store"""
        logger.info("💾 Saving to feature store")

        try:
            # Create feature store path
            date_formatted = date.replace("-", "")
            output_path = f"{self.feature_store_path}/{property_type}/{date.replace('-', '/')}/features_{property_type}_{date_formatted}.parquet"

            # Save features
            df.coalesce(10).write.mode("overwrite").parquet(output_path)

            # Save metadata
            metadata = {
                "date": date,
                "property_type": property_type,
                "record_count": df.count(),
                "feature_count": len(df.columns),
                "features": df.columns,
                "created_at": datetime.now().isoformat(),
                "config": self.config
            }

            metadata_path = f"{self.feature_store_path}/{property_type}/{date.replace('-', '/')}/metadata_{property_type}_{date_formatted}.json"

            # Convert metadata to JSON and save
            metadata_df = self.spark.createDataFrame([metadata])
            metadata_df.coalesce(1).write.mode("overwrite").json(metadata_path)

            logger.info(f"✅ Features saved to: {output_path}")
            logger.info(f"✅ Metadata saved to: {metadata_path}")

        except Exception as e:
            logger.error(f"❌ Failed to save to feature store: {str(e)}")
            raise

    def configure_pipeline(self, **kwargs) -> None:
        """🔧 Configure pipeline parameters"""
        for key, value in kwargs.items():
            if key in self.config:
                self.config[key] = value
                logger.info(f"🔧 Updated config: {key} = {value}")
            else:
                logger.warning(f"⚠️ Unknown config parameter: {key}")

    def get_config_summary(self) -> Dict[str, Any]:
        """📊 Get current pipeline configuration"""
        return self.config.copy()

    def __del__(self):
        """Cleanup"""
        if hasattr(self, "spark") and self.spark:
            self.spark.stop()


def main():
    """🚀 Main execution function for testing"""
    pipeline = MLDataPreprocessor()

    try:
        # Test with sample date
        test_date = "2025-05-24"
        result_df = pipeline.run_full_pipeline(test_date, "house")

        print(f"✅ Pipeline completed successfully!")
        print(
            f"📊 Final dataset shape: {result_df.count()} rows, {len(result_df.columns)} columns"
        )

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise
    finally:
        pipeline.spark.stop()


if __name__ == "__main__":
    main()
