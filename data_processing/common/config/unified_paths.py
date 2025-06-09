"""
🎯 Unified Path Configuration for Real Estate Project
=====================================================

Centralized path management for all components:
- ETL pipelines (Bronze → Silver → Gold)
- ML pipelines (Feature Store, Models)
- Analytics and serving layers
- Storage and backup paths

Author: Data Engineering Team
Date: June 2025
Version: 1.0.0
"""

import os
from typing import Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass


@dataclass
class PathConfig:
    """📁 Centralized path configuration for all data layers"""

    # ===== BASE PATHS =====
    hdfs_base: str = "/data/realestate"

    # ===== RAW DATA LAYER =====
    raw_base: str = "/data/realestate/raw"

    # ===== MEDALLION ARCHITECTURE LAYERS =====
    bronze_base: str = "/data/realestate/processed/bronze"  # Raw → Parquet
    silver_base: str = "/data/realestate/processed/silver"  # Cleaned & Standardized
    gold_base: str = "/data/realestate/processed/gold"  # Unified & Analytics-ready

    # ===== ML & ANALYTICS LAYERS =====
    feature_store_base: str = "/data/realestate/ml/features"
    model_registry_base: str = "/data/realestate/ml/models"
    analytics_base: str = "/data/realestate/analytics"

    # ===== SERVING LAYERS =====
    serving_base: str = "/data/realestate/serving"
    api_cache_base: str = "/data/realestate/serving/api_cache"

    # ===== BACKUP & ARCHIVE =====
    backup_base: str = "/data/realestate/backup"
    archive_base: str = "/data/realestate/archive"

    # ===== TEMPORARY & PROCESSING =====
    temp_base: str = "/data/realestate/temp"
    checkpoint_base: str = "/data/realestate/checkpoints"


class UnifiedPathManager:
    """🗂️ Unified path manager for consistent path generation across all components"""

    def __init__(self, config: PathConfig = None):
        self.config = config or PathConfig()

    # ===== DATE FORMATTING =====

    @staticmethod
    def format_date_for_partition(date: str = None, days_ago: int = 0) -> str:
        """Standard date formatting for HDFS partitions: yyyy/mm/dd"""
        if date:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        else:
            date_obj = datetime.now() - timedelta(days=days_ago)
        return date_obj.strftime("%Y/%m/%d")

    @staticmethod
    def format_date_for_filename(date: str = None, days_ago: int = 0) -> str:
        """Standard date formatting for filenames: yyyymmdd"""
        if date:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        else:
            date_obj = datetime.now() - timedelta(days=days_ago)
        return date_obj.strftime("%Y%m%d")

    @staticmethod
    def format_date_hyphen(date: str = None, days_ago: int = 0) -> str:
        """Standard date formatting with hyphens: yyyy-mm-dd"""
        if date:
            return date
        else:
            date_obj = datetime.now() - timedelta(days=days_ago)
            return date_obj.strftime("%Y-%m-%d")

    # ===== RAW DATA PATHS =====

    def get_raw_path(
        self, source: str, property_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get raw data path: /data/realestate/raw/{source}/{property_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(self.config.raw_base, source, property_type, date_partition)

    # ===== MEDALLION ARCHITECTURE PATHS =====

    def get_bronze_path(
        self, source: str, property_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get bronze layer path: /data/realestate/processed/bronze/{source}/{property_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(
            self.config.bronze_base, source, property_type, date_partition
        )

    def get_silver_path(
        self, source: str, property_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get silver layer path: /data/realestate/processed/silver/{source}/{property_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(
            self.config.silver_base, source, property_type, date_partition
        )

    def get_gold_path(
        self,
        property_type: str,
        date: str = None,
        days_ago: int = 0,
        unified: bool = True,
    ) -> str:
        """Get gold layer path: /data/realestate/processed/gold/unified/{property_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        if unified:
            return os.path.join(
                self.config.gold_base, "unified", property_type, date_partition
            )
        else:
            return os.path.join(self.config.gold_base, property_type, date_partition)

    # ===== SPECIFIC GOLD FILE PATHS =====

    def get_unified_file_path(
        self, property_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get unified parquet file path in Gold layer"""
        date_formatted = self.format_date_for_filename(date, days_ago)
        gold_dir = self.get_gold_path(property_type, date, days_ago)
        return os.path.join(
            gold_dir, f"unified_{property_type}_{date_formatted}.parquet"
        )

    # ===== ML & FEATURE STORE PATHS =====

    def get_feature_store_path(
        self, feature_set: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get feature store path: /data/realestate/ml/features/{feature_set}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(self.config.feature_store_base, feature_set, date_partition)

    def get_model_path(self, model_name: str, version: str) -> str:
        """Get model registry path: /data/realestate/ml/models/{model_name}/{version}/"""
        return os.path.join(self.config.model_registry_base, model_name, version)

    def get_ml_features_path(
        self, property_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get ML features path: /data/realestate/ml/features/{property_type}/{yyyy-mm-dd}/"""
        date_hyphen = self.format_date_hyphen(date, days_ago)
        return os.path.join(self.config.feature_store_base, property_type, date_hyphen)

    # ===== ANALYTICS PATHS =====

    def get_analytics_path(
        self,
        analysis_type: str,
        property_type: str,
        date: str = None,
        days_ago: int = 0,
    ) -> str:
        """Get analytics path: /data/realestate/analytics/{analysis_type}/{property_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(
            self.config.analytics_base, analysis_type, property_type, date_partition
        )

    # ===== SERVING LAYER PATHS =====

    def get_serving_path(
        self, service_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get serving layer path: /data/realestate/serving/{service_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(self.config.serving_base, service_type, date_partition)

    def get_api_cache_path(self, cache_key: str) -> str:
        """Get API cache path: /data/realestate/serving/api_cache/{cache_key}/"""
        return os.path.join(self.config.api_cache_base, cache_key)

    # ===== UTILITY PATHS =====

    def get_temp_path(self, job_name: str, date: str = None, days_ago: int = 0) -> str:
        """Get temporary processing path: /data/realestate/temp/{job_name}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(self.config.temp_base, job_name, date_partition)

    def get_checkpoint_path(self, job_name: str) -> str:
        """Get checkpoint path: /data/realestate/checkpoints/{job_name}/"""
        return os.path.join(self.config.checkpoint_base, job_name)

    def get_backup_path(
        self, backup_type: str, date: str = None, days_ago: int = 0
    ) -> str:
        """Get backup path: /data/realestate/backup/{backup_type}/{yyyy/mm/dd}/"""
        date_partition = self.format_date_for_partition(date, days_ago)
        return os.path.join(self.config.backup_base, backup_type, date_partition)

    # ===== PATH VALIDATION =====

    def validate_path_structure(self, path: str) -> bool:
        """Validate if path follows the standardized structure"""
        return path.startswith(self.config.hdfs_base)

    def get_all_paths_for_date(
        self, date: str = None, property_type: str = "house"
    ) -> Dict[str, str]:
        """Get all standardized paths for a specific date - useful for debugging/monitoring"""
        return {
            "raw_batdongsan": self.get_raw_path("batdongsan", property_type, date),
            "raw_chotot": self.get_raw_path("chotot", property_type, date),
            "bronze_batdongsan": self.get_bronze_path(
                "batdongsan", property_type, date
            ),
            "bronze_chotot": self.get_bronze_path("chotot", property_type, date),
            "silver_batdongsan": self.get_silver_path(
                "batdongsan", property_type, date
            ),
            "silver_chotot": self.get_silver_path("chotot", property_type, date),
            "gold_unified": self.get_gold_path(property_type, date),
            "unified_file": self.get_unified_file_path(property_type, date),
            "ml_features": self.get_ml_features_path(property_type, date),
            "analytics": self.get_analytics_path("market_trends", property_type, date),
            "serving": self.get_serving_path("api_data", date),
        }


# ===== GLOBAL INSTANCE =====
# Single instance to be used across all components
path_manager = UnifiedPathManager()

# ===== BACKWARD COMPATIBILITY FUNCTIONS =====
# These functions maintain compatibility with existing code


def get_hdfs_path(
    base_path: str,
    data_source: str,
    property_type: str,
    date: str = None,
    days_ago: int = 0,
    partition_format: str = "%Y/%m/%d",
) -> str:
    """
    Backward compatibility function for existing get_hdfs_path calls

    Args:
        base_path: Base HDFS path
        data_source: Source name (batdongsan, chotot, etc.)
        property_type: Property type (house, other, etc.)
        date: Date string in YYYY-MM-DD format
        days_ago: Days ago from current date
        partition_format: Date format for partitions

    Returns:
        str: Standardized HDFS path
    """
    # Map base_path to appropriate method
    if "raw" in base_path:
        return path_manager.get_raw_path(data_source, property_type, date, days_ago)
    elif "bronze" in base_path:
        return path_manager.get_bronze_path(data_source, property_type, date, days_ago)
    elif "silver" in base_path:
        return path_manager.get_silver_path(data_source, property_type, date, days_ago)
    elif "gold" in base_path:
        return path_manager.get_gold_path(property_type, date, days_ago)
    else:
        # Fallback to original logic for unknown paths
        if date:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        else:
            date_obj = datetime.now() - timedelta(days=days_ago)
        partition = date_obj.strftime(partition_format)
        return os.path.join(base_path, data_source, property_type, partition)


# ===== CONFIGURATION CONSTANTS =====
# For easy import in other modules

# Base paths
HDFS_BASE = path_manager.config.hdfs_base
RAW_BASE = path_manager.config.raw_base
BRONZE_BASE = path_manager.config.bronze_base
SILVER_BASE = path_manager.config.silver_base
GOLD_BASE = path_manager.config.gold_base

# ML paths
FEATURE_STORE_BASE = path_manager.config.feature_store_base
MODEL_REGISTRY_BASE = path_manager.config.model_registry_base

# Analytics & Serving
ANALYTICS_BASE = path_manager.config.analytics_base
SERVING_BASE = path_manager.config.serving_base

# Utilities
TEMP_BASE = path_manager.config.temp_base
CHECKPOINT_BASE = path_manager.config.checkpoint_base

# Environment-based overrides
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "airflow")


if __name__ == "__main__":
    # ===== DEMO & TESTING =====
    print("🗂️ Unified Path Manager Demo")
    print("=" * 50)

    # Example usage
    demo_date = "2025-06-06"
    demo_property_type = "house"

    print(f"📅 Demo date: {demo_date}")
    print(f"🏠 Property type: {demo_property_type}")
    print()

    # Get all paths for the demo date
    all_paths = path_manager.get_all_paths_for_date(demo_date, demo_property_type)

    for path_name, path_value in all_paths.items():
        print(f"{path_name:20}: {path_value}")

    print()
    print("✅ Path standardization complete!")
    print("   All components should use UnifiedPathManager for consistent paths.")
