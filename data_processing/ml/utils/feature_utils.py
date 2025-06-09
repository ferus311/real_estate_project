"""
🔧 Simplified ML Feature Engineering Utilities
==============================================

Practical and efficient feature engineering for real estate ML pipelines.
Focuses on creating useful features from available columns only.

Author: ML Team
Date: June 2025
"""

import logging
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    sqrt,
    pow,
    abs as spark_abs,
    log,
    avg,
    count,
    stddev,
    sin,
    cos,
    radians,
    atan2,
    isnan,
    isnull,
    max as spark_max,
    min as spark_min,
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """🔧 Simplified feature engineering toolkit for real estate data"""

    def __init__(self, spark: SparkSession = None):
        self.spark = spark
        # Ho Chi Minh City center coordinates (approximately)
        self.city_center_lat = 10.7769
        self.city_center_lng = 106.7009

    def add_basic_features(self, df: DataFrame) -> DataFrame:
        """
        Add basic property features that are practical and useful

        Args:
            df: Input DataFrame with real estate data

        Returns:
            DataFrame with basic features added
        """
        logger.info("🏗️ Adding basic property features...")

        try:
            # 1. Room ratios (if bedroom/bathroom columns exist)
            if "bedroom" in df.columns and "bathroom" in df.columns:
                df = df.withColumn(
                    "bedroom_bathroom_ratio",
                    when(
                        col("bathroom") > 0, col("bedroom") / col("bathroom")
                    ).otherwise(col("bedroom")),
                )

                # Total rooms
                df = df.withColumn(
                    "total_rooms",
                    coalesce(col("bedroom"), lit(0))
                    + coalesce(col("bathroom"), lit(0)),
                )

            # 2. Area per room (if area column exists)
            if "area" in df.columns:
                if "total_rooms" in df.columns:
                    df = df.withColumn(
                        "area_per_room",
                        when(
                            col("total_rooms") > 0, col("area") / col("total_rooms")
                        ).otherwise(col("area")),
                    )

                # Area categories
                df = df.withColumn(
                    "area_category",
                    when(col("area") <= 50, "small")
                    .when(col("area") <= 100, "medium")
                    .when(col("area") <= 200, "large")
                    .otherwise("very_large"),
                )

            # 3. Price efficiency (if price_per_m2 exists)
            if "price_per_m2" in df.columns:
                # Calculate price per square meter statistics for normalization
                price_stats = df.select(
                    avg("price_per_m2").alias("avg_price_per_m2"),
                    stddev("price_per_m2").alias("std_price_per_m2"),
                ).collect()[0]

                avg_price = price_stats["avg_price_per_m2"]
                std_price = price_stats["std_price_per_m2"]

                # Price relative to market average
                df = df.withColumn(
                    "price_vs_market",
                    (
                        (col("price_per_m2") - lit(avg_price)) / lit(std_price)
                        if std_price and std_price > 0
                        else lit(0)
                    ),
                )

                # Price categories
                df = df.withColumn(
                    "price_category",
                    when(col("price_per_m2") < lit(avg_price * 0.8), "budget")
                    .when(col("price_per_m2") < lit(avg_price * 1.2), "average")
                    .when(col("price_per_m2") < lit(avg_price * 1.5), "premium")
                    .otherwise("luxury"),
                )

            logger.info("✅ Basic features added successfully")
            return df

        except Exception as e:
            logger.error(f"❌ Error adding basic features: {str(e)}")
            return df

    def add_location_features(self, df: DataFrame) -> DataFrame:
        """
        Add location-based features using latitude and longitude

        Args:
            df: Input DataFrame with latitude/longitude columns

        Returns:
            DataFrame with location features added
        """
        logger.info("🗺️ Adding location features...")

        try:
            # Check if location columns exist
            if "latitude" not in df.columns or "longitude" not in df.columns:
                logger.warning(
                    "⚠️ Latitude/longitude columns not found, skipping location features"
                )
                return df

            # 1. Distance to city center (Haversine formula)
            df = df.withColumn(
                "distance_to_center",
                self._calculate_haversine_distance(
                    col("latitude"),
                    col("longitude"),
                    lit(self.city_center_lat),
                    lit(self.city_center_lng),
                ),
            )

            # 2. Location zones based on distance
            df = df.withColumn(
                "location_zone",
                when(col("distance_to_center") <= 5, "center")
                .when(col("distance_to_center") <= 15, "inner")
                .when(col("distance_to_center") <= 30, "outer")
                .otherwise("far"),
            )

            # 3. Coordinate-based features
            df = df.withColumn("lat_lng_ratio", col("latitude") / col("longitude"))

            logger.info("✅ Location features added successfully")
            return df

        except Exception as e:
            logger.error(f"❌ Error adding location features: {str(e)}")
            return df

    def add_market_features(self, df: DataFrame) -> DataFrame:
        """
        Add market-based features using district and ward information

        Args:
            df: Input DataFrame with district/ward columns

        Returns:
            DataFrame with market features added
        """
        logger.info("📊 Adding market features...")

        try:
            # 1. District-level statistics
            if "district" in df.columns:
                district_window = Window.partitionBy("district")

                df = df.withColumn(
                    "district_property_count", count("*").over(district_window)
                )

                if "price_per_m2" in df.columns:
                    df = df.withColumn(
                        "district_avg_price", avg("price_per_m2").over(district_window)
                    )
                    df = df.withColumn(
                        "price_vs_district",
                        col("price_per_m2") / col("district_avg_price"),
                    )

                if "area" in df.columns:
                    df = df.withColumn(
                        "district_avg_area", avg("area").over(district_window)
                    )

            # 2. Ward-level statistics (if ward column exists)
            if "ward" in df.columns:
                ward_window = Window.partitionBy("ward")

                df = df.withColumn("ward_property_count", count("*").over(ward_window))

                if "price_per_m2" in df.columns:
                    df = df.withColumn(
                        "ward_avg_price", avg("price_per_m2").over(ward_window)
                    )

            # 3. Property type distribution (if property_type exists)
            if "property_type" in df.columns and "district" in df.columns:
                type_district_window = Window.partitionBy("district", "property_type")
                df = df.withColumn(
                    "type_district_count", count("*").over(type_district_window)
                )

            logger.info("✅ Market features added successfully")
            return df

        except Exception as e:
            logger.error(f"❌ Error adding market features: {str(e)}")
            return df

    def add_interaction_features(self, df: DataFrame) -> DataFrame:
        """
        Add simple interaction features between key variables

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with interaction features added
        """
        logger.info("🔗 Adding interaction features...")

        try:
            # 1. Area and location interactions
            if all(col in df.columns for col in ["area", "distance_to_center"]):
                df = df.withColumn(
                    "area_distance_interaction", col("area") * col("distance_to_center")
                )

            # 2. Room and area interactions
            if all(col in df.columns for col in ["total_rooms", "area"]):
                df = df.withColumn(
                    "room_area_interaction", col("total_rooms") * col("area")
                )

            # 3. Price and location interactions
            if all(col in df.columns for col in ["price_per_m2", "distance_to_center"]):
                df = df.withColumn(
                    "price_distance_interaction",
                    col("price_per_m2")
                    / (
                        col("distance_to_center") + lit(1)
                    ),  # Add 1 to avoid division by zero
                )

            logger.info("✅ Interaction features added successfully")
            return df

        except Exception as e:
            logger.error(f"❌ Error adding interaction features: {str(e)}")
            return df

    def create_all_features(self, df: DataFrame) -> DataFrame:
        """
        Create all features in a single pipeline

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with all features added
        """
        logger.info("🚀 Starting comprehensive feature engineering...")

        # Apply all feature engineering steps
        df = self.add_basic_features(df)
        df = self.add_location_features(df)
        df = self.add_market_features(df)
        df = self.add_interaction_features(df)

        logger.info("✅ All features created successfully")
        return df

    def _calculate_haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate distance between two points using Haversine formula

        Returns:
            Distance in kilometers
        """
        # Convert latitude and longitude from degrees to radians
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)

        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2

        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        # Earth radius in kilometers
        earth_radius = 6371.0

        return earth_radius * c

    def get_feature_columns(self, df: DataFrame) -> List[str]:
        """
        Get list of engineered feature columns

        Args:
            df: DataFrame with features

        Returns:
            List of feature column names
        """
        # Original columns that should not be considered as features
        original_cols = [
            "property_id",
            "title",
            "description",
            "address",
            "url",
            "price_per_m2",  # This is typically the target variable
            "latitude",
            "longitude",
            "district",
            "ward",
            "property_type",
            "area",
            "bedroom",
            "bathroom",
        ]

        # Get all columns except original ones
        feature_cols = [col for col in df.columns if col not in original_cols]

        logger.info(f"📝 Found {len(feature_cols)} engineered features: {feature_cols}")
        return feature_cols

    def validate_features(self, df: DataFrame) -> Dict[str, any]:
        """
        Validate the quality of engineered features

        Args:
            df: DataFrame with features

        Returns:
            Dictionary with validation results
        """
        logger.info("🔍 Validating feature quality...")

        validation_results = {
            "total_rows": df.count(),
            "total_columns": len(df.columns),
            "feature_columns": self.get_feature_columns(df),
            "null_counts": {},
            "feature_stats": {},
        }

        try:
            # Check for null values in each column
            for col_name in df.columns:
                null_count = df.filter(
                    col(col_name).isNull() | isnan(col(col_name))
                ).count()
                validation_results["null_counts"][col_name] = null_count

            # Basic statistics for numeric features
            numeric_cols = [
                col_name
                for col_name, data_type in df.dtypes
                if data_type in ["int", "bigint", "float", "double"]
            ]

            for col_name in numeric_cols:
                if col_name in validation_results["feature_columns"]:
                    stats = df.select(
                        avg(col_name).alias("mean"),
                        stddev(col_name).alias("std"),
                        spark_min(col_name).alias("min"),
                        spark_max(col_name).alias("max"),
                    ).collect()[0]

                    validation_results["feature_stats"][col_name] = {
                        "mean": stats["mean"],
                        "std": stats["std"],
                        "min": stats["min"],
                        "max": stats["max"],
                    }

            logger.info("✅ Feature validation completed")

        except Exception as e:
            logger.error(f"❌ Error during validation: {str(e)}")
            validation_results["error"] = str(e)

        return validation_results


# Utility functions for standalone use
def create_features(df: DataFrame, spark: SparkSession = None) -> DataFrame:
    """
    Convenience function to create all features

    Args:
        df: Input DataFrame
        spark: SparkSession (optional)

    Returns:
        DataFrame with all features
    """
    engineer = FeatureEngineer(spark)
    return engineer.create_all_features(df)


def get_feature_list(df: DataFrame) -> List[str]:
    """
    Convenience function to get feature column names

    Args:
        df: DataFrame with features

    Returns:
        List of feature column names
    """
    engineer = FeatureEngineer()
    return engineer.get_feature_columns(df)


if __name__ == "__main__":
    # Example usage
    print("🔧 Simplified Feature Engineering Utilities")
    print(
        "This module provides practical feature engineering for real estate ML pipelines."
    )
    print(
        "Use FeatureEngineer class or convenience functions create_features() and get_feature_list()."
    )
