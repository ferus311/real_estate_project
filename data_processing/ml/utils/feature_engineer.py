"""
🔧 ML Feature Engineering for Final 16 Features
===============================================

RATIONAL APPROACH - Only meaningful features that work across multi-province data.

No HCM-centric bias, no location-based features that don't make sense
across different provinces.

Final 16 Features:
📊 CORE FEATURES (12 + target + id):
- Target: price
- Core (4): area, latitude, longitude, id
- Rooms (3): bedroom, bathroom, floor_count
- House (3): house_direction_code, legal_status_code, interior_code
- Location (3): province_id, district_id, ward_id

🏠 PROPERTY FEATURES (3):
- total_rooms: bedroom + bathroom (universal)
- area_per_room: area / total_rooms (density)
- bedroom_bathroom_ratio: bedroom / bathroom (ratio)

🌍 POPULATION FEATURES (1):
- population_density: từ CSV via province_id (không bias địa phương)

Total: 16 features (12 core + 3 property + 1 population)

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
    isnan,
    isnull,
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """🔧 RATIONAL feature engineering - only meaningful features across provinces"""

    def __init__(self, spark: SparkSession = None):
        self.spark = spark

        # Define our final 12 features + price target
        self.final_features = [
            # TARGET
            "price",
            # CORE FEATURES (4)
            "area",
            "latitude",
            "longitude",
            # ROOM CHARACTERISTICS (3)
            "bedroom",
            "bathroom",
            "floor_count",
            # HOUSE CHARACTERISTICS - ENCODED (3)
            "house_direction_code",
            "legal_status_code",
            "interior_code",
            # ADMINISTRATIVE (3)
            "province_id",
            "district_id",
            "ward_id",
            # METADATA
            "id",
        ]

    def create_all_features(self, df: DataFrame) -> DataFrame:
        """
        🎯 Create features - RATIONAL APPROACH

        Final 16 features:
        - 12 core features (từ data gốc)
        - 3 property features (total_rooms, area_per_room, bedroom_bathroom_ratio)
        - 1 population feature (population_density từ CSV)

        Only adds features that:
        1. Make sense across ALL provinces (not HCM-centric)
        2. Have clear business meaning
        3. Don't risk data leakage

        Args:
            df: Input DataFrame with raw data

        Returns:
            DataFrame with exactly 16 features (12 core + 3 property + 1 population)
        """
        logger.info("🎯 Creating ML features - Final 16 features (12+3+1)")

        # Step 1: Select and validate required columns (12 core features)
        df = self._select_required_columns(df)

        # Step 2: Add 3 universal property features
        df = self._add_property_features(df)

        # Step 3: Add 1 population density feature from CSV
        df = self._add_population_features(df)

        logger.info(f"✅ ML features created: {df.columns} columns")
        logger.info(
            "📊 Final feature count should be 16 (12 core + 3 property + 1 population)"
        )
        return df

    def _select_required_columns(self, df: DataFrame) -> DataFrame:
        """Select only the required columns for ML training"""
        logger.info("📋 Selecting required columns for 12-feature model")

        available_cols = df.columns
        required_cols = []
        missing_cols = []

        for col_name in self.final_features:
            if col_name in available_cols:
                required_cols.append(col_name)
            else:
                missing_cols.append(col_name)

        if missing_cols:
            logger.warning(f"⚠️ Missing required columns: {missing_cols}")
            logger.error(f"🚨 CRITICAL: Required columns missing: {missing_cols}")
            logger.error(
                "💡 Feature engineering expects ALL columns to be already cleaned and filled by data_cleaner!"
            )
            logger.error(
                "❌ Feature engineering does NOT fill missing values - that's cleaning job!"
            )

            # DO NOT ADD PLACEHOLDER COLUMNS HERE
            # This is feature engineering step - data should already be cleaned
            # If columns are missing, it's a pipeline configuration error
            raise ValueError(
                f"Missing required columns for feature engineering: {missing_cols}. "
                f"Ensure data_cleaner has already processed and filled all required columns."
            )

        # Select only required columns
        df = df.select(*required_cols)
        logger.info(f"✅ Selected {len(required_cols)} required columns")
        return df

    def _add_property_features(self, df: DataFrame) -> DataFrame:
        """
        Add 3 UNIVERSAL property features that make sense across ALL provinces

        Features added:
        1. total_rooms: bedroom + bathroom (universal concept)
        2. area_per_room: area / total_rooms (universal density measure)
        3. bedroom_bathroom_ratio: bedroom/bathroom ratio (property characteristic)

        ❌ NO distance_to_center (what center? HCM? Hanoi?)
        ❌ NO location_zones (meaningless across provinces)
        ❌ NO district_avg_price (potential data leakage)
        """
        logger.info("🏠 Adding 3 universal property features")

        try:
            # 1. Total rooms - UNIVERSAL CONCEPT
            if all(col_name in df.columns for col_name in ["bedroom", "bathroom"]):
                df = df.withColumn(
                    "total_rooms",
                    coalesce(col("bedroom"), lit(0))
                    + coalesce(col("bathroom"), lit(0)),
                )
                logger.info("✅ Added total_rooms (bedroom + bathroom)")

            # 2. Area per room - UNIVERSAL DENSITY MEASURE
            if "area" in df.columns and "total_rooms" in df.columns:
                df = df.withColumn(
                    "area_per_room",
                    when(
                        col("total_rooms") > 0, col("area") / col("total_rooms")
                    ).otherwise(
                        col("area")
                    ),  # If no rooms, area = total space
                )
                logger.info("✅ Added area_per_room (area / total_rooms)")

            # 3. Room ratio - UNIVERSAL PROPERTY CHARACTERISTIC
            if all(col_name in df.columns for col_name in ["bedroom", "bathroom"]):
                df = df.withColumn(
                    "bedroom_bathroom_ratio",
                    when(
                        col("bathroom") > 0, col("bedroom") / col("bathroom")
                    ).otherwise(
                        col("bedroom")
                    ),  # If no bathrooms, just bedroom count
                )
                logger.info("✅ Added bedroom_bathroom_ratio (bedroom / bathroom)")

        except Exception as e:
            logger.error(f"❌ Error adding property features: {str(e)}")

        return df

    def _add_population_features(self, df: DataFrame) -> DataFrame:
        """
        Add population density feature from CSV file via province_id join

        This adds exactly 1 feature:
        - population_density: Population density của tỉnh thành (từ CSV)

        Join logic:
        - Join main DataFrame với province_population_density.csv qua province_id
        - Default value = 0.0 nếu không match được
        """
        logger.info("🌍 Adding population density feature from CSV")

        try:
            # Path to population density CSV
            csv_path = "/app/ml/utils/province_stats/province_population_density.csv"

            # Read population density CSV
            pop_density_df = self.spark.read.csv(
                csv_path, header=True, inferSchema=True
            ).select(
                col("province_id").cast("double"),  # Ensure matching data type
                col("population_density").cast("double"),
            )

            logger.info(
                f"📊 Loaded population density data: {pop_density_df.count()} provinces"
            )

            # Left join to preserve all main data
            df = df.join(pop_density_df, on="province_id", how="left")

            # Fill null population_density with 0.0 (provinces not in CSV)
            df = df.withColumn(
                "population_density", coalesce(col("population_density"), lit(0.0))
            )

            logger.info("✅ Added population_density feature (joined from CSV)")

        except Exception as e:
            logger.error(f"❌ Error adding population features: {str(e)}")
            # Fallback: add default population_density column
            df = df.withColumn("population_density", lit(0.0))
            logger.info("⚠️ Using default population_density = 0.0")

        return df

    def get_final_feature_columns(self) -> List[str]:
        """
        Get list of final feature columns for ML training

        Returns 16 features:
        - 12 core features từ self.final_features
        - 3 property features: total_rooms, area_per_room, bedroom_bathroom_ratio
        - 1 population feature: population_density
        """
        final_16_features = self.final_features.copy()  # 12 core + price + id

        # Add 3 property features
        final_16_features.extend(
            ["total_rooms", "area_per_room", "bedroom_bathroom_ratio"]
        )

        # Add 1 population feature
        final_16_features.append("population_density")

        return final_16_features

    def validate_features(self, df: DataFrame) -> Dict[str, any]:
        """Validate feature quality and completeness for all 16 features"""
        logger.info("🔍 Validating feature quality for 16 features")

        final_features = self.get_final_feature_columns()

        validation_results = {
            "total_rows": df.count(),
            "required_features": final_features,
            "available_features": [col for col in final_features if col in df.columns],
            "missing_features": [
                col for col in final_features if col not in df.columns
            ],
            "feature_completeness": {},
        }

        # Check completeness for each feature
        for feature in final_features:
            if feature in df.columns:
                total_count = df.count()
                non_null_count = df.filter(col(feature).isNotNull()).count()
                validation_results["feature_completeness"][feature] = (
                    non_null_count / total_count if total_count > 0 else 0
                )

        logger.info(f"✅ Feature validation completed")
        logger.info(
            f"📊 Available features: {len(validation_results['available_features'])}/{len(final_features)}"
        )

        return validation_results


# Convenience functions
def create_features(df: DataFrame, spark: SparkSession = None) -> DataFrame:
    """Convenience function to create ML features"""
    engineer = FeatureEngineer(spark)
    return engineer.create_all_features(df)


def get_required_columns() -> List[str]:
    """Get list of required columns for ML training"""
    engineer = FeatureEngineer()
    return engineer.get_final_feature_columns()


if __name__ == "__main__":
    print("🔧 RATIONAL ML Feature Engineering - Final 16 Features")
    print("12 core + 3 property + 1 population = 16 features total")
    print("No HCM bias, only universal meaningful features")
    print()
    print("Final 16 features:")
    final_features = get_required_columns()
    for i, feature in enumerate(final_features, 1):
        print(f"{i:2d}. {feature}")
    print()
    print(f"Total: {len(final_features)} features")
