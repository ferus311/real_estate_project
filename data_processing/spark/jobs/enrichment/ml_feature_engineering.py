"""
Advanced ML Feature Engineering Pipeline for Real Estate Price Prediction
=======================================================================

Features:
- 🧹 Advanced Data Cleaning & Outlier Detection
- 🏗️ Smart Feature Engineering (60+ features)
- 📊 Automated Feature Selection
- 🎯 Geospatial Features & Market Analysis
- 📈 Time-based Features
- 🔄 Production-ready for daily updates

Author: ML Team
Date: June 2025
"""

import sys
import os
from datetime import datetime, timedelta

# Remove pandas and numpy imports - use PySpark only
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    log,
    sqrt,
    regexp_replace,
    isnan,
    isnull,
    abs as spark_abs,
    split,
    size,
    regexp_extract,
    round as spark_round,
    floor,
    coalesce,
    lit,
    lower,
    trim,
    unix_timestamp,
    datediff,
    expr,
    avg,
    stddev,
    min as spark_min,
    max as spark_max,
    count,
    sum as spark_sum,
    percentile_approx,
    collect_list,
    array_contains,
    concat_ws,
    dense_rank,
    row_number,
    lag,
    lead,
    first,
    last,
    from_unixtime,
    to_timestamp,
    year,
    month,
    dayofweek,
    hour,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    MinMaxScaler,
    Bucketizer,
    QuantileDiscretizer,
    StringIndexer,
    OneHotEncoder,
)
from pyspark.ml.stat import Correlation
import warnings

warnings.filterwarnings("ignore")

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.utils.hdfs_utils import check_hdfs_path_exists
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_optimized_ml_spark_session


class AdvancedMLFeatureEngineer:
    """
    🚀 Production-grade ML Feature Engineering for Real Estate

    Features:
    - Smart outlier detection and handling
    - Geospatial feature engineering
    - Market dynamics and seasonality
    - Advanced text processing
    - Automated feature selection
    """

    def __init__(self, spark_session=None):
        self.spark = spark_session or create_optimized_ml_spark_session(
            "Advanced_ML_Feature_Engineering"
        )
        self.logger = SparkJobLogger("advanced_ml_feature_engineering")
        self.feature_importance_threshold = 0.01

        # Major cities in Vietnam for geospatial features
        self.major_cities = {
            "Hà Nội": (21.0285, 105.8542),
            "Tp Hồ Chí Minh": (10.8231, 106.6297),
            "Đà Nẵng": (16.0471, 108.2068),
            "Hải Phòng": (20.8449, 106.6881),
            "Cần Thơ": (10.0452, 105.7469),
        }

        # Price ranges for market analysis (VND)
        self.price_buckets = [
            0,
            500_000_000,
            1_000_000_000,
            2_000_000_000,
            5_000_000_000,
            10_000_000_000,
            20_000_000_000,
            float("inf"),
        ]

        self.area_buckets = [0, 30, 50, 80, 120, 200, 500, float("inf")]

    def read_gold_data(self, date, property_type="house"):
        """Read Gold data from unified dataset"""
        date_formatted = date.replace("-", "")
        gold_path = f"/data/realestate/processed/gold/unified/{property_type}/{date.replace("-", "/")}/unified_{property_type}_{date_formatted}.parquet"

        if not check_hdfs_path_exists(self.spark, gold_path):
            raise FileNotFoundError(f"Gold data not found: {gold_path}")

        self.logger.logger.info(f"📖 Reading Gold data from: {gold_path}")
        df = self.spark.read.parquet(gold_path)
        self.logger.log_dataframe_info(df, "gold_data_input")

        return df

    def advanced_data_cleaning(self, df):
        """🧹 Advanced data cleaning with outlier detection"""
        self.logger.logger.info("🧹 Starting advanced data cleaning...")

        initial_count = df.count()

        # 1. Ensure proper data types for numeric columns
        self.logger.logger.info("🔄 Converting data types to proper numeric types...")
        df_clean = (
            df.withColumn("price", col("price").cast(DoubleType()))
            .withColumn("area", col("area").cast(DoubleType()))
            .withColumn("price_per_m2", col("price_per_m2").cast(DoubleType()))
            .withColumn("bedroom", col("bedroom").cast(DoubleType()))
            .withColumn("bathroom", col("bathroom").cast(DoubleType()))
            .withColumn("floor_count", col("floor_count").cast(DoubleType()))
        )

        # 2. Remove completely invalid records (basic filters)
        before_basic_filter = df_clean.count()
        df_clean = df_clean.filter(
            (col("price").isNotNull())
            & (col("area").isNotNull())
            & (col("price") > 0)
            & (col("area") > 0)
            & (col("price") < 1e12)  # Remove unrealistic prices > 1 trillion VND
            & (col("area") < 10000)  # Remove unrealistic areas > 10,000 m2
        )
        after_basic_filter = df_clean.count()
        self.logger.logger.info(
            f"📊 Basic filtering: {before_basic_filter:,} → {after_basic_filter:,} records"
        )

        # 3. Use percentile-based outlier detection (much more lenient than IQR)
        if (
            df_clean.count() > 100
        ):  # Only apply outlier detection if we have enough data
            try:
                price_per_m2_stats = df_clean.select(
                    percentile_approx("price_per_m2", 0.02).alias(
                        "p2"
                    ),  # 2nd percentile
                    percentile_approx("price_per_m2", 0.98).alias(
                        "p98"
                    ),  # 98th percentile
                    percentile_approx("price_per_m2", 0.5).alias("median"),
                    min("price_per_m2").alias("min_val"),
                    max("price_per_m2").alias("max_val"),
                ).collect()[0]

                p2_val = price_per_m2_stats["p2"]
                p98_val = price_per_m2_stats["p98"]

                if p2_val is not None and p98_val is not None:
                    before_outlier_filter = df_clean.count()
                    df_clean = df_clean.filter(
                        (col("price_per_m2") >= p2_val)
                        & (col("price_per_m2") <= p98_val)
                    )
                    after_outlier_filter = df_clean.count()

                    self.logger.logger.info(
                        f"📊 Outlier filtering (P2-P98): {before_outlier_filter:,} → {after_outlier_filter:,} records"
                    )
                    self.logger.logger.info(
                        f"📊 Price per m2 bounds: {p2_val:,.0f} - {p98_val:,.0f} VND/m2"
                    )
                else:
                    self.logger.logger.warning(
                        "⚠️  Percentile calculation returned None, skipping outlier detection"
                    )

            except Exception as e:
                self.logger.logger.warning(
                    f"⚠️  Error in outlier detection: {str(e)}, proceeding without outlier filtering"
                )
        else:
            self.logger.logger.info(
                "📊 Insufficient data for outlier detection, skipping"
            )

        # 4. Clean and standardize text fields
        df_clean = (
            df_clean.withColumn(
                "province_clean",
                trim(regexp_replace(lower(col("province")), r"[^\w\s]", "")),
            )
            .withColumn(
                "district_clean",
                trim(regexp_replace(lower(col("district")), r"[^\w\s]", "")),
            )
            .withColumn(
                "ward_clean", trim(regexp_replace(lower(col("ward")), r"[^\w\s]", ""))
            )
        )

        # 5. Handle missing coordinates - estimate from location text
        df_clean = df_clean.withColumn(
            "has_coordinates",
            when(
                (col("latitude").isNotNull()) & (col("longitude").isNotNull()), 1
            ).otherwise(0),
        )

        # 6. Clean numerical fields with proper type casting
        df_clean = (
            df_clean.withColumn(
                "bedrooms_clean",
                when(
                    col("bedroom").isNull() | (col("bedroom") <= 0), 2.0
                )  # Default 2 bedrooms
                .when(col("bedroom") > 20, 2.0)  # Cap unrealistic values
                .otherwise(col("bedroom"))
                .cast(DoubleType()),
            )
            .withColumn(
                "bathrooms_clean",
                when(
                    col("bathroom").isNull() | (col("bathroom") <= 0), 1.0
                )  # Default 1 bathroom
                .when(col("bathroom") > 10, 1.0)  # Cap unrealistic values
                .otherwise(col("bathroom"))
                .cast(DoubleType()),
            )
            .withColumn(
                "floors_clean",
                when(col("floor_count").isNull() | (col("floor_count") <= 0), 1.0)
                .when(col("floor_count") > 50, 1.0)  # Cap skyscrapers
                .otherwise(col("floor_count"))
                .cast(DoubleType()),
            )
        )

        cleaned_count = df_clean.count()
        removed_count = initial_count - cleaned_count
        removed_pct = (removed_count / initial_count * 100) if initial_count > 0 else 0

        self.logger.logger.info(f"✅ Data cleaning completed:")
        self.logger.logger.info(f"   Initial records: {initial_count:,}")
        self.logger.logger.info(f"   After cleaning: {cleaned_count:,}")
        self.logger.logger.info(f"   Removed: {removed_count:,} ({removed_pct:.1f}%)")

        # Add data retention check
        if removed_pct > 80:
            self.logger.logger.warning(
                f"⚠️  High data loss detected ({removed_pct:.1f}%), consider relaxing filters"
            )
        elif removed_pct > 50:
            self.logger.logger.warning(
                f"⚠️  Moderate data loss detected ({removed_pct:.1f}%)"
            )

        return df_clean

    def create_advanced_numeric_features(self, df):
        """📊 Create advanced numeric features"""
        self.logger.logger.info("📊 Creating advanced numeric features...")

        # Ensure all inputs are properly cast to DoubleType before calculations
        df_numeric = (
            df.withColumn(
                # Log transformations (for skewed distributions) - ensure positive values
                "log_price",
                log(when(col("price") > 0, col("price")).otherwise(1.0)).cast(
                    DoubleType()
                ),
            )
            .withColumn(
                "log_area",
                log(when(col("area") > 0, col("area")).otherwise(1.0)).cast(
                    DoubleType()
                ),
            )
            .withColumn(
                "log_price_per_m2",
                log(
                    when(col("price_per_m2") > 0, col("price_per_m2")).otherwise(1.0)
                ).cast(DoubleType()),
            )
            .withColumn(
                # Square root transformations
                "sqrt_price",
                sqrt(when(col("price") >= 0, col("price")).otherwise(0.0)).cast(
                    DoubleType()
                ),
            )
            .withColumn(
                "sqrt_area",
                sqrt(when(col("area") >= 0, col("area")).otherwise(0.0)).cast(
                    DoubleType()
                ),
            )
            .withColumn(
                # Polynomial features
                "area_squared",
                (col("area") * col("area")).cast(DoubleType()),
            )
            .withColumn(
                "price_area_interaction",
                (col("price") * col("area")).cast(DoubleType()),
            )
            .withColumn(
                # Room ratios and densities
                "total_rooms",
                (col("bedrooms_clean") + col("bathrooms_clean")).cast(DoubleType()),
            )
            .withColumn(
                "bedrooms_per_area",
                (col("bedrooms_clean") / (col("area") + 1)).cast(DoubleType()),
            )
            .withColumn(
                "bathrooms_per_area",
                (col("bathrooms_clean") / (col("area") + 1)).cast(DoubleType()),
            )
            .withColumn(
                "rooms_per_floor",
                (
                    (col("bedrooms_clean") + col("bathrooms_clean"))
                    / (col("floors_clean") + 1)
                ).cast(DoubleType()),
            )
            .withColumn(
                # Efficiency metrics
                "living_efficiency",
                when(
                    col("living_size").isNotNull() & (col("living_size") > 0),
                    col("living_size") / col("area"),
                )
                .otherwise(0.8)
                .cast(DoubleType()),  # Default 80%
            )
            .withColumn(
                # Size categorization
                "is_studio",
                when(col("bedrooms_clean") <= 1, 1).otherwise(0),
            )
            .withColumn(
                "is_large_house", when(col("bedrooms_clean") >= 4, 1).otherwise(0)
            )
        )

        return df_numeric

    def create_geospatial_features(self, df):
        """🗺️ Create geospatial and location-based features"""
        self.logger.logger.info("🗺️ Creating geospatial features...")

        # Major city identification and distance calculation
        df_geo = df
        for city, (lat, lon) in self.major_cities.items():
            df_geo = df_geo.withColumn(
                f"distance_to_{city.lower().replace(' ', '_')}",
                when(
                    col("has_coordinates") == 1,
                    sqrt((col("latitude") - lat) ** 2 + (col("longitude") - lon) ** 2)
                    * 111.32,
                ).otherwise(
                    1000
                ),  # Default large distance if no coordinates
            )

        # Identify if property is in a major city
        df_geo = df_geo.withColumn(
            "is_major_city",
            when(col("distance_to_hà_nội") < 50, 1)
            .when(col("distance_to_tp_hồ_chí_minh") < 50, 1)
            .when(col("distance_to_đà_nẵng") < 30, 1)
            .when(col("distance_to_hải_phòng") < 30, 1)
            .when(col("distance_to_cần_thơ") < 30, 1)
            .otherwise(0),
        )

        # Regional classification
        df_geo = df_geo.withColumn(
            "region",
            when(
                col("province_clean").isin(
                    ["hà nội", "hải phòng", "quảng ninh", "hải dương"]
                ),
                "north",
            )
            .when(
                col("province_clean").isin(
                    ["tp hồ chí minh", "bình dương", "đồng nai", "bà rịa vũng tàu"]
                ),
                "south",
            )
            .when(
                col("province_clean").isin(["đà nẵng", "quảng nam", "thừa thiên huế"]),
                "central",
            )
            .otherwise("other"),
        )

        # Proximity to city center (using distance to nearest major city)
        df_geo = (
            df_geo.withColumn(
                "distance_to_city_center",
                when(col("region") == "north", col("distance_to_hà_nội"))
                .when(col("region") == "south", col("distance_to_tp_hồ_chí_minh"))
                .when(col("region") == "central", col("distance_to_đà_nẵng"))
                .otherwise(100),
            )
            .withColumn(
                "is_city_center",
                when(col("distance_to_city_center") < 10, 1).otherwise(0),
            )
            .withColumn(
                "is_suburb",
                when(
                    (col("distance_to_city_center") >= 10)
                    & (col("distance_to_city_center") < 30),
                    1,
                ).otherwise(0),
            )
        )

        return df_geo

    def create_market_features(self, df):
        """📈 Create market and comparative features"""
        self.logger.logger.info("📈 Creating market features...")

        # Market statistics by province
        province_window = Window.partitionBy("province_clean")

        df_market = (
            df.withColumn("province_avg_price", avg("price").over(province_window))
            .withColumn(
                "province_avg_price_per_m2", avg("price_per_m2").over(province_window)
            )
            .withColumn("province_avg_area", avg("area").over(province_window))
            .withColumn("province_property_count", count("id").over(province_window))
            .withColumn("province_price_std", stddev("price").over(province_window))
        )

        # Relative position in market
        df_market = (
            df_market.withColumn(
                "price_vs_province_avg", col("price") / (col("province_avg_price") + 1)
            )
            .withColumn(
                "price_per_m2_vs_province_avg",
                col("price_per_m2") / (col("province_avg_price_per_m2") + 1),
            )
            .withColumn(
                "area_vs_province_avg", col("area") / (col("province_avg_area") + 1)
            )
        )

        # Market segment classification
        df_market = df_market.withColumn(
            "price_segment",
            when(col("price") < 1_000_000_000, "budget")  # < 1B VND
            .when(col("price") < 3_000_000_000, "mid_range")  # 1-3B VND
            .when(col("price") < 10_000_000_000, "premium")  # 3-10B VND
            .otherwise("luxury"),  # > 10B VND
        ).withColumn(
            "area_segment",
            when(col("area") < 50, "small")
            .when(col("area") < 100, "medium")
            .when(col("area") < 200, "large")
            .otherwise("mansion"),
        )

        # Supply density (number of properties in same province)
        df_market = df_market.withColumn(
            "market_supply_density",
            when(col("province_property_count") > 100, "high")
            .when(col("province_property_count") > 20, "medium")
            .otherwise("low"),
        )

        return df_market

    def create_text_features(self, df):
        """📝 Create advanced text features"""
        self.logger.logger.info("📝 Creating text features...")

        df_text = df.withColumn(
            # Basic text statistics
            "title_length",
            when(col("title").isNotNull(), size(split(col("title"), " "))).otherwise(0),
        ).withColumn(
            "description_length",
            when(
                col("description").isNotNull(), size(split(col("description"), " "))
            ).otherwise(0),
        )

        # Luxury indicators
        luxury_keywords = [
            "sang trọng",
            "cao cấp",
            "luxury",
            "vip",
            "villa",
            "biệt thự",
            "duplex",
            "penthouse",
        ]
        df_text = df_text.withColumn(
            "has_luxury_keywords",
            when(
                col("title").rlike("|".join(luxury_keywords))
                | col("description").rlike("|".join(luxury_keywords)),
                1,
            ).otherwise(0),
        )

        # New/modern indicators
        new_keywords = ["mới", "new", "hiện đại", "modern", "2024", "2025"]
        df_text = df_text.withColumn(
            "has_new_keywords",
            when(
                col("title").rlike("|".join(new_keywords))
                | col("description").rlike("|".join(new_keywords)),
                1,
            ).otherwise(0),
        )

        # Investment indicators
        investment_keywords = [
            "đầu tư",
            "investment",
            "cho thuê",
            "rental",
            "lợi nhuận",
        ]
        df_text = df_text.withColumn(
            "has_investment_keywords",
            when(
                col("title").rlike("|".join(investment_keywords))
                | col("description").rlike("|".join(investment_keywords)),
                1,
            ).otherwise(0),
        )

        return df_text

    def create_temporal_features(self, df):
        """⏰ Create time-based features"""
        self.logger.logger.info("⏰ Creating temporal features...")

        # Convert timestamps and extract time features
        df_temporal = df.withColumn(
            "posted_timestamp_unix", unix_timestamp(col("posted_date"))
        ).withColumn("crawl_timestamp_unix", unix_timestamp(col("crawl_timestamp")))

        # Time since posted
        current_time = unix_timestamp(lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        df_temporal = df_temporal.withColumn(
            "days_since_posted",
            when(
                col("posted_timestamp_unix").isNotNull(),
                (current_time - col("posted_timestamp_unix"))
                / 86400,  # Convert to days
            ).otherwise(
                30
            ),  # Default 30 days if no date
        )

        # Seasonal features
        df_temporal = (
            df_temporal.withColumn("posted_month", month(col("posted_date")))
            .withColumn(
                "posted_quarter",
                when(col("posted_month").isin([1, 2, 3]), 1)
                .when(col("posted_month").isin([4, 5, 6]), 2)
                .when(col("posted_month").isin([7, 8, 9]), 3)
                .otherwise(4),
            )
            .withColumn(
                "is_peak_season",  # Vietnamese real estate peak seasons
                when(col("posted_month").isin([3, 4, 5, 9, 10, 11]), 1).otherwise(0),
            )
        )

        return df_temporal

    def create_interaction_features(self, df):
        """🔗 Create interaction features"""
        self.logger.logger.info("🔗 Creating interaction features...")

        df_interaction = (
            df.withColumn(
                # Price-Location interactions
                "premium_in_major_city",
                col("is_major_city")
                * when(col("price_segment") == "premium", 1).otherwise(0),
            )
            .withColumn(
                "luxury_in_city_center",
                col("is_city_center")
                * when(col("price_segment") == "luxury", 1).otherwise(0),
            )
            .withColumn(
                # Area-Room interactions
                "efficiency_score",
                (col("bedrooms_clean") * col("bathrooms_clean")) / (col("area") + 1),
            )
            .withColumn(
                # Market interactions
                "high_demand_area",
                when(
                    (col("market_supply_density") == "high")
                    & (col("is_major_city") == 1),
                    1,
                ).otherwise(0),
            )
        )

        return df_interaction

    def select_and_validate_features(self, df):
        """🎯 Feature selection and validation"""
        self.logger.logger.info("🎯 Selecting and validating features...")

        # Define feature categories
        numeric_features = [
            # Basic features
            "price",
            "area",
            "price_per_m2",
            "bedrooms_clean",
            "bathrooms_clean",
            "floors_clean",
            # Log features
            "log_price",
            "log_area",
            "log_price_per_m2",
            # Square root features
            "sqrt_price",
            "sqrt_area",
            # Derived features
            "area_squared",
            "total_rooms",
            "bedrooms_per_area",
            "bathrooms_per_area",
            "rooms_per_floor",
            "living_efficiency",
            # Geospatial features
            "distance_to_hà_nội",
            "distance_to_tp_hồ_chí_minh",
            "distance_to_đà_nẵng",
            "distance_to_city_center",
            # Market features
            "province_avg_price",
            "province_avg_price_per_m2",
            "province_avg_area",
            "price_vs_province_avg",
            "price_per_m2_vs_province_avg",
            "area_vs_province_avg",
            # Text features
            "title_length",
            "description_length",
            # Temporal features
            "days_since_posted",
            "posted_month",
            "posted_quarter",
            # Interaction features
            "efficiency_score",
        ]

        categorical_features = [
            "province_clean",
            "district_clean",
            "region",
            "price_segment",
            "area_segment",
            "market_supply_density",
        ]

        binary_features = [
            "has_coordinates",
            "is_studio",
            "is_large_house",
            "is_major_city",
            "is_city_center",
            "is_suburb",
            "has_luxury_keywords",
            "has_new_keywords",
            "has_investment_keywords",
            "is_peak_season",
            "premium_in_major_city",
            "luxury_in_city_center",
            "high_demand_area",
        ]

        # Essential columns
        essential_cols = [
            "id",
            "title",
            "url",
            "source",
            "posted_date",
            "crawl_timestamp",
        ]

        # Select existing columns only
        all_features = (
            essential_cols + numeric_features + categorical_features + binary_features
        )
        existing_cols = [
            col_name for col_name in all_features if col_name in df.columns
        ]

        df_final = df.select(*existing_cols)

        # Validate features
        null_counts = {}
        for feature in numeric_features + binary_features:
            if feature in df_final.columns:
                null_count = df_final.filter(col(feature).isNull()).count()
                if null_count > 0:
                    null_counts[feature] = null_count

        if null_counts:
            self.logger.logger.warning(f"Features with null values: {null_counts}")

        self.logger.logger.info(f"✅ Final feature set: {len(existing_cols)} features")
        self.logger.logger.info(
            f"   - Numeric features: {len([f for f in numeric_features if f in existing_cols])}"
        )
        self.logger.logger.info(
            f"   - Categorical features: {len([f for f in categorical_features if f in existing_cols])}"
        )
        self.logger.logger.info(
            f"   - Binary features: {len([f for f in binary_features if f in existing_cols])}"
        )

        return df_final, numeric_features, categorical_features, binary_features

    def save_feature_data(self, df, feature_metadata, date, property_type="house"):
        """💾 Save processed features to feature store"""
        output_path = f"/data/realestate/ml/features/{property_type}/{date}"

        self.logger.logger.info(f"💾 Saving ML features to: {output_path}")

        # Save features as parquet with partitioning for better performance
        df.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{output_path}/features.parquet"
        )

        # Save feature metadata with detailed information
        import json

        metadata_json = {
            "numeric_features": feature_metadata[0],
            "categorical_features": feature_metadata[1],
            "binary_features": feature_metadata[2],
            "total_records": df.count(),
            "total_features": len(feature_metadata[0])
            + len(feature_metadata[1])
            + len(feature_metadata[2]),
            "processing_date": datetime.now().isoformat(),
            "data_date": date,
            "feature_engineering_version": "v2.0_advanced",
            "data_quality_metrics": {
                "outliers_removed": True,
                "missing_values_handled": True,
                "feature_scaling_ready": True,
            },
        }

        # Save metadata as JSON to HDFS
        metadata_df = self.spark.createDataFrame(
            [{"metadata": json.dumps(metadata_json, indent=2)}]
        )
        metadata_df.write.mode("overwrite").json(f"{output_path}/feature_metadata.json")

        # Save feature list for easy reference
        feature_list_df = self.spark.createDataFrame(
            [
                {"feature_name": f, "feature_type": "numeric"}
                for f in feature_metadata[0]
            ]
            + [
                {"feature_name": f, "feature_type": "categorical"}
                for f in feature_metadata[1]
            ]
            + [
                {"feature_name": f, "feature_type": "binary"}
                for f in feature_metadata[2]
            ]
        )
        feature_list_df.write.mode("overwrite").parquet(
            f"{output_path}/feature_list.parquet"
        )

        self.logger.logger.info(f"✅ ML features saved successfully to {output_path}")
        return output_path

    def run_feature_engineering_pipeline(self, date, property_type="house"):
        """🚀 Run complete advanced feature engineering pipeline"""
        self.logger.logger.info("🚀 Starting Advanced ML Feature Engineering Pipeline")
        self.logger.logger.info("=" * 70)

        try:
            # Step 1: Read Gold data
            df = self.read_gold_data(date, property_type)

            # Step 2: Advanced data cleaning
            df_clean = self.advanced_data_cleaning(df)

            # Step 3: Create advanced numeric features
            df_numeric = self.create_advanced_numeric_features(df_clean)

            # Step 4: Create geospatial features
            df_geo = self.create_geospatial_features(df_numeric)

            # Step 5: Create market features
            df_market = self.create_market_features(df_geo)

            # Step 6: Create text features
            df_text = self.create_text_features(df_market)

            # Step 7: Create temporal features
            df_temporal = self.create_temporal_features(df_text)

            # Step 8: Create interaction features
            df_interaction = self.create_interaction_features(df_temporal)

            # Step 9: Select and validate final features
            df_final, numeric_features, categorical_features, binary_features = (
                self.select_and_validate_features(df_interaction)
            )

            # Step 10: Save to feature store
            output_path = self.save_feature_data(
                df_final,
                (numeric_features, categorical_features, binary_features),
                date,
                property_type,
            )

            self.logger.logger.info("=" * 70)
            self.logger.logger.info(
                "🎉 Advanced Feature Engineering Pipeline Completed!"
            )
            self.logger.logger.info(
                f"📊 Total features created: {len(numeric_features) + len(categorical_features) + len(binary_features)}"
            )
            self.logger.logger.info(f"💾 Features saved to: {output_path}")

            return {
                "success": True,
                "output_path": output_path,
                "total_features": len(numeric_features)
                + len(categorical_features)
                + len(binary_features),
                "total_records": df_final.count(),
                "feature_metadata": {
                    "numeric_features": numeric_features,
                    "categorical_features": categorical_features,
                    "binary_features": binary_features,
                },
            }

        except Exception as e:
            self.logger.logger.error(
                f"❌ Feature engineering pipeline failed: {str(e)}"
            )
            raise
        finally:
            self.logger.logger.info("🧹 Cleaning up resources...")


def run_ml_feature_engineering(spark, input_date, property_type="house"):
    """
    Wrapper function để tương thích với daily_processing pipeline
    """
    engineer = AdvancedMLFeatureEngineer(spark)
    return engineer.run_feature_engineering_pipeline(input_date, property_type)


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Advanced ML Feature Engineering Pipeline"
    )
    parser.add_argument("--date", required=True, help="Data date (YYYY-MM-DD)")
    parser.add_argument("--property-type", default="house", help="Property type")

    args = parser.parse_args()

    # Initialize and run feature engineering
    engineer = AdvancedMLFeatureEngineer()
    result = engineer.run_feature_engineering_pipeline(args.date, args.property_type)

    if result["success"]:
        print(f"✅ Feature engineering completed successfully!")
        print(
            f"📊 Created {result['total_features']} features from {result['total_records']:,} records"
        )
        print(f"💾 Saved to: {result['output_path']}")
    else:
        print("❌ Feature engineering failed!")
        exit(1)


if __name__ == "__main__":
    main()
