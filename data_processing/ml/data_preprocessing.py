"""
Data Preprocessing & Feature Engineering cho Real Estate ML Pipeline
Xử lý dữ liệu từ Gold layer để chuẩn bị cho ML training
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    isnan,
    isnull,
    regexp_replace,
    split,
    trim,
    lower,
    upper,
    log,
    sqrt,
    pow,
    abs as spark_abs,
    coalesce,
    lit,
    regexp_extract,
    length,
    avg,
    stddev,
    percentile_approx,
    count,
    sum as spark_sum,
    udf,
    monotonically_increasing_id,
    desc,
    asc,
    dense_rank,
    year,
    month,
    dayofmonth,
    weekofyear,
    quarter,
    window,
    lag,
    lead,
    first,
    last,
)
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    StringIndexer,
    OneHotEncoder,
)
from pyspark.ml.stat import Correlation
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class RealEstateDataProcessor:
    """Comprehensive data processing for real estate ML"""

    def __init__(self, spark_session):
        self.spark = spark_session
        self.data_quality_stats = {}

    def validate_data_quality(self, df: DataFrame) -> DataFrame:
        """
        Bước 1: Data Quality Validation
        Kiểm tra chất lượng dữ liệu và ghi log các vấn đề
        """
        logger.info("=== Data Quality Validation ===")

        total_records = df.count()
        logger.info(f"Total records: {total_records:,}")

        # Check missing values per column
        missing_stats = []
        for col_name in df.columns:
            missing_count = df.filter(
                col(col_name).isNull() | isnan(col(col_name))
            ).count()
            missing_pct = (missing_count / total_records) * 100
            missing_stats.append(
                {
                    "column": col_name,
                    "missing_count": missing_count,
                    "missing_percentage": missing_pct,
                }
            )
            if missing_pct > 50:
                logger.warning(f"High missing rate in {col_name}: {missing_pct:.1f}%")

        # Check duplicates
        duplicate_count = total_records - df.dropDuplicates().count()
        logger.info(f"Duplicate records: {duplicate_count:,}")

        # Price validation
        if "price" in df.columns:
            price_stats = df.select(
                col("price"),
                percentile_approx("price", 0.01).alias("p1"),
                percentile_approx("price", 0.99).alias("p99"),
                avg("price").alias("mean_price"),
                stddev("price").alias("std_price"),
            ).collect()[0]

            logger.info(
                f"Price stats - Mean: {price_stats['mean_price']:,.0f}, "
                f"P1: {price_stats['p1']:,.0f}, P99: {price_stats['p99']:,.0f}"
            )

        # Area validation
        if "area" in df.columns:
            area_stats = df.select(
                percentile_approx("area", 0.01).alias("p1"),
                percentile_approx("area", 0.99).alias("p99"),
                avg("area").alias("mean_area"),
            ).collect()[0]

            logger.info(
                f"Area stats - Mean: {area_stats['mean_area']:.1f}, "
                f"P1: {area_stats['p1']:.1f}, P99: {area_stats['p99']:.1f}"
            )

        self.data_quality_stats = {
            "total_records": total_records,
            "missing_stats": missing_stats,
            "duplicate_count": duplicate_count,
        }

        return df

    def clean_basic_data(self, df: DataFrame) -> DataFrame:
        """
        Bước 2: Basic Data Cleaning
        Loại bỏ records không hợp lệ và standardize format
        """
        logger.info("=== Basic Data Cleaning ===")

        initial_count = df.count()

        # Remove duplicates
        df_clean = df.dropDuplicates()

        # Clean price data
        if "price" in df.columns:
            df_clean = df_clean.filter(
                (col("price").isNotNull())
                & (col("price") > 0)
                & (col("price") < 1e12)  # Remove unrealistic prices
            )

        # Clean area data
        if "area" in df.columns:
            df_clean = df_clean.filter(
                (col("area").isNotNull())
                & (col("area") > 0)
                & (col("area") < 10000)  # Remove unrealistic areas (>1 hectare)
            )

        # Clean text fields
        text_columns = ["title", "description", "address", "district", "province"]
        for col_name in text_columns:
            if col_name in df.columns:
                df_clean = df_clean.withColumn(
                    col_name, trim(regexp_replace(col(col_name), r"[^\w\s\-\,\.]", ""))
                )

        final_count = df_clean.count()
        removed_count = initial_count - final_count
        logger.info(
            f"Removed {removed_count:,} invalid records ({removed_count/initial_count*100:.1f}%)"
        )

        return df_clean

    def handle_outliers(self, df: DataFrame) -> DataFrame:
        """
        Bước 3: Outlier Detection & Handling
        Phát hiện và xử lý các giá trị ngoại lai
        """
        logger.info("=== Outlier Detection & Handling ===")

        # Price outliers using IQR method
        if "price" in df.columns:
            price_percentiles = df.select(
                percentile_approx("price", 0.25).alias("q1"),
                percentile_approx("price", 0.75).alias("q3"),
            ).collect()[0]

            q1, q3 = price_percentiles["q1"], price_percentiles["q3"]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            outlier_count = df.filter(
                (col("price") < lower_bound) | (col("price") > upper_bound)
            ).count()

            logger.info(f"Price outliers detected: {outlier_count:,}")
            logger.info(f"Price bounds: {lower_bound:,.0f} - {upper_bound:,.0f}")

            # Cap outliers instead of removing them
            df = df.withColumn(
                "price",
                when(col("price") < lower_bound, lower_bound)
                .when(col("price") > upper_bound, upper_bound)
                .otherwise(col("price")),
            )

        # Area outliers
        if "area" in df.columns:
            area_percentiles = df.select(
                percentile_approx("area", 0.05).alias("p5"),
                percentile_approx("area", 0.95).alias("p95"),
            ).collect()[0]

            area_lower = area_percentiles["p5"]
            area_upper = area_percentiles["p95"]

            df = df.withColumn(
                "area",
                when(col("area") < area_lower, area_lower)
                .when(col("area") > area_upper, area_upper)
                .otherwise(col("area")),
            )

        return df

    def clean_and_parse_address(self, df: DataFrame) -> DataFrame:
        """
        Bước 4: Address Cleaning & Parsing
        Standardize địa chỉ và extract thông tin location
        """
        logger.info("=== Address Cleaning & Parsing ===")

        # Standardize province names
        if "province" in df.columns:
            df = df.withColumn(
                "province_clean",
                trim(upper(regexp_replace(col("province"), r"(Tỉnh|Thành phố)", ""))),
            )

            # Map common province variations
            province_mapping = {
                "HO CHI MINH": "HCM",
                "HA NOI": "HANOI",
                "DA NANG": "DANANG",
            }

            for old_name, new_name in province_mapping.items():
                df = df.withColumn(
                    "province_clean",
                    when(col("province_clean").contains(old_name), new_name).otherwise(
                        col("province_clean")
                    ),
                )

        # Extract district information
        if "address" in df.columns:
            df = df.withColumn(
                "district_extracted",
                regexp_extract(col("address"), r"(Quận|Huyện)\s+([^,]+)", 2),
            )

            # Extract ward information
            df = df.withColumn(
                "ward_extracted",
                regexp_extract(col("address"), r"(Phường|Xã)\s+([^,]+)", 2),
            )

        return df

    def impute_missing_values(self, df: DataFrame) -> DataFrame:
        """
        Bước 5: Missing Value Imputation
        Điền các giá trị thiếu bằng strategy phù hợp
        """
        logger.info("=== Missing Value Imputation ===")

        # Bedrooms: fill with median by province and property type
        if "bedrooms" in df.columns:
            # Calculate median bedrooms by province
            median_bedrooms = df.groupBy("province_clean").agg(
                percentile_approx("bedrooms", 0.5).alias("median_bedrooms")
            )

            df = df.join(median_bedrooms, "province_clean", "left")
            df = df.withColumn(
                "bedrooms", coalesce(col("bedrooms"), col("median_bedrooms"), lit(2))
            ).drop("median_bedrooms")

        # Bathrooms: fill with bedrooms/2 or 1
        if "bathrooms" in df.columns and "bedrooms" in df.columns:
            df = df.withColumn(
                "bathrooms",
                coalesce(
                    col("bathrooms"), (col("bedrooms") / 2).cast(IntegerType()), lit(1)
                ),
            )

        # Floor: fill with median
        if "floor" in df.columns:
            median_floor = df.select(percentile_approx("floor", 0.5)).collect()[0][0]
            df = df.withColumn("floor", coalesce(col("floor"), lit(median_floor)))

        return df

    def create_advanced_features(self, df: DataFrame) -> DataFrame:
        """
        Bước 6: Advanced Feature Engineering
        Tạo các features phức tạp cho ML model
        """
        logger.info("=== Advanced Feature Engineering ===")

        # Price-based features
        if "price" in df.columns and "area" in df.columns:
            df = df.withColumn("price_per_sqm", col("price") / col("area"))
            df = df.withColumn("log_price", log(col("price") + 1))
            df = df.withColumn("log_area", log(col("area") + 1))
            df = df.withColumn("sqrt_area", sqrt(col("area")))

        # Room-based features
        if "bedrooms" in df.columns and "area" in df.columns:
            df = df.withColumn("area_per_bedroom", col("area") / col("bedrooms"))

        if "bedrooms" in df.columns and "bathrooms" in df.columns:
            df = df.withColumn(
                "bedroom_bathroom_ratio", col("bedrooms") / col("bathrooms")
            )

        # Location-based features
        # Create location score based on province (can be enhanced with actual data)
        location_scores = {
            "HCM": 10,
            "HANOI": 9,
            "DANANG": 8,
            "HAIPHONG": 7,
            "CANTHO": 6,
        }

        location_score_expr = col("province_clean")
        for province, score in location_scores.items():
            location_score_expr = when(
                col("province_clean") == province, score
            ).otherwise(location_score_expr)

        df = df.withColumn(
            "location_score",
            when(
                col("province_clean").isin(list(location_scores.keys())),
                location_score_expr,
            ).otherwise(lit(5)),
        )

        # Text-based features
        if "title" in df.columns:
            df = df.withColumn("title_length", length(col("title")))

            # Luxury keywords
            luxury_keywords = ["cao cấp", "luxury", "vip", "sang trọng", "thượng lưu"]
            df = df.withColumn("is_luxury", lit(False))
            for keyword in luxury_keywords:
                df = df.withColumn(
                    "is_luxury",
                    col("is_luxury") | lower(col("title")).contains(keyword),
                )

        # Market segment features
        if "price_per_sqm" in [col.name for col in df.schema.fields]:
            # Calculate price quartiles for market segmentation
            price_per_sqm_percentiles = df.select(
                percentile_approx("price_per_sqm", 0.25).alias("q1"),
                percentile_approx("price_per_sqm", 0.5).alias("q2"),
                percentile_approx("price_per_sqm", 0.75).alias("q3"),
            ).collect()[0]

            q1, q2, q3 = (
                price_per_sqm_percentiles["q1"],
                price_per_sqm_percentiles["q2"],
                price_per_sqm_percentiles["q3"],
            )

            df = df.withColumn(
                "market_segment",
                when(col("price_per_sqm") <= q1, "budget")
                .when(col("price_per_sqm") <= q2, "mid-range")
                .when(col("price_per_sqm") <= q3, "high-end")
                .otherwise("luxury"),
            )

        # Property size category
        if "area" in df.columns:
            df = df.withColumn(
                "size_category",
                when(col("area") <= 50, "small")
                .when(col("area") <= 100, "medium")
                .when(col("area") <= 200, "large")
                .otherwise("very_large"),
            )

        return df

    def create_market_features(self, df: DataFrame) -> DataFrame:
        """
        Bước 7: Market-based Features
        Tạo features dựa trên thị trường và trend
        """
        logger.info("=== Market Features Creation ===")

        # Average price by province
        province_stats = df.groupBy("province_clean").agg(
            avg("price").alias("province_avg_price"),
            avg("price_per_sqm").alias("province_avg_price_per_sqm"),
            count("*").alias("province_listing_count"),
        )

        df = df.join(province_stats, "province_clean", "left")

        # Price relative to province average
        df = df.withColumn(
            "price_vs_province_avg", col("price") / col("province_avg_price")
        )

        # Market activity level
        df = df.withColumn(
            "market_activity_level",
            when(col("province_listing_count") >= 100, "high")
            .when(col("province_listing_count") >= 50, "medium")
            .otherwise("low"),
        )

        return df

    def prepare_final_features(self, df: DataFrame) -> DataFrame:
        """
        Bước 8: Final Feature Preparation
        Chuẩn bị features cuối cùng cho ML model
        """
        logger.info("=== Final Feature Preparation ===")

        # Select numeric features
        numeric_features = []
        for field in df.schema.fields:
            if field.dataType in [DoubleType(), IntegerType()]:
                if field.name not in ["price", "id"]:  # Exclude target and ID
                    numeric_features.append(field.name)

        # Select categorical features to encode
        categorical_features = [
            "province_clean",
            "market_segment",
            "size_category",
            "market_activity_level",
        ]
        categorical_features = [f for f in categorical_features if f in df.columns]

        # Handle any remaining null values in numeric features
        for feature in numeric_features:
            median_val = df.select(percentile_approx(feature, 0.5)).collect()[0][0]
            if median_val is not None:
                df = df.withColumn(feature, coalesce(col(feature), lit(median_val)))

        logger.info(f"Numeric features ({len(numeric_features)}): {numeric_features}")
        logger.info(
            f"Categorical features ({len(categorical_features)}): {categorical_features}"
        )

        return df, numeric_features, categorical_features

    def run_full_preprocessing(self, df: DataFrame) -> tuple:
        """
        Chạy toàn bộ pipeline preprocessing
        """
        logger.info("🚀 Starting Full Data Preprocessing Pipeline")

        # Step 1: Data Quality Validation
        df = self.validate_data_quality(df)

        # Step 2: Basic Cleaning
        df = self.clean_basic_data(df)

        # Step 3: Outlier Handling
        df = self.handle_outliers(df)

        # Step 4: Address Cleaning
        df = self.clean_and_parse_address(df)

        # Step 5: Missing Value Imputation
        df = self.impute_missing_values(df)

        # Step 6: Advanced Feature Engineering
        df = self.create_advanced_features(df)

        # Step 7: Market Features
        df = self.create_market_features(df)

        # Step 8: Final Preparation
        df, numeric_features, categorical_features = self.prepare_final_features(df)

        logger.info("✅ Data Preprocessing Pipeline Completed")
        logger.info(
            f"Final dataset shape: {df.count():,} rows, {len(df.columns)} columns"
        )

        return df, numeric_features, categorical_features, self.data_quality_stats
