"""
Chuyển đổi dữ liệu Chotot với FLAG data quality issues (Bronze → Silver)
THEO KIẾN TRÚC MEDALLION ĐÚNG: Bronze->Silver KHÔNG loại bỏ data, KHÔNG impute data, chỉ flag issues
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    lit,
    regexp_replace,
    trim,
    when,
    upper,
    lower,
    split,
    element_at,
    round as spark_round,
    udf,
    length,
    avg,
    count as sql_count,
    percentile_approx,
    stddev,
    min as spark_min,
    max as spark_max,
    regexp_extract,
    concat,
    isnull,
    isnan,
    from_unixtime,
    concat_ws,
    array,
)
from pyspark.sql.types import StringType, DoubleType, BooleanType

import sys
import os
from datetime import datetime
import argparse
import re

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.schema.chotot_schema import get_chotot_processed_schema
from common.utils.date_utils import (
    get_date_format,
    get_hdfs_path,
    generate_processing_id,
)
from common.utils.hdfs_utils import check_hdfs_path_exists, ensure_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


# ===================== DATA QUALITY FLAGGING (MEDALLION ARCHITECTURE) =====================


def flag_data_quality_issues(df: DataFrame) -> DataFrame:
    """
    FLAG data quality issues instead of removing or imputing data (TRUE Medallion Architecture)
    Bronze → Silver: Preserve ALL data including NULLs, only add quality flags
    """
    logger = SparkJobLogger("data_quality_flagging")

    original_count = df.count()
    logger.logger.info(
        f"📊 Original dataset: {original_count:,} records - PRESERVING ALL DATA INCLUDING NULLS"
    )

    # ===== BUSINESS LOGIC FLAGS =====
    logger.logger.info("🏢 Adding business logic flags...")

    # Price business flag
    df_flagged = df.withColumn(
        "price_business_flag",
        when(col("price").isNull(), lit("MISSING_PRICE"))
        .when(col("price") < 30_000_000, lit("PRICE_TOO_LOW"))  # < 30M VND for Chotot
        .when(col("price") > 200_000_000_000, lit("PRICE_TOO_HIGH"))  # > 200B VND
        .otherwise(lit("VALID")),
    )

    # Area business flag
    df_flagged = df_flagged.withColumn(
        "area_business_flag",
        when(col("area").isNull(), lit("MISSING_AREA"))
        .when(col("area") < 5, lit("AREA_TOO_SMALL"))  # < 5m² for land
        .when(col("area") > 10000, lit("AREA_TOO_LARGE"))  # > 10000m²
        .otherwise(lit("VALID")),
    )

    # Price per m2 business flag
    df_flagged = df_flagged.withColumn(
        "price_per_m2_business_flag",
        when(col("price_per_m2").isNull(), lit("MISSING_PRICE_PER_M2"))
        .when(col("price_per_m2") < 200_000, lit("PRICE_M2_TOO_LOW"))  # < 200K/m²
        .when(col("price_per_m2") > 2_000_000_000, lit("PRICE_M2_TOO_HIGH"))  # > 2B/m²
        .otherwise(lit("VALID")),
    )

    # Coordinates flag
    df_flagged = df_flagged.withColumn(
        "coordinates_flag",
        when(
            col("latitude").isNull() | col("longitude").isNull(),
            lit("MISSING_COORDINATES"),
        )
        .when(
            (col("latitude") < 8.0) | (col("latitude") > 23.5),
            lit("INVALID_LATITUDE"),  # Vietnam bounds
        )
        .when(
            (col("longitude") < 102.0) | (col("longitude") > 110.0),
            lit("INVALID_LONGITUDE"),  # Vietnam bounds
        )
        .otherwise(lit("VALID")),
    )

    # Bedroom business flag (allow NULL - no imputation)
    df_flagged = df_flagged.withColumn(
        "bedroom_business_flag",
        when(
            col("bedroom").isNull(),
            lit("MISSING_BEDROOM"),  # Keep as missing, don't impute
        )
        .when(col("bedroom") < 0, lit("BEDROOM_NEGATIVE"))
        .when(col("bedroom") > 20, lit("BEDROOM_TOO_MANY"))
        .otherwise(lit("VALID")),
    )

    # Bathroom business flag (allow NULL - no imputation)
    df_flagged = df_flagged.withColumn(
        "bathroom_business_flag",
        when(
            col("bathroom").isNull(),
            lit("MISSING_BATHROOM"),  # Keep as missing, don't impute
        )
        .when(col("bathroom") < 0, lit("BATHROOM_NEGATIVE"))
        .when(col("bathroom") > 15, lit("BATHROOM_TOO_MANY"))
        .otherwise(lit("VALID")),
    )

    # Dimensions flags (Chotot specific)
    df_flagged = (
        df_flagged.withColumn(
            "length_business_flag",
            when(col("length").isNull(), lit("MISSING_LENGTH"))
            .when(col("length") < 1, lit("LENGTH_TOO_SMALL"))
            .when(col("length") > 1000, lit("LENGTH_TOO_LARGE"))
            .otherwise(lit("VALID")),
        )
        .withColumn(
            "width_business_flag",
            when(col("width").isNull(), lit("MISSING_WIDTH"))
            .when(col("width") < 1, lit("WIDTH_TOO_SMALL"))
            .when(col("width") > 1000, lit("WIDTH_TOO_LARGE"))
            .otherwise(lit("VALID")),
        )
        .withColumn(
            "living_size_business_flag",
            when(col("living_size").isNull(), lit("MISSING_LIVING_SIZE"))
            .when(col("living_size") < 5, lit("LIVING_SIZE_TOO_SMALL"))
            .when(col("living_size") > 5000, lit("LIVING_SIZE_TOO_LARGE"))
            .otherwise(lit("VALID")),
        )
    )

    # ===== STATISTICAL OUTLIER FLAGS (IQR METHOD) =====
    logger.logger.info("📈 Adding statistical outlier flags...")

    # Price outliers (IQR method) - only for non-null values
    price_data = df_flagged.filter(col("price").isNotNull() & (col("price") > 0))
    if price_data.count() > 100:
        price_quartiles = price_data.select(
            percentile_approx("price", 0.25).alias("q1"),
            percentile_approx("price", 0.75).alias("q3"),
        ).collect()[0]

        price_iqr = price_quartiles["q3"] - price_quartiles["q1"]
        price_lower = price_quartiles["q1"] - 2.0 * price_iqr  # 2.0 for less aggressive
        price_upper = price_quartiles["q3"] + 2.0 * price_iqr

        df_flagged = df_flagged.withColumn(
            "price_statistical_flag",
            when(col("price").isNull(), lit("MISSING_PRICE"))
            .when(
                col("price").isNotNull() & (col("price") < price_lower),
                lit("STATISTICAL_OUTLIER_LOW"),
            )
            .when(
                col("price").isNotNull() & (col("price") > price_upper),
                lit("STATISTICAL_OUTLIER_HIGH"),
            )
            .otherwise(lit("NORMAL")),
        )

        logger.logger.info(
            f"📊 Price IQR bounds: {price_lower/1_000_000:.0f}M - {price_upper/1_000_000_000:.1f}B VND"
        )
    else:
        df_flagged = df_flagged.withColumn(
            "price_statistical_flag", lit("INSUFFICIENT_DATA")
        )

    # Area outliers (IQR method) - only for non-null values
    area_data = df_flagged.filter(col("area").isNotNull() & (col("area") > 0))
    if area_data.count() > 100:
        area_quartiles = area_data.select(
            percentile_approx("area", 0.25).alias("q1"),
            percentile_approx("area", 0.75).alias("q3"),
        ).collect()[0]

        area_iqr = area_quartiles["q3"] - area_quartiles["q1"]
        area_lower = area_quartiles["q1"] - 2.0 * area_iqr
        area_upper = area_quartiles["q3"] + 2.0 * area_iqr

        df_flagged = df_flagged.withColumn(
            "area_statistical_flag",
            when(col("area").isNull(), lit("MISSING_AREA"))
            .when(
                col("area").isNotNull() & (col("area") < area_lower),
                lit("STATISTICAL_OUTLIER_LOW"),
            )
            .when(
                col("area").isNotNull() & (col("area") > area_upper),
                lit("STATISTICAL_OUTLIER_HIGH"),
            )
            .otherwise(lit("NORMAL")),
        )

        logger.logger.info(
            f"📊 Area IQR bounds: {area_lower:.0f} - {area_upper:.0f} m²"
        )
    else:
        df_flagged = df_flagged.withColumn(
            "area_statistical_flag", lit("INSUFFICIENT_DATA")
        )

    # ===== RELATIONSHIP-BASED FLAGS =====
    logger.logger.info("🔗 Adding relationship validation flags...")

    df_flagged = df_flagged.withColumn(
        "price_area_relationship_flag",
        when(col("price").isNull() | col("area").isNull(), lit("MISSING_DATA"))
        .when(
            (col("price") / col("area")) < 100_000,
            lit("PRICE_AREA_RATIO_TOO_LOW"),  # < 100K VND/m²
        )
        .when(
            (col("price") / col("area")) > 1_000_000_000,
            lit("PRICE_AREA_RATIO_TOO_HIGH"),  # > 1B VND/m²
        )
        .otherwise(lit("VALID")),
    )

    # ===== COMPREHENSIVE DATA QUALITY SUMMARY =====
    logger.logger.info("📋 Creating comprehensive data quality summary...")

    # Build quality issues array step by step to avoid complex concat_ws operation
    df_flagged = df_flagged.withColumn(
        "quality_issues_array",
        array(
            when(
                col("price_business_flag") != "VALID",
                concat(lit("PRICE: "), col("price_business_flag")),
            ).otherwise(lit("")),
            when(
                col("area_business_flag") != "VALID",
                concat(lit("AREA: "), col("area_business_flag")),
            ).otherwise(lit("")),
            when(
                col("price_per_m2_business_flag") != "VALID",
                concat(lit("PRICE_M2: "), col("price_per_m2_business_flag")),
            ).otherwise(lit("")),
            when(
                col("coordinates_flag") != "VALID",
                concat(lit("COORDS: "), col("coordinates_flag")),
            ).otherwise(lit("")),
            when(
                col("bedroom_business_flag") != "VALID",
                concat(lit("BEDROOM: "), col("bedroom_business_flag")),
            ).otherwise(lit("")),
            when(
                col("bathroom_business_flag") != "VALID",
                concat(lit("BATHROOM: "), col("bathroom_business_flag")),
            ).otherwise(lit("")),
            when(
                col("length_business_flag") != "VALID",
                concat(lit("LENGTH: "), col("length_business_flag")),
            ).otherwise(lit("")),
            when(
                col("width_business_flag") != "VALID",
                concat(lit("WIDTH: "), col("width_business_flag")),
            ).otherwise(lit("")),
            when(
                col("living_size_business_flag") != "VALID",
                concat(lit("LIVING_SIZE: "), col("living_size_business_flag")),
            ).otherwise(lit("")),
            when(
                col("price_statistical_flag").isin(
                    ["STATISTICAL_OUTLIER_LOW", "STATISTICAL_OUTLIER_HIGH"]
                ),
                concat(lit("PRICE_STAT: "), col("price_statistical_flag")),
            ).otherwise(lit("")),
            when(
                col("area_statistical_flag").isin(
                    ["STATISTICAL_OUTLIER_LOW", "STATISTICAL_OUTLIER_HIGH"]
                ),
                concat(lit("AREA_STAT: "), col("area_statistical_flag")),
            ).otherwise(lit("")),
            when(
                col("price_area_relationship_flag") != "VALID",
                concat(lit("PRICE_AREA: "), col("price_area_relationship_flag")),
            ).otherwise(lit("")),
        ),
    )

    # Create the concatenated quality issues string in a simpler way
    df_flagged = df_flagged.withColumn(
        "data_quality_issues_temp", concat_ws("; ", col("quality_issues_array"))
    )

    # Clean up empty quality issues and remove the temporary array column
    df_flagged = df_flagged.withColumn(
        "data_quality_issues",
        when(
            (trim(col("data_quality_issues_temp")) == "")
            | (
                trim(col("data_quality_issues_temp")) == ";;;;;;;;;;;;"
            ),  # All empty concatenated
            lit("NO_ISSUES"),
        ).otherwise(
            regexp_replace(
                col("data_quality_issues_temp"), "^;+|;+$", ""
            )  # Remove leading/trailing semicolons
        ),
    ).drop("quality_issues_array", "data_quality_issues_temp")

    # ===== DATA QUALITY SCORING (NO IMPUTATION IMPACT) =====
    logger.logger.info("🎯 Computing quality scores...")

    # Calculate individual component scores step by step to avoid complex when() chains
    # Price score
    df_flagged = df_flagged.withColumn(
        "price_score",
        when(col("price_business_flag") == "VALID", lit(10))
        .when(col("price_business_flag") == "MISSING_PRICE", lit(0))
        .otherwise(lit(3)),  # Business rule violation but data exists
    )

    # Area score
    df_flagged = df_flagged.withColumn(
        "area_score",
        when(col("area_business_flag") == "VALID", lit(10))
        .when(col("area_business_flag") == "MISSING_AREA", lit(0))
        .otherwise(lit(3)),
    )

    # Coordinates score
    df_flagged = df_flagged.withColumn(
        "coordinates_score",
        when(col("coordinates_flag") == "VALID", lit(10))
        .when(col("coordinates_flag") == "MISSING_COORDINATES", lit(0))
        .otherwise(lit(2)),  # Invalid but present
    )

    # Bedroom score
    df_flagged = df_flagged.withColumn(
        "bedroom_score",
        when(col("bedroom_business_flag") == "VALID", lit(8))
        .when(
            col("bedroom_business_flag") == "MISSING_BEDROOM", lit(0)
        )  # No imputation penalty
        .otherwise(lit(3)),
    )

    # Bathroom score
    df_flagged = df_flagged.withColumn(
        "bathroom_score",
        when(col("bathroom_business_flag") == "VALID", lit(7))
        .when(
            col("bathroom_business_flag") == "MISSING_BATHROOM", lit(0)
        )  # No imputation penalty
        .otherwise(lit(3)),
    )

    # Dimensions score - simplified logic
    df_flagged = (
        df_flagged.withColumn(
            "all_dimensions_valid",
            (col("length_business_flag") == "VALID")
            & (col("width_business_flag") == "VALID")
            & (col("living_size_business_flag") == "VALID"),
        )
        .withColumn(
            "all_dimensions_missing",
            (col("length_business_flag") == "MISSING_LENGTH")
            & (col("width_business_flag") == "MISSING_WIDTH")
            & (col("living_size_business_flag") == "MISSING_LIVING_SIZE"),
        )
        .withColumn(
            "dimensions_score",
            when(col("all_dimensions_valid"), lit(10))
            .when(col("all_dimensions_missing"), lit(0))
            .otherwise(lit(5)),  # Partial data or some invalid
        )
        .drop("all_dimensions_valid", "all_dimensions_missing")
    )

    # Statistical score - simplified logic
    df_flagged = (
        df_flagged.withColumn(
            "price_stat_normal", col("price_statistical_flag") == "NORMAL"
        )
        .withColumn("area_stat_normal", col("area_statistical_flag") == "NORMAL")
        .withColumn(
            "price_stat_missing", col("price_statistical_flag") == "MISSING_PRICE"
        )
        .withColumn("area_stat_missing", col("area_statistical_flag") == "MISSING_AREA")
        .withColumn(
            "price_stat_outlier",
            col("price_statistical_flag").isin(
                ["STATISTICAL_OUTLIER_LOW", "STATISTICAL_OUTLIER_HIGH"]
            ),
        )
        .withColumn(
            "area_stat_outlier",
            col("area_statistical_flag").isin(
                ["STATISTICAL_OUTLIER_LOW", "STATISTICAL_OUTLIER_HIGH"]
            ),
        )
        .withColumn(
            "statistical_score",
            when(col("price_stat_normal") & col("area_stat_normal"), lit(10))
            .when(
                col("price_stat_missing") | col("area_stat_missing"), lit(0)
            )  # Missing key data
            .when(col("price_stat_outlier") | col("area_stat_outlier"), lit(3))
            .otherwise(lit(7)),  # Insufficient data cases
        )
        .drop(
            "price_stat_normal",
            "area_stat_normal",
            "price_stat_missing",
            "area_stat_missing",
            "price_stat_outlier",
            "area_stat_outlier",
        )
    )

    # Relationship score
    df_flagged = df_flagged.withColumn(
        "relationship_score",
        when(col("price_area_relationship_flag") == "VALID", lit(10))
        .when(col("price_area_relationship_flag") == "MISSING_DATA", lit(0))
        .otherwise(lit(2)),
    )

    # Overall quality score (0-100)
    df_flagged = df_flagged.withColumn(
        "data_quality_score",
        col("price_score")
        + col("area_score")
        + col("coordinates_score")
        + col("bedroom_score")
        + col("bathroom_score")
        + col("dimensions_score")
        + col("statistical_score")
        + col("relationship_score"),
    )

    # Quality classification
    df_flagged = df_flagged.withColumn(
        "overall_quality_score",
        when(col("data_quality_score") >= 70, lit("GOOD"))
        .when(col("data_quality_score") >= 45, lit("FAIR"))
        .otherwise(lit("POOR")),
    )

    # Drop intermediate scoring columns
    df_final = df_flagged.drop(
        "price_score",
        "area_score",
        "coordinates_score",
        "bedroom_score",
        "bathroom_score",
        "dimensions_score",
        "statistical_score",
        "relationship_score",
    )

    # ===== LOG QUALITY STATISTICS =====
    # Use simple operations to avoid CodeGenerator compilation issues
    logger.logger.info("📊 Computing data quality statistics...")

    # Cache the dataframe to avoid recomputation
    df_final.cache()

    # Get total count first
    total_records = df_final.count()

    # Calculate quality distribution with simple filters to avoid CodeGen issues
    good_quality_count = df_final.filter(col("overall_quality_score") == "GOOD").count()
    fair_quality_count = df_final.filter(col("overall_quality_score") == "FAIR").count()
    poor_quality_count = df_final.filter(col("overall_quality_score") == "POOR").count()

    logger.logger.info("📊 DATA QUALITY SUMMARY (NO RECORDS REMOVED, NO IMPUTATION):")
    if total_records > 0:
        logger.logger.info(
            f"   GOOD: {good_quality_count:,} records ({(good_quality_count/total_records)*100:.1f}%)"
        )
        logger.logger.info(
            f"   FAIR: {fair_quality_count:,} records ({(fair_quality_count/total_records)*100:.1f}%)"
        )
        logger.logger.info(
            f"   POOR: {poor_quality_count:,} records ({(poor_quality_count/total_records)*100:.1f}%)"
        )

    # Log missing data statistics (preserved for Gold layer decisions)
    # Use simple filter operations to avoid CodeGen compilation issues
    logger.logger.info("📋 Computing missing data statistics...")

    missing_bedroom = df_final.filter(col("bedroom").isNull()).count()
    missing_bathroom = df_final.filter(col("bathroom").isNull()).count()
    missing_price = df_final.filter(col("price").isNull()).count()
    missing_area = df_final.filter(col("area").isNull()).count()
    missing_coordinates = df_final.filter(
        col("latitude").isNull() | col("longitude").isNull()
    ).count()

    logger.logger.info("📋 MISSING DATA PRESERVED FOR GOLD LAYER:")
    logger.logger.info(f"   Missing bedrooms: {missing_bedroom:,}")
    logger.logger.info(f"   Missing bathrooms: {missing_bathroom:,}")
    logger.logger.info(f"   Missing prices: {missing_price:,}")
    logger.logger.info(f"   Missing areas: {missing_area:,}")
    logger.logger.info(f"   Missing coordinates: {missing_coordinates:,}")

    # Calculate data quality issues count
    issue_count = df_final.filter(col("data_quality_issues") != "NO_ISSUES").count()
    no_issues_count = total_records - issue_count

    logger.logger.info(
        f"📋 Records with quality issues: {issue_count:,} ({(issue_count/total_records)*100:.1f}%)"
    )
    logger.logger.info(
        f"✅ Records with no issues: {no_issues_count:,} ({(no_issues_count/total_records)*100:.1f}%)"
    )

    logger.logger.info(
        f"🎯 TRUE MEDALLION ARCHITECTURE: ALL {total_records:,} records preserved with original NULLs intact"
    )

    return df_final


# ===================== MAIN TRANSFORMATION FUNCTION =====================


def transform_chotot_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """
    Transform Chotot data from Bronze to Silver layer with TRUE Medallion Architecture
    NO DATA REMOVAL, NO IMPUTATION - only flagging and standardization
    """
    logger = SparkJobLogger("transform_chotot_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    if input_date is None:
        input_date = get_date_format()

    processing_id = generate_processing_id("chotot_transform")

    # Define paths
    bronze_path = get_hdfs_path(
        "/data/realestate/processed/bronze", "chotot", property_type, input_date
    )
    silver_path = get_hdfs_path(
        "/data/realestate/processed/silver", "chotot", property_type, input_date
    )

    logger.logger.info(f"Reading bronze data from: {bronze_path}")
    logger.logger.info(f"Writing silver data to: {silver_path}")

    ensure_hdfs_path(spark, silver_path)

    try:
        # Read bronze data
        bronze_file = f"{bronze_path}/chotot_{input_date.replace('-', '')}.parquet"
        if not check_hdfs_path_exists(spark, bronze_file):
            error_message = f"Bronze data not found: {bronze_file}"
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        bronze_df = spark.read.parquet(bronze_file)
        logger.log_dataframe_info(bronze_df, "bronze_data")

        # Step 1: Clean and convert numeric columns (preserving nulls)
        logger.logger.info("Step 1: Cleaning numeric data (preserving nulls)...")
        numeric_cleaned_df = (
            bronze_df.withColumn(
                "area",
                when(
                    col("area").isNotNull(),
                    regexp_replace(col("area"), "[^0-9\\.]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "bathroom",
                when(
                    col("bathroom").isNotNull(),
                    regexp_replace(col("bathroom"), "[^0-9]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "bedroom",
                when(
                    col("bedroom").isNotNull(),
                    regexp_replace(col("bedroom"), "[^0-9]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "floor_count",
                when(
                    col("floor_count").isNotNull(),
                    regexp_replace(col("floor_count"), "[^0-9]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "length",
                when(
                    col("length").isNotNull(),
                    regexp_replace(col("length"), "[^0-9\\.]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "width",
                when(
                    col("width").isNotNull(),
                    regexp_replace(col("width"), "[^0-9\\.]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "living_size",
                when(
                    col("living_size").isNotNull(),
                    regexp_replace(col("living_size"), "[^0-9\\.]", "").cast("double"),
                ).otherwise(lit(None)),
            )
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
        )

        # Step 2: Process price fields (preserving nulls)
        logger.logger.info("Step 2: Processing price data (preserving nulls)...")

        # Calculate price_per_m2 only when both price and area are available
        calculated_df = numeric_cleaned_df.withColumn(
            "price_per_m2",
            when(
                col("price_per_m2").isNull()
                & col("price").isNotNull()
                & col("area").isNotNull()
                & (col("area") > 0),
                spark_round(col("price") / col("area"), 2),
            ).otherwise(col("price_per_m2")),
        )

        # Step 3: FLAG data quality issues (NO REMOVAL, NO IMPUTATION)
        logger.logger.info(
            "Step 3: Flagging data quality issues (TRUE Medallion Architecture)..."
        )
        flagged_df = flag_data_quality_issues(calculated_df)

        # Step 4: Convert timestamps
        logger.logger.info("Step 4: Converting timestamps...")
        timestamp_df = flagged_df.withColumn(
            "crawl_timestamp", to_timestamp(col("crawl_timestamp"))
        ).withColumn("posted_date", to_timestamp(col("posted_date")))

        # Step 5: Add metadata ONLY (no data modification)
        logger.logger.info("Step 5: Adding metadata only...")

        # Handle source column
        pre_final_df = timestamp_df
        if (
            "source" not in timestamp_df.columns
            and "data_source" in timestamp_df.columns
        ):
            pre_final_df = timestamp_df.withColumnRenamed("data_source", "source")
        elif "source" not in timestamp_df.columns:
            pre_final_df = timestamp_df.withColumn("source", lit("chotot"))

        # Add final metadata (no quality modification)
        final_df = (
            pre_final_df.withColumn(
                "has_valid_price", col("price").isNotNull() & (col("price") > 0)
            )
            .withColumn("has_valid_area", col("area").isNotNull() & (col("area") > 0))
            .withColumn(
                "has_valid_location",
                col("location").isNotNull() & (length(col("location")) > 5),
            )
            .withColumn(
                "has_coordinates",
                col("latitude").isNotNull() & col("longitude").isNotNull(),
            )
        )

        # Calculate traditional quality score step by step to avoid complex when() chains
        final_df = (
            final_df.withColumn(
                "area_score_trad",
                when(col("has_valid_area"), lit(20)).otherwise(lit(0)),
            )
            .withColumn(
                "price_score_trad",
                when(col("has_valid_price"), lit(20)).otherwise(lit(0)),
            )
            .withColumn(
                "bedroom_score_trad",
                when(
                    col("bedroom").isNotNull() & (col("bedroom") > 0), lit(15)
                ).otherwise(lit(0)),
            )
            .withColumn(
                "bathroom_score_trad",
                when(
                    col("bathroom").isNotNull() & (col("bathroom") > 0), lit(15)
                ).otherwise(lit(0)),
            )
            .withColumn(
                "location_score_trad",
                when(col("has_valid_location"), lit(15)).otherwise(lit(0)),
            )
            .withColumn(
                "coords_score_trad",
                when(col("has_coordinates"), lit(15)).otherwise(lit(0)),
            )
            .withColumn(
                "traditional_quality_score",
                col("area_score_trad")
                + col("price_score_trad")
                + col("bedroom_score_trad")
                + col("bathroom_score_trad")
                + col("location_score_trad")
                + col("coords_score_trad"),
            )
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("processing_id", lit(processing_id))
            .drop(
                "area_score_trad",
                "price_score_trad",
                "bedroom_score_trad",
                "bathroom_score_trad",
                "location_score_trad",
                "coords_score_trad",
            )
        )

        # Drop helper columns
        final_clean_df = final_df.drop(
            "has_valid_price", "has_valid_area", "has_valid_location", "has_coordinates"
        )

        logger.log_dataframe_info(final_clean_df, "silver_data")

        # Step 6: Write ALL data to silver (NO FILTERING)
        output_path = f"{silver_path}/chotot_{input_date.replace('-', '')}.parquet"

        logger.logger.info(
            "🎯 WRITING ALL DATA TO SILVER LAYER (NO RECORDS REMOVED, NO IMPUTATION)"
        )
        final_clean_df.write.mode("overwrite").parquet(output_path)

        final_count = final_clean_df.count()
        logger.logger.info(
            f"✅ Successfully processed ALL {final_count:,} records to {output_path}"
        )

        # Log comprehensive preservation statistics using simple operations
        logger.logger.info("📊 Computing final preservation metrics...")

        # Cache the final dataframe for multiple operations
        final_clean_df.cache()

        # Calculate statistics using simple aggregations to avoid CodeGen issues
        total_count = final_clean_df.count()

        # Calculate quality metrics with simple filters
        good_quality_count = final_clean_df.filter(
            col("data_quality_score") >= 70
        ).count()
        fair_quality_count = final_clean_df.filter(
            col("data_quality_score") >= 45
        ).count()
        no_issues_count = final_clean_df.filter(
            col("data_quality_issues") == "NO_ISSUES"
        ).count()

        # Calculate preserved null counts
        null_bedrooms_count = final_clean_df.filter(col("bedroom").isNull()).count()
        null_bathrooms_count = final_clean_df.filter(col("bathroom").isNull()).count()

        # Calculate average quality scores using simple operations
        avg_quality_row = final_clean_df.select(avg("data_quality_score")).collect()[0]
        avg_quality = avg_quality_row[0] if avg_quality_row[0] is not None else 0.0

        avg_traditional_row = final_clean_df.select(
            avg("traditional_quality_score")
        ).collect()[0]
        avg_traditional_quality = (
            avg_traditional_row[0] if avg_traditional_row[0] is not None else 0.0
        )

        logger.logger.info("📊 FINAL PRESERVATION METRICS:")
        logger.logger.info(f"   Total records processed: {total_count:,}")
        logger.logger.info(f"   Average quality score: {avg_quality:.1f}/100")
        logger.logger.info(
            f"   Traditional quality score: {avg_traditional_quality:.1f}/100"
        )
        logger.logger.info(f"   Good quality (≥70): {good_quality_count:,}")
        logger.logger.info(f"   Fair quality (≥45): {fair_quality_count:,}")
        logger.logger.info(f"   No quality issues: {no_issues_count:,}")
        logger.logger.info(f"   NULL bedrooms preserved: {null_bedrooms_count:,}")
        logger.logger.info(f"   NULL bathrooms preserved: {null_bathrooms_count:,}")

        # Uncache the dataframe
        final_clean_df.unpersist()

        logger.end_job()
        return final_clean_df

    except Exception as e:
        logger.log_error("Error processing data", e)
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Transform Chotot Data to Silver (TRUE Medallion)"
    )
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-type",
        type=str,
        default="house",
        choices=["house", "other"],
        help="Property type",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Initialize Spark session
    spark = create_spark_session("Transform Chotot Data - TRUE Medallion")

    try:
        # Transform data
        transform_chotot_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
