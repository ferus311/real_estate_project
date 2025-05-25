"""
Chuyển đổi dữ liệu Chotot với xử lý outliers và smart imputation (Bronze → Silver)
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
    count,
    percentile_approx,
    stddev,
    min as spark_min,
    max as spark_max,
    regexp_extract,
    concat,
    isnull,
    isnan,
    from_unixtime,
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


# ===================== OUTLIER DETECTION AND REMOVAL =====================


def detect_and_remove_outliers(df: DataFrame) -> DataFrame:
    """
    Detect and remove outliers using business rules and statistical methods for Chotot data
    """
    logger = SparkJobLogger("chotot_outlier_detection")

    original_count = df.count()
    logger.logger.info(f"Original dataset: {original_count:,} records")

    # Step 1: Business Logic Outliers (Hard thresholds for Vietnam real estate)
    business_filtered = df.filter(
        # Price reasonable range (30M - 200B VND) - Chotot has more diverse price range
        (
            col("price").isNull()
            | ((col("price") >= 30_000_000) & (col("price") <= 200_000_000_000))
        )
        &
        # Area reasonable range (5 - 10000 m²) - Chotot includes land plots
        (col("area").isNull() | ((col("area") >= 5) & (col("area") <= 10000)))
        &
        # Price per m2 reasonable range (200K - 2B VND/m²) - Wider range for land
        (
            col("price_per_m2").isNull()
            | (
                (col("price_per_m2") >= 200_000)
                & (col("price_per_m2") <= 2_000_000_000)
            )
        )
        &
        # Geographic bounds (Vietnam coordinates)
        (
            col("latitude").isNull()
            | ((col("latitude") >= 8.0) & (col("latitude") <= 23.5))
        )
        & (
            col("longitude").isNull()
            | ((col("longitude") >= 102.0) & (col("longitude") <= 110.0))
        )
        &
        # Bedroom/Bathroom reasonable ranges (allow 0 for land)
        (col("bedroom").isNull() | ((col("bedroom") >= 0) & (col("bedroom") <= 20)))
        & (
            col("bathroom").isNull()
            | ((col("bathroom") >= 0) & (col("bathroom") <= 15))
        )
        &
        # Dimensions reasonable ranges
        (col("length").isNull() | ((col("length") >= 1) & (col("length") <= 1000)))
        & (col("width").isNull() | ((col("width") >= 1) & (col("width") <= 1000)))
        & (
            col("living_size").isNull()
            | ((col("living_size") >= 5) & (col("living_size") <= 5000))
        )
    )

    business_count = business_filtered.count()
    business_removed = original_count - business_count
    logger.logger.info(
        f"After business rules: {business_count:,} records "
        f"(removed {business_removed:,}, {business_removed/original_count*100:.1f}%)"
    )

    # Step 2: Statistical Outliers (IQR method) for key fields
    df_with_flags = business_filtered

    # Price outliers using IQR
    price_data = business_filtered.filter(col("price").isNotNull())
    if price_data.count() > 100:
        price_quartiles = price_data.select(
            percentile_approx("price", 0.25).alias("q1"),
            percentile_approx("price", 0.75).alias("q3"),
        ).collect()[0]

        price_iqr = price_quartiles["q3"] - price_quartiles["q1"]
        price_lower = (
            price_quartiles["q1"] - 2.0 * price_iqr
        )  # Use 2.0 for less aggressive filtering
        price_upper = price_quartiles["q3"] + 2.0 * price_iqr

        df_with_flags = df_with_flags.withColumn(
            "price_outlier",
            when(
                col("price").isNotNull()
                & ((col("price") < price_lower) | (col("price") > price_upper)),
                lit(True),
            ).otherwise(lit(False)),
        )

        logger.logger.info(
            f"Price IQR bounds: {price_lower/1_000_000:.0f}M - {price_upper/1_000_000_000:.1f}B VND"
        )
    else:
        df_with_flags = df_with_flags.withColumn("price_outlier", lit(False))

    # Area outliers using IQR
    area_data = business_filtered.filter(col("area").isNotNull())
    if area_data.count() > 100:
        area_quartiles = area_data.select(
            percentile_approx("area", 0.25).alias("q1"),
            percentile_approx("area", 0.75).alias("q3"),
        ).collect()[0]

        area_iqr = area_quartiles["q3"] - area_quartiles["q1"]
        area_lower = area_quartiles["q1"] - 2.0 * area_iqr
        area_upper = area_quartiles["q3"] + 2.0 * area_iqr

        df_with_flags = df_with_flags.withColumn(
            "area_outlier",
            when(
                col("area").isNotNull()
                & ((col("area") < area_lower) | (col("area") > area_upper)),
                lit(True),
            ).otherwise(lit(False)),
        )

        logger.logger.info(f"Area IQR bounds: {area_lower:.0f} - {area_upper:.0f} m²")
    else:
        df_with_flags = df_with_flags.withColumn("area_outlier", lit(False))

    # Step 3: Relationship-based outliers (price vs area)
    df_with_flags = df_with_flags.withColumn(
        "relationship_outlier",
        when(
            col("price").isNotNull()
            & col("area").isNotNull()
            & (
                (col("price") / col("area")) < 100_000
            ),  # < 100K/m² (too cheap, might be error)
            lit(True),
        )
        .when(
            col("price").isNotNull()
            & col("area").isNotNull()
            & ((col("price") / col("area")) > 1_000_000_000),  # > 1B/m² (too expensive)
            lit(True),
        )
        .otherwise(lit(False)),
    )

    # Flag records that are outliers in multiple dimensions (more conservative)
    df_with_flags = df_with_flags.withColumn(
        "is_outlier",
        (col("price_outlier") & col("area_outlier")) | col("relationship_outlier"),
    )

    # Remove outliers but keep flagged data for analysis
    clean_df = df_with_flags.filter(col("is_outlier") == False).drop(
        "price_outlier", "area_outlier", "relationship_outlier", "is_outlier"
    )

    final_count = clean_df.count()
    total_removed = original_count - final_count
    logger.logger.info(
        f"Final dataset: {final_count:,} records "
        f"(total removed {total_removed:,}, {total_removed/original_count*100:.1f}%)"
    )

    return clean_df


# ===================== SMART IMPUTATION =====================


def smart_impute_bedroom_bathroom(df: DataFrame) -> DataFrame:
    """
    Smart imputation for bedroom and bathroom for Chotot data
    """
    logger = SparkJobLogger("chotot_smart_imputation")

    # Step 1: Extract location information (simpler for Chotot)
    df_with_city = df.withColumn(
        "city_extracted",
        when(
            lower(col("location")).contains("hồ chí minh")
            | lower(col("location")).contains("tp.hcm")
            | lower(col("location")).contains("tphcm")
            | lower(col("location")).contains("hcm")
            | lower(col("location")).contains("sài gòn")
            | lower(col("location")).contains("saigon"),
            lit("Ho Chi Minh"),
        )
        .when(
            lower(col("location")).contains("hà nội")
            | lower(col("location")).contains("hanoi")
            | lower(col("location")).contains("ha noi"),
            lit("Hanoi"),
        )
        .when(
            lower(col("location")).contains("đà nẵng")
            | lower(col("location")).contains("da nang"),
            lit("Da Nang"),
        )
        .when(
            lower(col("location")).contains("cần thơ")
            | lower(col("location")).contains("can tho"),
            lit("Can Tho"),
        )
        .when(
            lower(col("location")).contains("hải phòng")
            | lower(col("location")).contains("hai phong"),
            lit("Hai Phong"),
        )
        .otherwise(lit("Other")),
    )

    # Step 2: Extract bedroom/bathroom from title and description (if available)
    df_extracted = (
        df_with_city.withColumn(
            "title_desc_combined",
            concat(
                when(col("title").isNotNull(), lower(col("title"))).otherwise(lit("")),
                lit(" "),
                when(
                    col("description").isNotNull(), lower(col("description"))
                ).otherwise(lit("")),
            ),
        )
        .withColumn(
            "bedroom_from_text",
            when(
                col("title_desc_combined").rlike(r"(\d+)\s*(phòng\s*ngủ|pn|bedroom)"),
                regexp_extract(
                    col("title_desc_combined"), r"(\d+)\s*(?:phòng\s*ngủ|pn|bedroom)", 1
                ).cast("double"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "bathroom_from_text",
            when(
                col("title_desc_combined").rlike(
                    r"(\d+)\s*(phòng\s*tắm|wc|toilet|bathroom)"
                ),
                regexp_extract(
                    col("title_desc_combined"),
                    r"(\d+)\s*(?:phòng\s*tắm|wc|toilet|bathroom)",
                    1,
                ).cast("double"),
            ).otherwise(lit(None)),
        )
    )

    # Step 3: Estimate from area and living_size
    df_area_based = df_extracted.withColumn(
        "bedroom_from_area",
        when(
            col("living_size").isNotNull() & (col("living_size") > 0),
            # Use living_size if available (more accurate for bedrooms)
            when(col("living_size") <= 25, lit(1))
            .when(col("living_size") <= 45, lit(2))
            .when(col("living_size") <= 70, lit(3))
            .when(col("living_size") <= 100, lit(4))
            .when(col("living_size") <= 150, lit(5))
            .otherwise(lit(6)),
        )
        .when(
            col("area").isNotNull() & (col("area") > 0),
            # Fallback to total area
            when(col("area") <= 30, lit(1))
            .when(col("area") <= 60, lit(2))
            .when(col("area") <= 100, lit(3))
            .when(col("area") <= 150, lit(4))
            .when(col("area") <= 250, lit(5))
            .otherwise(lit(6)),
        )
        .otherwise(lit(None)),
    ).withColumn(
        "bathroom_from_area",
        when(
            col("living_size").isNotNull() & (col("living_size") > 0),
            when(col("living_size") <= 35, lit(1))
            .when(col("living_size") <= 70, lit(2))
            .when(col("living_size") <= 120, lit(3))
            .otherwise(lit(4)),
        )
        .when(
            col("area").isNotNull() & (col("area") > 0),
            when(col("area") <= 50, lit(1))
            .when(col("area") <= 100, lit(2))
            .when(col("area") <= 200, lit(3))
            .otherwise(lit(4)),
        )
        .otherwise(lit(None)),
    )

    # Step 4: Calculate group medians
    df_grouped = df_area_based.withColumn(
        "price_range",
        when(col("price").isNull(), lit("unknown"))
        .when(col("price") < 500_000_000, lit("under_500m"))  # Under 500M
        .when(col("price") < 1_500_000_000, lit("500m_1.5b"))  # 500M-1.5B
        .when(col("price") < 3_000_000_000, lit("1.5b_3b"))  # 1.5B-3B
        .when(col("price") < 7_000_000_000, lit("3b_7b"))  # 3B-7B
        .when(col("price") < 15_000_000_000, lit("7b_15b"))  # 7B-15B
        .otherwise(lit("over_15b")),  # Over 15B
    ).withColumn(
        "area_range",
        when(col("area").isNull(), lit("unknown"))
        .when(col("area") < 30, lit("tiny"))  # < 30m²
        .when(col("area") < 60, lit("small"))  # 30-60m²
        .when(col("area") < 100, lit("medium"))  # 60-100m²
        .when(col("area") < 200, lit("large"))  # 100-200m²
        .when(col("area") < 500, lit("very_large"))  # 200-500m²
        .otherwise(lit("huge")),  # > 500m²
    )

    # Calculate medians by group (handle smaller sample sizes)
    try:
        bedroom_medians = (
            df_grouped.filter(col("bedroom").isNotNull() & (col("bedroom") > 0))
            .groupBy("city_extracted", "price_range", "area_range")
            .agg(
                percentile_approx("bedroom", 0.5).alias("bedroom_median"),
                count("*").alias("bedroom_count"),
            )
            .filter(col("bedroom_count") >= 2)
        )  # Lower threshold for Chotot

        bathroom_medians = (
            df_grouped.filter(col("bathroom").isNotNull() & (col("bathroom") > 0))
            .groupBy("city_extracted", "price_range", "area_range")
            .agg(
                percentile_approx("bathroom", 0.5).alias("bathroom_median"),
                count("*").alias("bathroom_count"),
            )
            .filter(col("bathroom_count") >= 2)
        )
    except:
        bedroom_medians = spark.createDataFrame(
            [],
            "city_extracted string, price_range string, area_range string, bedroom_median double, bedroom_count long",
        )
        bathroom_medians = spark.createDataFrame(
            [],
            "city_extracted string, price_range string, area_range string, bathroom_median double, bathroom_count long",
        )

    # Join medians
    df_with_medians = df_grouped.join(
        bedroom_medians, ["city_extracted", "price_range", "area_range"], "left"
    ).join(bathroom_medians, ["city_extracted", "price_range", "area_range"], "left")

    # Overall medians as fallback
    try:
        overall_bedroom_median = (
            df_with_medians.filter(col("bedroom").isNotNull() & (col("bedroom") > 0))
            .select(percentile_approx("bedroom", 0.5))
            .collect()[0][0]
            or 2.0
        )
        overall_bathroom_median = (
            df_with_medians.filter(col("bathroom").isNotNull() & (col("bathroom") > 0))
            .select(percentile_approx("bathroom", 0.5))
            .collect()[0][0]
            or 1.0
        )
    except:
        overall_bedroom_median = 2.0
        overall_bathroom_median = 1.0

    # Step 5: Apply smart filling logic with priority
    df_imputed = df_with_medians.withColumn(
        "bedroom_imputed",
        when((col("bedroom").isNotNull()) & (col("bedroom") > 0), col("bedroom"))
        .when(
            col("bedroom_from_text").isNotNull()
            & (col("bedroom_from_text") >= 1)
            & (col("bedroom_from_text") <= 15),
            col("bedroom_from_text"),
        )
        .when(col("bedroom_median").isNotNull(), col("bedroom_median"))
        .when(col("bedroom_from_area").isNotNull(), col("bedroom_from_area"))
        .otherwise(lit(overall_bedroom_median)),
    ).withColumn(
        "bathroom_imputed",
        when((col("bathroom").isNotNull()) & (col("bathroom") > 0), col("bathroom"))
        .when(
            col("bathroom_from_text").isNotNull()
            & (col("bathroom_from_text") >= 1)
            & (col("bathroom_from_text") <= 8),
            col("bathroom_from_text"),
        )
        .when(col("bathroom_median").isNotNull(), col("bathroom_median"))
        .when(col("bathroom_from_area").isNotNull(), col("bathroom_from_area"))
        .otherwise(lit(overall_bathroom_median)),
    )

    # Step 6: Validation rules (allow 0 for land plots)
    df_validated = df_imputed.withColumn(
        "bathroom_final",
        when(
            col("bathroom_imputed") > (col("bedroom_imputed") + 2),
            col("bedroom_imputed"),
        )  # More lenient
        .when(col("bathroom_imputed") < 0, lit(0))
        .when(col("bathroom_imputed") > 10, lit(4))
        .otherwise(col("bathroom_imputed")),
    ).withColumn(
        "bedroom_final",
        when((col("area") < 25) & (col("bedroom_imputed") > 2), lit(1))
        .when((col("area") < 40) & (col("bedroom_imputed") > 3), lit(2))
        .when(col("bedroom_imputed") < 0, lit(0))  # Allow 0 for land
        .when(col("bedroom_imputed") > 15, lit(6))
        .otherwise(col("bedroom_imputed")),
    )

    # Clean up intermediate columns
    result_df = (
        df_validated.drop(
            "title_desc_combined",
            "bedroom_from_text",
            "bathroom_from_text",
            "bedroom_from_area",
            "bathroom_from_area",
            "price_range",
            "area_range",
            "bedroom_median",
            "bathroom_median",
            "bedroom_count",
            "bathroom_count",
            "bedroom_imputed",
            "bathroom_imputed",
            "city_extracted",
        )
        .withColumn("bedroom", col("bedroom_final"))
        .withColumn("bathroom", col("bathroom_final"))
        .drop("bedroom_final", "bathroom_final")
    )

    # Log imputation results
    original_bedroom_nulls = df.filter(
        (col("bedroom").isNull()) | (col("bedroom") == 0)
    ).count()
    original_bathroom_nulls = df.filter(
        (col("bathroom").isNull()) | (col("bathroom") == 0)
    ).count()
    final_bedroom_nulls = result_df.filter(
        (col("bedroom").isNull()) | (col("bedroom") == 0)
    ).count()
    final_bathroom_nulls = result_df.filter(
        (col("bathroom").isNull()) | (col("bathroom") == 0)
    ).count()

    logger.logger.info(
        f"Bedroom imputation: {original_bedroom_nulls} -> {final_bedroom_nulls} null/zero values"
    )
    logger.logger.info(
        f"Bathroom imputation: {original_bathroom_nulls} -> {final_bathroom_nulls} null/zero values"
    )

    return result_df


# ===================== MAIN TRANSFORMATION FUNCTION =====================


def transform_chotot_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """
    Transform Chotot data from Bronze to Silver layer
    """
    logger = SparkJobLogger("transform_chotot_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    if input_date is None:
        input_date = get_date_format()

    processing_id = generate_processing_id("chotot_transform")

    # Define paths
    bronze_path = get_hdfs_path(
        "/data/realestate/processed/cleaned", property_type, input_date
    )
    silver_path = get_hdfs_path(
        "/data/realestate/processed/integrated", property_type, input_date
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

        # Step 1: Clean and convert numeric columns
        logger.logger.info("Step 1: Cleaning numeric data...")
        numeric_cleaned_df = (
            bronze_df.withColumn(
                "area", regexp_replace(col("area"), "[^0-9\\.]", "").cast("double")
            )
            .withColumn(
                "bathroom", regexp_replace(col("bathroom"), "[^0-9]", "").cast("double")
            )
            .withColumn(
                "bedroom", regexp_replace(col("bedroom"), "[^0-9]", "").cast("double")
            )
            .withColumn(
                "floor_count",
                regexp_replace(col("floor_count"), "[^0-9]", "").cast("double"),
            )
            .withColumn(
                "length", regexp_replace(col("length"), "[^0-9\\.]", "").cast("double")
            )
            .withColumn(
                "width", regexp_replace(col("width"), "[^0-9\\.]", "").cast("double")
            )
            .withColumn(
                "living_size",
                regexp_replace(col("living_size"), "[^0-9\\.]", "").cast("double"),
            )
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
        )

        # Step 2: Process price fields
        logger.logger.info("Step 2: Processing price data...")
        price_processed_df = (
            numeric_cleaned_df.withColumn("price_text", trim(col("price")))
            .withColumn(
                "price",
                when(
                    lower(col("price_text")).contains("tỷ")
                    | lower(col("price_text")).contains("ty"),
                    regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")
                    * 1_000_000_000,
                )
                .when(
                    lower(col("price_text")).contains("triệu")
                    | lower(col("price_text")).contains("trieu"),
                    regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")
                    * 1_000_000,
                )
                .otherwise(
                    regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")
                ),
            )
            .withColumn("price_per_m2_text", trim(col("price_per_m2")))
            .withColumn(
                "price_per_m2",
                # Chotot price_per_m2 is typically in millions VND
                regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast("double")
                * 1_000_000,
            )
            .drop("price_text", "price_per_m2_text")
        )

        # Step 3: Process timestamps
        logger.logger.info("Step 3: Processing timestamps...")
        timestamp_df = price_processed_df.withColumn(
            "crawl_timestamp", from_unixtime(col("crawl_timestamp")).cast("timestamp")
        ).withColumn("posted_date", from_unixtime(col("posted_date")).cast("timestamp"))

        # Step 4: Clean text fields
        text_cleaned_df = (
            timestamp_df.withColumn("location", trim(col("location")))
            .withColumn("title", trim(col("title")))
            .withColumn("description", trim(col("description")))
        )

        # Step 5: Calculate missing price_per_m2
        calculated_df = text_cleaned_df.withColumn(
            "price_per_m2",
            when(
                col("price_per_m2").isNull()
                & col("price").isNotNull()
                & col("area").isNotNull()
                & (col("area") > 0),
                spark_round(col("price") / col("area"), 2),
            ).otherwise(col("price_per_m2")),
        )

        # Step 6: Remove outliers
        logger.logger.info("Step 6: Removing outliers...")
        outlier_removed_df = detect_and_remove_outliers(calculated_df)

        # Step 7: Smart imputation for bedroom/bathroom
        logger.logger.info("Step 7: Smart imputation for bedroom/bathroom...")
        imputed_df = smart_impute_bedroom_bathroom(outlier_removed_df)

        # Step 8: Drop seller_info if exists (low analytical value)
        columns_to_drop = []
        if "seller_info" in imputed_df.columns:
            columns_to_drop.append("seller_info")
        if "seller_name" in imputed_df.columns and "seller_id" in imputed_df.columns:
            # Keep seller_id but drop seller_name for privacy
            columns_to_drop.append("seller_name")

        if columns_to_drop:
            imputed_df = imputed_df.drop(*columns_to_drop)

        # Step 9: Add validation flags and metadata
        logger.logger.info("Step 9: Adding validation and metadata...")

        # First, rename data_source to source for consistency with common schema
        final_df = imputed_df
        if "data_source" in imputed_df.columns:
            final_df = final_df.withColumnRenamed("data_source", "source")

        # Add missing fields that are expected by common schema
        final_df = final_df.withColumn(
            "facade_width", lit(None).cast(DoubleType())  # Batdongsan specific
        ).withColumn(
            "road_width", lit(None).cast(DoubleType())  # Batdongsan specific
        )

        final_df = (
            final_df.withColumn(
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
            .withColumn(
                "is_house_type",
                col("bedroom").isNotNull()
                & (col("bedroom") > 0),  # Distinguish from land
            )
            .withColumn(
                "is_land_type",
                (col("bedroom").isNull() | (col("bedroom") == 0))
                & (col("bathroom").isNull() | (col("bathroom") == 0))
                & col("area").isNotNull()
                & (col("area") > 0),
            )
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("processing_id", lit(processing_id))
        )

        # Step 10: Quality scoring (adapted for Chotot)
        quality_df = final_df.withColumn(
            "data_quality_score",
            (when(col("has_valid_area"), lit(25)).otherwise(lit(0)))
            + (when(col("has_valid_price"), lit(25)).otherwise(lit(0)))
            + (
                when(
                    col("bedroom").isNotNull() | col("is_land_type"), lit(15)
                ).otherwise(lit(0))
            )
            + (
                when(
                    col("bathroom").isNotNull() | col("is_land_type"), lit(15)
                ).otherwise(lit(0))
            )
            + (when(col("has_valid_location"), lit(10)).otherwise(lit(0)))
            + (when(col("has_coordinates"), lit(10)).otherwise(lit(0))),
        )

        # Step 11: Filter for minimum quality requirements
        final_filtered_df = quality_df.filter(
            col("has_valid_area")
            & col("has_valid_price")
            & col("has_valid_location")
            & (col("data_quality_score") >= 50)  # Lower threshold for Chotot
        ).drop(
            "has_valid_price",
            "has_valid_area",
            "has_valid_location",
            "has_coordinates",
            "is_house_type",
            "is_land_type",
        )

        logger.log_dataframe_info(final_filtered_df, "silver_data")

        # Step 12: Write silver data
        output_path = os.path.join(
            silver_path, f"chotot_{input_date.replace('-', '')}.parquet"
        )

        final_filtered_df.write.mode("overwrite").parquet(output_path)

        final_count = final_filtered_df.count()
        logger.logger.info(
            f"Successfully processed {final_count:,} silver records to {output_path}"
        )

        # Log quality statistics
        quality_stats = final_filtered_df.select(
            avg("data_quality_score").alias("avg_quality"),
            count(when(col("data_quality_score") >= 70, True)).alias(
                "high_quality_count"
            ),
            count(when(col("data_quality_score") >= 85, True)).alias(
                "premium_quality_count"
            ),
        ).collect()[0]

        logger.logger.info(
            f"Quality metrics - Average: {quality_stats['avg_quality']:.1f}, "
            f"High quality (≥70): {quality_stats['high_quality_count']:,}, "
            f"Premium quality (≥85): {quality_stats['premium_quality_count']:,}"
        )

        logger.end_job()
        return final_filtered_df

    except Exception as e:
        logger.log_error("Error processing data", e)
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Transform Chotot Data to Silver")
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
    spark = create_spark_session("Transform Chotot Data")

    try:
        # Transform data
        transform_chotot_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
