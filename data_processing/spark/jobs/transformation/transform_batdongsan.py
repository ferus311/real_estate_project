"""
Chuyển đổi dữ liệu Batdongsan với xử lý outliers và smart imputation (Bronze → Silver)
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

from common.schema.batdongsan_schema import get_batdongsan_processed_schema
from common.utils.date_utils import (
    get_date_format,
    get_hdfs_path,
    generate_processing_id,
)
from common.utils.hdfs_utils import check_hdfs_path_exists, ensure_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


# ===================== MAPPING FUNCTIONS =====================


def map_house_direction(value):
    """Map house direction to standardized values"""
    if value is None or value == "":
        return "UNKNOWN"

    value_normalized = value.lower().replace(" ", "").replace("-", "")

    direction_mapping = {
        "dong": "EAST",
        "đông": "EAST",
        "tay": "WEST",
        "tây": "WEST",
        "nam": "SOUTH",
        "bac": "NORTH",
        "bắc": "NORTH",
        "dongnam": "SOUTHEAST",
        "đôngnam": "SOUTHEAST",
        "namdong": "SOUTHEAST",
        "namđông": "SOUTHEAST",
        "dongbac": "NORTHEAST",
        "đôngbắc": "NORTHEAST",
        "bacdong": "NORTHEAST",
        "bắcđông": "NORTHEAST",
        "taynam": "SOUTHWEST",
        "tâynam": "SOUTHWEST",
        "namtay": "SOUTHWEST",
        "namtây": "SOUTHWEST",
        "taybac": "NORTHWEST",
        "tâybắc": "NORTHWEST",
        "bactay": "NORTHWEST",
        "bắctây": "NORTHWEST",
    }

    return direction_mapping.get(value_normalized, "UNKNOWN")


def map_interior_status(value):
    """Map interior status to standardized categories"""
    if value is None or value == "":
        return "UNKNOWN"

    value_lower = value.lower()

    luxury_keywords = [
        "cao cấp",
        "caocấp",
        "luxury",
        "sang trọng",
        "sangtrọng",
        "xịn",
        "5*",
        "5 sao",
        "nhập khẩu",
        "nhậpkhẩu",
    ]
    if any(keyword in value_lower for keyword in luxury_keywords):
        return "LUXURY"

    furnished_keywords = [
        "đầy đủ",
        "đầyđủ",
        "full",
        "hoàn thiện",
        "hoànthiện",
        "trang bị",
        "trangbị",
        "nội thất",
        "nộithất",
    ]
    if any(keyword in value_lower for keyword in furnished_keywords):
        return "FULLY_FURNISHED"

    basic_keywords = ["cơ bản", "cơbản", "bình thường", "bìnhthường", "chuẩn"]
    if any(keyword in value_lower for keyword in basic_keywords):
        return "BASIC"

    unfurnished_keywords = ["thô", "trống", "không", "k ", "nt", "nhà thô", "nhàthô"]
    if any(keyword in value_lower for keyword in unfurnished_keywords):
        return "UNFURNISHED"

    return "UNKNOWN"


def map_legal_status(value):
    """Map legal status to standardized categories"""
    if value is None or value == "":
        return "UNKNOWN"

    value_normalized = (
        value.lower()
        .replace(" ", "")
        .replace("-", "")
        .replace(".", "")
        .replace("/", "")
        .replace("\\", "")
        .replace(",", "")
    )

    no_legal_keywords = ["khôngpháplý", "khongphapły", "không", "khong"]
    if any(keyword in value_normalized for keyword in no_legal_keywords):
        return "NO_LEGAL"

    land_use_keywords = ["thổcư", "thocu", "cnqsdđ", "cnqsdd"]
    if any(keyword in value_normalized for keyword in land_use_keywords):
        return "LAND_USE_CERTIFICATE"

    red_book_keywords = ["sổđỏ", "sodo", "sổhồng", "sohong", "sđcc"]
    if any(keyword in value_normalized for keyword in red_book_keywords):
        return "RED_BOOK"

    ownership_keywords = ["shcc", "shr", "ccqsh", "chứngnhận", "chungnhan"]
    if any(keyword in value_normalized for keyword in ownership_keywords):
        return "OWNERSHIP_CERTIFICATE"

    return "UNKNOWN"


# ===================== OUTLIER DETECTION =====================


def detect_and_remove_outliers(df: DataFrame) -> DataFrame:
    """Detect and remove outliers using business rules and statistical methods"""
    logger = SparkJobLogger("outlier_detection")

    original_count = df.count()
    logger.logger.info(f"Original dataset: {original_count:,} records")

    # Business Logic Outliers
    business_filtered = df.filter(
        (
            col("price").isNull()
            | ((col("price") >= 50_000_000) & (col("price") <= 100_000_000_000))
        )
        & (col("area").isNull() | ((col("area") >= 10) & (col("area") <= 2000)))
        & (
            col("price_per_m2").isNull()
            | (
                (col("price_per_m2") >= 500_000)
                & (col("price_per_m2") <= 1_000_000_000)
            )
        )
        & (
            col("latitude").isNull()
            | ((col("latitude") >= 8.0) & (col("latitude") <= 23.5))
        )
        & (
            col("longitude").isNull()
            | ((col("longitude") >= 102.0) & (col("longitude") <= 110.0))
        )
        & (col("bedroom").isNull() | ((col("bedroom") >= 1) & (col("bedroom") <= 15)))
        & (
            col("bathroom").isNull()
            | ((col("bathroom") >= 1) & (col("bathroom") <= 10))
        )
    )

    business_count = business_filtered.count()
    logger.logger.info(f"After business rules: {business_count:,} records")

    # Statistical Outliers (IQR method)
    df_with_flags = business_filtered

    # Price outliers
    price_data = business_filtered.filter(col("price").isNotNull())
    if price_data.count() > 100:
        price_quartiles = price_data.select(
            percentile_approx("price", 0.25).alias("q1"),
            percentile_approx("price", 0.75).alias("q3"),
        ).collect()[0]

        price_iqr = price_quartiles["q3"] - price_quartiles["q1"]
        price_lower = price_quartiles["q1"] - 1.5 * price_iqr
        price_upper = price_quartiles["q3"] + 1.5 * price_iqr

        df_with_flags = df_with_flags.withColumn(
            "price_outlier",
            when(
                col("price").isNotNull()
                & ((col("price") < price_lower) | (col("price") > price_upper)),
                lit(True),
            ).otherwise(lit(False)),
        )
    else:
        df_with_flags = df_with_flags.withColumn("price_outlier", lit(False))

    # Area outliers
    area_data = business_filtered.filter(col("area").isNotNull())
    if area_data.count() > 100:
        area_quartiles = area_data.select(
            percentile_approx("area", 0.25).alias("q1"),
            percentile_approx("area", 0.75).alias("q3"),
        ).collect()[0]

        area_iqr = area_quartiles["q3"] - area_quartiles["q1"]
        area_lower = area_quartiles["q1"] - 1.5 * area_iqr
        area_upper = area_quartiles["q3"] + 1.5 * area_iqr

        df_with_flags = df_with_flags.withColumn(
            "area_outlier",
            when(
                col("area").isNotNull()
                & ((col("area") < area_lower) | (col("area") > area_upper)),
                lit(True),
            ).otherwise(lit(False)),
        )
    else:
        df_with_flags = df_with_flags.withColumn("area_outlier", lit(False))

    # Relationship-based outliers
    df_with_flags = df_with_flags.withColumn(
        "relationship_outlier",
        when(
            col("price").isNotNull()
            & col("area").isNotNull()
            & ((col("price") / col("area")) < 1_000_000),
            lit(True),
        )
        .when(
            col("price").isNotNull()
            & col("area").isNotNull()
            & ((col("price") / col("area")) > 500_000_000),
            lit(True),
        )
        .otherwise(lit(False)),
    )

    # Remove multi-dimensional outliers
    df_with_flags = df_with_flags.withColumn(
        "is_outlier",
        (col("price_outlier") & col("area_outlier")) | col("relationship_outlier"),
    )

    clean_df = df_with_flags.filter(col("is_outlier") == False).drop(
        "price_outlier", "area_outlier", "relationship_outlier", "is_outlier"
    )

    final_count = clean_df.count()
    total_removed = original_count - final_count
    logger.logger.info(
        f"Final dataset: {final_count:,} records "
        f"(removed {total_removed:,}, {total_removed/original_count*100:.1f}%)"
    )

    return clean_df


# ===================== SMART IMPUTATION =====================


def smart_impute_bedroom_bathroom(df: DataFrame) -> DataFrame:
    """Smart imputation for bedroom and bathroom using multiple strategies"""
    logger = SparkJobLogger("smart_imputation")

    # Extract city information
    df_with_city = df.withColumn(
        "city_extracted",
        when(
            lower(col("location")).contains("hồ chí minh")
            | lower(col("location")).contains("tp.hcm")
            | lower(col("location")).contains("tphcm")
            | lower(col("location")).contains("hcm"),
            lit("Ho Chi Minh"),
        )
        .when(
            lower(col("location")).contains("hà nội")
            | lower(col("location")).contains("hanoi"),
            lit("Hanoi"),
        )
        .otherwise(lit("Other")),
    )

    # Extract from text
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

    # Estimate from area
    df_area_based = df_extracted.withColumn(
        "bedroom_from_area",
        when(
            col("area").isNotNull(),
            when(col("area") <= 30, lit(1))
            .when(col("area") <= 50, lit(2))
            .when(col("area") <= 80, lit(3))
            .when(col("area") <= 120, lit(4))
            .when(col("area") <= 200, lit(5))
            .otherwise(lit(6)),
        ).otherwise(lit(None)),
    ).withColumn(
        "bathroom_from_area",
        when(
            col("area").isNotNull(),
            when(col("area") <= 40, lit(1))
            .when(col("area") <= 80, lit(2))
            .when(col("area") <= 150, lit(3))
            .otherwise(lit(4)),
        ).otherwise(lit(None)),
    )

    # Group-based medians
    df_grouped = df_area_based.withColumn(
        "price_range",
        when(col("price").isNull(), lit("unknown"))
        .when(col("price") < 1_000_000_000, lit("under_1b"))
        .when(col("price") < 3_000_000_000, lit("1b_3b"))
        .when(col("price") < 5_000_000_000, lit("3b_5b"))
        .when(col("price") < 10_000_000_000, lit("5b_10b"))
        .otherwise(lit("over_10b")),
    ).withColumn(
        "area_range",
        when(col("area").isNull(), lit("unknown"))
        .when(col("area") < 50, lit("small"))
        .when(col("area") < 100, lit("medium"))
        .when(col("area") < 200, lit("large"))
        .otherwise(lit("very_large")),
    )

    # Calculate medians
    try:
        bedroom_medians = (
            df_grouped.filter(col("bedroom").isNotNull())
            .groupBy("city_extracted", "price_range", "area_range")
            .agg(percentile_approx("bedroom", 0.5).alias("bedroom_median"))
            .filter(col("bedroom_median").isNotNull())
        )

        bathroom_medians = (
            df_grouped.filter(col("bathroom").isNotNull())
            .groupBy("city_extracted", "price_range", "area_range")
            .agg(percentile_approx("bathroom", 0.5).alias("bathroom_median"))
            .filter(col("bathroom_median").isNotNull())
        )
    except:
        bedroom_medians = spark.createDataFrame(
            [],
            "city_extracted string, price_range string, area_range string, bedroom_median double",
        )
        bathroom_medians = spark.createDataFrame(
            [],
            "city_extracted string, price_range string, area_range string, bathroom_median double",
        )

    # Join medians
    df_with_medians = df_grouped.join(
        bedroom_medians, ["city_extracted", "price_range", "area_range"], "left"
    ).join(bathroom_medians, ["city_extracted", "price_range", "area_range"], "left")

    # Overall medians as fallback
    try:
        overall_bedroom_median = (
            df_with_medians.filter(col("bedroom").isNotNull())
            .select(percentile_approx("bedroom", 0.5))
            .collect()[0][0]
            or 2.0
        )
        overall_bathroom_median = (
            df_with_medians.filter(col("bathroom").isNotNull())
            .select(percentile_approx("bathroom", 0.5))
            .collect()[0][0]
            or 1.0
        )
    except:
        overall_bedroom_median = 2.0
        overall_bathroom_median = 1.0

    # Apply smart filling logic
    df_imputed = df_with_medians.withColumn(
        "bedroom_final",
        when(col("bedroom").isNotNull(), col("bedroom"))
        .when(
            col("bedroom_from_text").isNotNull()
            & (col("bedroom_from_text") >= 1)
            & (col("bedroom_from_text") <= 10),
            col("bedroom_from_text"),
        )
        .when(col("bedroom_median").isNotNull(), col("bedroom_median"))
        .when(col("bedroom_from_area").isNotNull(), col("bedroom_from_area"))
        .otherwise(lit(overall_bedroom_median)),
    ).withColumn(
        "bathroom_final",
        when(col("bathroom").isNotNull(), col("bathroom"))
        .when(
            col("bathroom_from_text").isNotNull()
            & (col("bathroom_from_text") >= 1)
            & (col("bathroom_from_text") <= 5),
            col("bathroom_from_text"),
        )
        .when(col("bathroom_median").isNotNull(), col("bathroom_median"))
        .when(col("bathroom_from_area").isNotNull(), col("bathroom_from_area"))
        .otherwise(lit(overall_bathroom_median)),
    )

    # Validation rules
    df_validated = df_imputed.withColumn(
        "bathroom_final",
        when(col("bathroom_final") > (col("bedroom_final") + 1), col("bedroom_final"))
        .when(col("bathroom_final") < 1, lit(1))
        .when(col("bathroom_final") > 6, lit(4))
        .otherwise(col("bathroom_final")),
    ).withColumn(
        "bedroom_final",
        when((col("area") < 30) & (col("bedroom_final") > 2), lit(1))
        .when((col("area") < 50) & (col("bedroom_final") > 3), lit(2))
        .when(col("bedroom_final") < 1, lit(1))
        .when(col("bedroom_final") > 10, lit(6))
        .otherwise(col("bedroom_final")),
    )

    # Clean up
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
            "city_extracted",
        )
        .withColumn("bedroom", col("bedroom_final"))
        .withColumn("bathroom", col("bathroom_final"))
        .drop("bedroom_final", "bathroom_final")
    )

    # Log results
    original_bedroom_nulls = df.filter(col("bedroom").isNull()).count()
    original_bathroom_nulls = df.filter(col("bathroom").isNull()).count()
    final_bedroom_nulls = result_df.filter(col("bedroom").isNull()).count()
    final_bathroom_nulls = result_df.filter(col("bathroom").isNull()).count()

    logger.logger.info(
        f"Bedroom imputation: {original_bedroom_nulls} -> {final_bedroom_nulls} nulls"
    )
    logger.logger.info(
        f"Bathroom imputation: {original_bathroom_nulls} -> {final_bathroom_nulls} nulls"
    )

    return result_df


# ===================== MAIN TRANSFORMATION =====================


def transform_batdongsan_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """Transform Batdongsan data from Bronze to Silver layer"""
    logger = SparkJobLogger("transform_batdongsan_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    if input_date is None:
        input_date = get_date_format()

    processing_id = generate_processing_id("batdongsan_transform")

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
        bronze_file = f"{bronze_path}/batdongsan_{input_date.replace('-', '')}.parquet"
        if not check_hdfs_path_exists(spark, bronze_file):
            error_message = f"Bronze data not found: {bronze_file}"
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        bronze_df = spark.read.parquet(bronze_file)
        logger.log_dataframe_info(bronze_df, "bronze_data")

        # Create UDFs for mapping functions
        map_direction_udf = udf(map_house_direction, StringType())
        map_interior_udf = udf(map_interior_status, StringType())
        map_legal_udf = udf(map_legal_status, StringType())

        # Step 1: Data type conversions
        logger.logger.info("Step 1: Converting data types...")
        numeric_df = (
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
                "facade_width",
                regexp_replace(col("facade_width"), "[^0-9\\.]", "").cast("double"),
            )
            .withColumn(
                "road_width",
                regexp_replace(col("road_width"), "[^0-9\\.]", "").cast("double"),
            )
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))
        )

        # Step 2: Price processing
        logger.logger.info("Step 2: Processing price data...")
        price_df = (
            numeric_df.withColumn("price_text", trim(col("price")))
            .withColumn(
                "is_negotiable",
                when(
                    lower(col("price_text")).contains("thỏa thuận")
                    | lower(col("price_text")).contains("thoathuan"),
                    lit(True),
                ).otherwise(lit(False)),
            )
            .withColumn(
                "price",
                when(col("is_negotiable"), lit(None))
                .when(
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
                when(
                    lower(col("price_per_m2_text")).contains("tỷ"),
                    regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast(
                        "double"
                    )
                    * 1_000_000_000,
                )
                .when(
                    lower(col("price_per_m2_text")).contains("triệu"),
                    regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast(
                        "double"
                    )
                    * 1_000_000,
                )
                .otherwise(
                    regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast(
                        "double"
                    )
                ),
            )
            .drop("price_text", "price_per_m2_text")
        )

        # Step 3: Categorical mappings
        logger.logger.info("Step 3: Applying categorical mappings...")
        mapped_df = (
            price_df.withColumn(
                "house_direction", map_direction_udf(col("house_direction"))
            )
            .withColumn("interior", map_interior_udf(col("interior")))
            .withColumn("legal_status", map_legal_udf(col("legal_status")))
        )

        # Step 4: Calculate missing price_per_m2
        calculated_df = mapped_df.withColumn(
            "price_per_m2",
            when(
                col("price_per_m2").isNull()
                & col("price").isNotNull()
                & col("area").isNotNull()
                & (col("area") > 0),
                spark_round(col("price") / col("area"), 2),
            ).otherwise(col("price_per_m2")),
        )

        # Step 5: Remove outliers
        logger.logger.info("Step 5: Removing outliers...")
        outlier_removed_df = detect_and_remove_outliers(calculated_df)

        # Step 6: Smart imputation
        logger.logger.info("Step 6: Smart imputation...")
        imputed_df = smart_impute_bedroom_bathroom(outlier_removed_df)

        # Step 7: Convert timestamps
        timestamp_df = imputed_df.withColumn(
            "crawl_timestamp", to_timestamp(col("crawl_timestamp"))
        ).withColumn("posted_date", to_timestamp(col("posted_date")))

        # Step 8: Add metadata and quality scoring
        logger.logger.info("Step 8: Adding quality scoring...")

        # First, rename data_source to source for consistency with common schema
        pre_final_df = timestamp_df
        if "data_source" in timestamp_df.columns:
            pre_final_df = timestamp_df.withColumnRenamed("data_source", "source")

        # Add missing fields that are expected by common schema
        pre_final_df = (
            pre_final_df.withColumn(
                "width", lit(None).cast(DoubleType())  # Chotot specific
            )
            .withColumn("length", lit(None).cast(DoubleType()))  # Chotot specific
            .withColumn("living_size", lit(None).cast(DoubleType()))  # Chotot specific
        )

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
            .withColumn(
                "data_quality_score",
                (when(col("has_valid_area"), lit(20)).otherwise(lit(0)))
                + (when(col("has_valid_price"), lit(20)).otherwise(lit(0)))
                + (
                    when(
                        col("bedroom").isNotNull() & (col("bedroom") > 0), lit(15)
                    ).otherwise(lit(0))
                )
                + (
                    when(
                        col("bathroom").isNotNull() & (col("bathroom") > 0), lit(15)
                    ).otherwise(lit(0))
                )
                + (when(col("has_valid_location"), lit(15)).otherwise(lit(0)))
                + (when(col("has_coordinates"), lit(15)).otherwise(lit(0))),
            )
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("processing_id", lit(processing_id))
        )

        # Step 9: Filter for quality
        final_filtered_df = final_df.filter(
            col("has_valid_area")
            & (col("has_valid_price") | col("price_per_m2").isNotNull())
            & col("has_valid_location")
            & (col("data_quality_score") >= 60)
        ).drop(
            "has_valid_price", "has_valid_area", "has_valid_location", "has_coordinates"
        )

        logger.log_dataframe_info(final_filtered_df, "silver_data")

        # Step 10: Write silver data
        output_path = os.path.join(
            silver_path, f"batdongsan_{input_date.replace('-', '')}.parquet"
        )

        final_filtered_df.write.mode("overwrite").parquet(output_path)

        final_count = final_filtered_df.count()
        logger.logger.info(
            f"Successfully processed {final_count:,} silver records to {output_path}"
        )

        # Log quality statistics
        quality_stats = final_filtered_df.select(
            avg("data_quality_score").alias("avg_quality"),
            count(when(col("data_quality_score") >= 80, True)).alias(
                "high_quality_count"
            ),
            count(when(col("data_quality_score") >= 90, True)).alias(
                "premium_quality_count"
            ),
        ).collect()[0]

        logger.logger.info(
            f"Quality metrics - Average: {quality_stats['avg_quality']:.1f}, "
            f"High quality (≥80): {quality_stats['high_quality_count']:,}, "
            f"Premium quality (≥90): {quality_stats['premium_quality_count']:,}"
        )

        logger.end_job()
        return final_filtered_df

    except Exception as e:
        logger.log_error("Error processing data", e)
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Transform Batdongsan Data to Silver")
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
    spark = create_spark_session("Transform Batdongsan Data")

    try:
        # Transform data
        transform_batdongsan_data(spark, args.date, args.property_type)

    finally:
        spark.stop()

if __name__ == "__main__":
    args = parse_args()

    # Initialize Spark session
    spark = create_spark_session("Transform Batdongsan Data")

    try:
        # Transform data
        transform_batdongsan_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
