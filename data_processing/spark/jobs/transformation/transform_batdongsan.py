"""
Chuyển đổi và chuẩn hóa dữ liệu Batdongsan (Bronze → Silver)
Focus: Data type conversion, standardization, and normalization only
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
    length,
    regexp_extract,
    concat,
    avg,
    count,
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


# ===================== PRICE PROCESSING =====================


def process_price_data(df: DataFrame) -> DataFrame:
    """Process and standardize price information"""
    logger = SparkJobLogger("price_processing")

    # Process main price field
    df_price = df.withColumn("price_text", trim(col("price")))

    # Check for negotiable prices
    df_price = df_price.withColumn(
        "is_negotiable",
        when(
            lower(col("price_text")).contains("thỏa thuận")
            | lower(col("price_text")).contains("thoathuan"),
            lit(True),
        ).otherwise(lit(False)),
    )

    # Convert price based on unit (tỷ, triệu, etc.)
    df_price = df_price.withColumn(
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
        .otherwise(regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")),
    )

    # Process price per m2 field
    df_price = df_price.withColumn("price_per_m2_text", trim(col("price_per_m2")))

    df_price = df_price.withColumn(
        "price_per_m2",
        when(
            lower(col("price_per_m2_text")).contains("tỷ"),
            regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast("double")
            * 1_000_000_000,
        )
        .when(
            lower(col("price_per_m2_text")).contains("triệu"),
            regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast("double")
            * 1_000_000,
        )
        .otherwise(
            regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast("double")
        ),
    )

    # Calculate price per m2 if both price and area are available and price_per_m2 is null
    df_price = df_price.withColumn(
        "price_per_m2",
        when(
            col("price_per_m2").isNull()
            & col("price").isNotNull()
            & col("area").isNotNull()
            & (col("area") > 0),
            spark_round(col("price") / col("area"), 2),
        ).otherwise(col("price_per_m2")),
    )

    logger.logger.info("Price processing completed")
    return df_price.drop("price_text", "price_per_m2_text")


# ===================== COORDINATE VALIDATION =====================


def validate_coordinates(df: DataFrame) -> DataFrame:
    """Validate and clean coordinate data for Vietnam region"""
    logger = SparkJobLogger("coordinate_validation")

    # Valid coordinates for Vietnam: Lat 8.0-23.5, Lon 102.0-110.0
    df_validated = df.withColumn(
        "latitude",
        when(
            (col("latitude") >= 8.0) & (col("latitude") <= 23.5), col("latitude")
        ).otherwise(lit(None)),
    ).withColumn(
        "longitude",
        when(
            (col("longitude") >= 102.0) & (col("longitude") <= 110.0), col("longitude")
        ).otherwise(lit(None)),
    )

    logger.logger.info("Coordinate validation completed")
    return df_validated


# ===================== DATA TYPE CONVERSION =====================


def convert_data_types(df: DataFrame) -> DataFrame:
    """Convert and clean data types for standardization"""
    logger = SparkJobLogger("data_type_conversion")

    # Convert numeric fields with proper cleaning
    numeric_df = (
        df.withColumn(
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

    logger.logger.info("Data type conversion completed")
    return numeric_df


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
        "/data/realestate/processed/bronze", "batdongsan", property_type, input_date
    )
    silver_path = get_hdfs_path(
        "/data/realestate/processed/silver", "batdongsan", property_type, input_date
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

        # Step 1: Data type conversions
        logger.logger.info("Step 1: Converting data types...")
        numeric_df = convert_data_types(bronze_df)

        # Cache to break lineage and prevent code generation issues
        numeric_df.cache()
        logger.logger.info(f"Cached numeric conversion: {numeric_df.count():,} records")

        # Step 2: Price processing
        logger.logger.info("Step 2: Processing price data...")
        price_df = process_price_data(numeric_df)

        # Step 3: Categorical mappings - Using direct SQL expressions instead of UDFs
        logger.logger.info("Step 3: Applying categorical mappings...")

        # Direction mapping with CASE WHEN
        mapped_df = price_df.withColumn(
            "house_direction",
            when(
                (
                    lower(col("house_direction")).contains("dong")
                    | lower(col("house_direction")).contains("đông")
                ),
                lit("EAST"),
            )
            .when(
                (
                    lower(col("house_direction")).contains("tay")
                    | lower(col("house_direction")).contains("tây")
                ),
                lit("WEST"),
            )
            .when(lower(col("house_direction")).contains("nam"), lit("SOUTH"))
            .when(
                (
                    lower(col("house_direction")).contains("bac")
                    | lower(col("house_direction")).contains("bắc")
                ),
                lit("NORTH"),
            )
            .when(
                (
                    lower(col("house_direction")).contains("dongnam")
                    | lower(col("house_direction")).contains("đôngnam")
                ),
                lit("SOUTHEAST"),
            )
            .when(
                (
                    lower(col("house_direction")).contains("dongbac")
                    | lower(col("house_direction")).contains("đôngbắc")
                ),
                lit("NORTHEAST"),
            )
            .when(
                (
                    lower(col("house_direction")).contains("taynam")
                    | lower(col("house_direction")).contains("tâynam")
                ),
                lit("SOUTHWEST"),
            )
            .when(
                (
                    lower(col("house_direction")).contains("taybac")
                    | lower(col("house_direction")).contains("tâybắc")
                ),
                lit("NORTHWEST"),
            )
            .otherwise(lit("UNKNOWN")),
        )

        # Interior mapping with CASE WHEN
        mapped_df = mapped_df.withColumn(
            "interior",
            when(
                (
                    lower(col("interior")).contains("cao cấp")
                    | lower(col("interior")).contains("luxury")
                    | lower(col("interior")).contains("sang trọng")
                ),
                lit("LUXURY"),
            )
            .when(
                (
                    lower(col("interior")).contains("đầy đủ")
                    | lower(col("interior")).contains("full")
                    | lower(col("interior")).contains("nội thất")
                ),
                lit("FULLY_FURNISHED"),
            )
            .when(
                (
                    lower(col("interior")).contains("cơ bản")
                    | lower(col("interior")).contains("bình thường")
                ),
                lit("BASIC"),
            )
            .when(
                (
                    lower(col("interior")).contains("thô")
                    | lower(col("interior")).contains("trống")
                    | lower(col("interior")).contains("không")
                ),
                lit("UNFURNISHED"),
            )
            .otherwise(lit("UNKNOWN")),
        )

        # Legal status mapping with CASE WHEN
        mapped_df = mapped_df.withColumn(
            "legal_status",
            when(
                (
                    lower(col("legal_status")).contains("không")
                    | lower(col("legal_status")).contains("khong")
                ),
                lit("NO_LEGAL"),
            )
            .when(
                (
                    lower(col("legal_status")).contains("thổ cư")
                    | lower(col("legal_status")).contains("thổcư")
                    | lower(col("legal_status")).contains("cnqsd")
                ),
                lit("LAND_USE_CERTIFICATE"),
            )
            .when(
                (
                    lower(col("legal_status")).contains("sổ đỏ")
                    | lower(col("legal_status")).contains("sổhồng")
                    | lower(col("legal_status")).contains("sđcc")
                ),
                lit("RED_BOOK"),
            )
            .when(
                (
                    lower(col("legal_status")).contains("shcc")
                    | lower(col("legal_status")).contains("shr")
                    | lower(col("legal_status")).contains("chứng nhận")
                ),
                lit("OWNERSHIP_CERTIFICATE"),
            )
            .otherwise(lit("UNKNOWN")),
        )

        # Cache after categorical mapping to break lineage
        mapped_df.cache()
        logger.logger.info(f"Cached categorical mapping: {mapped_df.count():,} records")

        # Step 4: Apply coordinate validation
        logger.logger.info("Step 4: Validating coordinates...")
        validated_df = validate_coordinates(mapped_df)

        # Step 5: Convert timestamps
        timestamp_df = validated_df.withColumn(
            "crawl_timestamp", to_timestamp(col("crawl_timestamp"))
        ).withColumn("posted_date", to_timestamp(col("posted_date")))

        # Step 6: Add metadata and quality scoring
        logger.logger.info("Step 6: Adding quality scoring...")

        # Handle source column - either use existing source or rename data_source to source
        pre_final_df = timestamp_df
        if (
            "source" not in timestamp_df.columns
            and "data_source" in timestamp_df.columns
        ):
            pre_final_df = timestamp_df.withColumnRenamed("data_source", "source")
        elif "source" not in timestamp_df.columns:
            # If neither exists, add source column with default value
            pre_final_df = timestamp_df.withColumn("source", lit("batdongsan"))

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

        # Step 7: Filter for quality
        final_filtered_df = final_df.filter(
            col("has_valid_area")
            & (col("has_valid_price") | col("price_per_m2").isNotNull())
            & col("has_valid_location")
            & (col("data_quality_score") >= 50)  # Lowered threshold since no imputation
        ).drop(
            "has_valid_price", "has_valid_area", "has_valid_location", "has_coordinates"
        )

        logger.log_dataframe_info(final_filtered_df, "silver_data")

        # Step 8: Write silver data
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
