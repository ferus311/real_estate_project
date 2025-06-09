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
            bronze_df
            # Process price with proper unit handling
            .withColumn(
                "price_normalized",
                when(
                    col("price").isNotNull(),
                    # First remove dots (thousand separators), then replace comma with period for decimal separator
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                col("price"), "\\.", ""
                            ),  # Remove dots (thousand separators)
                            ",",
                            ".",  # Convert comma to decimal point
                        ),
                        "[^0-9\\.]",
                        "",  # Remove all non-numeric except decimal point
                    ),
                ).otherwise(lit(None)),
            )
            .withColumn(
                "price",
                when(col("price").isNull(), lit(None))
                .when(
                    lower(col("price")).contains("tỷ")
                    | lower(col("price")).contains("ty"),
                    col("price_normalized").cast("double") * 1_000_000_000,
                )
                .when(
                    lower(col("price")).contains("triệu")
                    | lower(col("price")).contains("trieu"),
                    col("price_normalized").cast("double") * 1_000_000,
                )
                .otherwise(col("price_normalized").cast("double")),
            )
            # Check for invalid units
            .withColumn(
                "has_invalid_unit",
                when(
                    lower(col("price")).contains("nghìn")
                    | lower(col("price")).contains("nghin")
                    | lower(col("price")).contains("k ")
                    | lower(col("price")).rlike("\\d+k$"),  # số + k ở cuối
                    lit(True),
                ).otherwise(lit(False)),
            )
            # Process price_per_m2 - Chotot already has numeric values, just multiply by 1 million
            .withColumn(
                "price_per_m2",
                when(col("price_per_m2").isNull(), lit(None)).otherwise(
                    col("price_per_m2").cast("double") * 1_000_000
                ),  # Convert to VND (multiply by 1 million)
            )
            # Drop temporary normalized columns
            .drop("price_normalized")
            .withColumn(
                "area",
                when(
                    col("area").isNotNull(),
                    # For Chotot: Only remove non-numeric characters, no decimal separator conversion
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

        # Step 3: Skip quality flagging - will be done in unify step
        logger.logger.info(
            "Step 3: Skipping quality scoring - will be done in unify step..."
        )
        clean_df = calculated_df

        # Step 4: Convert timestamps
        logger.logger.info("Step 4: Converting timestamps...")
        timestamp_df = clean_df.withColumn(
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

        # Add final metadata only (no quality scoring)
        final_df = pre_final_df.withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn("processing_id", lit(processing_id))

        # Simple filter: only remove invalid units (quality scoring will be done in unify step)
        final_clean_df = final_df.filter(
            ~col("has_invalid_unit")  # Filter out records with "nghìn" units
        ).drop("has_invalid_unit")

        logger.log_dataframe_info(final_clean_df, "silver_data")

        # Step 6: Write filtered data to silver (REMOVED RECORDS WITH INVALID UNITS)
        output_path = f"{silver_path}/chotot_{input_date.replace('-', '')}.parquet"

        logger.logger.info(
            "🎯 WRITING FILTERED DATA TO SILVER LAYER (REMOVED RECORDS WITH INVALID UNITS)"
        )
        final_clean_df.write.mode("overwrite").parquet(output_path)

        final_count = final_clean_df.count()
        logger.logger.info(
            f"✅ Successfully processed {final_count:,} records to {output_path} (filtered out invalid units)"
        )

        # Uncache and finish
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
