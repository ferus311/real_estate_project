"""
Chuyển đổi dữ liệu Batdongsan
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
)
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


def transform_batdongsan_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """
    Chuyển đổi dữ liệu Batdongsan

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_type: Loại bất động sản ("house" hoặc "other"). Mặc định là "house".

    Returns:
        DataFrame đã được chuyển đổi
    """
    logger = SparkJobLogger("transform_batdongsan_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Tạo ID xử lý
    processing_id = generate_processing_id("batdongsan_transform")

    # Đường dẫn nguồn và đích
    bronze_path = get_hdfs_path(
        "/data/realestate/processed/cleaned", property_type, input_date
    )
    silver_path = get_hdfs_path(
        "/data/realestate/processed/integrated", property_type, input_date
    )

    logger.logger.info(f"Đọc dữ liệu từ: {bronze_path}")
    logger.logger.info(f"Ghi dữ liệu vào: {silver_path}")

    # Đảm bảo đường dẫn đích tồn tại
    ensure_hdfs_path(spark, silver_path)

    try:
        # Đọc dữ liệu bronze
        bronze_file = f"{bronze_path}/batdongsan_{input_date.replace('-', '')}.parquet"
        if not check_hdfs_path_exists(spark, bronze_file):
            error_message = f"Dữ liệu bronze không tồn tại: {bronze_file}"
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        bronze_df = spark.read.parquet(bronze_file)
        logger.log_dataframe_info(bronze_df, "bronze_data")

        # Chuyển đổi các cột số
        transformed_df = (
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

        # Xử lý trường giá
        transformed_df = (
            transformed_df.withColumn("price_text", trim(col("price")))
            .withColumn(
                "price",
                when(
                    lower(col("price_text")).contains("tỷ"),
                    regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")
                    * 1000000000,
                )
                .when(
                    lower(col("price_text")).contains("triệu"),
                    regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")
                    * 1000000,
                )
                .otherwise(
                    regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double")
                ),
            )
            .drop("price_text")
        )

        # Xử lý trường giá/m2
        transformed_df = (
            transformed_df.withColumn("price_per_m2_text", trim(col("price_per_m2")))
            .withColumn(
                "price_per_m2",
                when(
                    lower(col("price_per_m2_text")).contains("tỷ"),
                    regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast(
                        "double"
                    )
                    * 1000000000,
                )
                .when(
                    lower(col("price_per_m2_text")).contains("triệu"),
                    regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast(
                        "double"
                    )
                    * 1000000,
                )
                .otherwise(
                    regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast(
                        "double"
                    )
                ),
            )
            .drop("price_per_m2_text")
        )

        # Chuyển đổi timestamp
        transformed_df = transformed_df.withColumn(
            "crawl_timestamp", to_timestamp(col("crawl_timestamp"))
        ).withColumn("posted_date", to_timestamp(col("posted_date")))

        # Xử lý text fields
        transformed_df = (
            transformed_df.withColumn(
                "house_direction", trim(upper(col("house_direction")))
            )
            .withColumn("legal_status", trim(upper(col("legal_status"))))
            .withColumn("interior", trim(upper(col("interior"))))
            .withColumn("location", trim(col("location")))
        )

        # Tạo hoặc điền giá trị nếu thiếu
        transformed_df = transformed_df.withColumn(
            "price_per_m2",
            when(
                col("price_per_m2").isNull()
                & col("price").isNotNull()
                & col("area").isNotNull(),
                spark_round(col("price") / col("area"), 2),
            ).otherwise(col("price_per_m2")),
        )

        # Lọc bỏ các bản ghi không hợp lệ
        valid_df = transformed_df.filter(
            (col("price").isNotNull() | col("price_per_m2").isNotNull())
            & col("area").isNotNull()
            & col("location").isNotNull()
        )

        # Log thông tin sau chuyển đổi
        logger.log_dataframe_info(valid_df, "transformed_data")

        # Ghi dữ liệu ra
        output_path = os.path.join(
            silver_path, f"batdongsan_{input_date.replace('-', '')}.parquet"
        )
        valid_df.write.mode("overwrite").parquet(output_path)

        logger.logger.info(f"Đã ghi {valid_df.count()} bản ghi vào {output_path}")
        logger.end_job()

        return valid_df

    except Exception as e:
        logger.log_error("Lỗi khi xử lý dữ liệu", e)
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Transform Batdongsan Data")
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

    # Khởi tạo Spark session
    spark = create_spark_session("Transform Batdongsan Data")

    try:
        # Chuyển đổi dữ liệu
        transform_batdongsan_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
