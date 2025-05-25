"""
Trích xuất dữ liệu thô từ Chotot từ HDFS
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit
import sys
import os
from datetime import datetime
import argparse

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.schema.chotot_schema import get_chotot_raw_schema
from common.utils.date_utils import (
    get_date_format,
    get_hdfs_path,
    generate_processing_id,
)
from common.utils.hdfs_utils import check_hdfs_path_exists, ensure_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


def extract_chotot_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """
    Trích xuất dữ liệu thô từ Chotot từ HDFS

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_type: Loại bất động sản ("house" hoặc "other"). Mặc định là "house".

    Returns:
        DataFrame chứa dữ liệu thô
    """
    logger = SparkJobLogger("extract_chotot_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Tạo ID xử lý
    processing_id = generate_processing_id("chotot_extract")

    # Đường dẫn nguồn và đích
    raw_data_path = get_hdfs_path(
        "/data/realestate/raw", "chotot", property_type, input_date
    )
    bronze_path = get_hdfs_path(
        "/data/realestate/processed/bronze", "chotot", property_type, input_date
    )

    logger.logger.info(f"Đọc dữ liệu từ: {raw_data_path}")
    logger.logger.info(f"Ghi dữ liệu vào: {bronze_path}")

    # Kiểm tra đường dẫn nguồn
    if not check_hdfs_path_exists(spark, raw_data_path):
        error_message = f"Đường dẫn dữ liệu thô không tồn tại: {raw_data_path}"
        logger.log_error(error_message)
        raise FileNotFoundError(error_message)

    # Đảm bảo đường dẫn đích tồn tại
    ensure_hdfs_path(spark, bronze_path)

    try:
        # Đọc dữ liệu với schema đã định nghĩa
        schema = get_chotot_raw_schema()
        raw_df = spark.read.schema(schema).json(raw_data_path)

        # Thêm các trường metadata theo common schema
        augmented_df = (
            raw_df.withColumn("processing_date", current_timestamp())
            .withColumn("processing_id", lit(processing_id))
            .withColumn("data_source", lit("chotot.com"))  # Field name consistency
            .withColumn("data_layer", lit("bronze"))
        )

        # Log thông tin cơ bản
        logger.log_dataframe_info(augmented_df, "raw_data")

        # Ghi dữ liệu ra
        output_path = os.path.join(
            bronze_path, f"chotot_{input_date.replace('-', '')}.parquet"
        )
        augmented_df.write.mode("overwrite").parquet(output_path)

        logger.logger.info(f"Đã ghi {augmented_df.count()} bản ghi vào {output_path}")
        logger.end_job()

        return augmented_df

    except Exception as e:
        logger.log_error("Lỗi khi xử lý dữ liệu", e)
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Extract Chotot Data")
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
    spark = create_spark_session("Extract Chotot Data")

    try:
        # Trích xuất dữ liệu
        extract_chotot_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
