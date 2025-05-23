"""
Lưu dữ liệu vào Delta Lake
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
import sys
import os
from datetime import datetime
import argparse

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.utils.date_utils import (
    get_date_format,
    get_hdfs_path,
    generate_processing_id,
)
from common.utils.hdfs_utils import check_hdfs_path_exists, ensure_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


def load_to_delta_lake(
    spark: SparkSession,
    input_date=None,
    property_type="house",
    table_name="property_data",
) -> None:
    """
    Lưu dữ liệu vào Delta Lake

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_type: Loại bất động sản ("house", "other", hoặc "all"). Mặc định là "house".
        table_name: Tên bảng Delta Lake. Mặc định là "property_data".
    """
    logger = SparkJobLogger("load_to_delta_lake")
    logger.start_job(
        {
            "input_date": input_date,
            "property_type": property_type,
            "table_name": table_name,
        }
    )

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Đường dẫn nguồn và đích
    gold_path = get_hdfs_path(
        "/data/realestate/processed/analytics", property_type, input_date
    )
    delta_path = f"/data/realestate/ml/features/{table_name}"

    # Đảm bảo đường dẫn đích tồn tại
    ensure_hdfs_path(spark, delta_path)

    try:
        # Đọc dữ liệu đã được làm giàu
        enriched_file = f"{gold_path}/enriched_{property_type}_{input_date.replace('-', '')}.parquet"

        if not check_hdfs_path_exists(spark, enriched_file):
            error_message = f"Dữ liệu đã làm giàu không tồn tại: {enriched_file}"
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        enriched_df = spark.read.parquet(enriched_file)
        logger.log_dataframe_info(enriched_df, "input_data")

        # Thêm thông tin phiên bản và thời gian cập nhật
        final_df = (
            enriched_df.withColumn("update_timestamp", current_timestamp())
            .withColumn("processing_date", lit(input_date))
            .withColumn("data_version", lit("v1"))
            .withColumn("property_type", lit(property_type))
        )

        # Kiểm tra xem bảng Delta đã tồn tại chưa
        is_delta_exists = check_hdfs_path_exists(spark, delta_path)

        # Ghi dữ liệu vào Delta Lake
        if is_delta_exists:
            # Cập nhật dữ liệu mới vào bảng hiện có
            # Sử dụng merge để cập nhật hoặc insert dữ liệu mới
            delta_table = spark.read.format("delta").load(delta_path)

            # Đăng ký view tạm thời
            final_df.createOrReplaceTempView("updates")
            delta_table.createOrReplaceTempView("target")

            # Thực hiện MERGE
            spark.sql(
                f"""
                MERGE INTO target t
                USING updates s
                ON t.id = s.id AND t.source = s.source
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
            )

            logger.logger.info(f"Đã cập nhật dữ liệu vào bảng Delta: {delta_path}")
        else:
            # Tạo bảng mới
            final_df.write.format("delta").mode("overwrite").partitionBy(
                "property_type", "province"
            ).save(delta_path)

            # Tạo bảng Delta trong metastore
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING DELTA
                LOCATION '{delta_path}'
            """
            )

            logger.logger.info(f"Đã tạo bảng Delta mới: {delta_path}")

        # Kiểm tra kết quả
        final_count = spark.read.format("delta").load(delta_path).count()
        logger.logger.info(f"Tổng số bản ghi trong bảng Delta: {final_count}")

        # Tối ưu hóa bảng Delta
        spark.sql(f"OPTIMIZE {table_name}")
        logger.logger.info(f"Đã tối ưu hóa bảng Delta: {table_name}")

        logger.end_job()

    except Exception as e:
        logger.log_error("Lỗi khi lưu dữ liệu vào Delta Lake", e)
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Load Data to Delta Lake")
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-type",
        type=str,
        default="house",
        choices=["house", "other", "all"],
        help="Property type",
    )
    parser.add_argument(
        "--table-name", type=str, default="property_data", help="Delta Lake table name"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Khởi tạo Spark session với Delta Lake
    spark = create_spark_session(
        "Load Data to Delta Lake",
        config={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    )

    try:
        # Lưu dữ liệu
        load_to_delta_lake(spark, args.date, args.property_type, args.table_name)

    finally:
        spark.stop()
