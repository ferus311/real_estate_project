"""
Kiểm tra chất lượng dữ liệu
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    when,
    isnan,
    isnull,
    mean,
    min,
    max,
    stddev,
    sum as sql_sum,
    lit,
)
import sys
import os
from datetime import datetime
import argparse
import json

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


def validate_property_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> tuple:
    """
    Kiểm tra chất lượng dữ liệu bất động sản

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_type: Loại bất động sản ("house", "other", hoặc "all"). Mặc định là "house".

    Returns:
        tuple: (DataFrame đã được lọc, dict chứa các chỉ số chất lượng dữ liệu)
    """
    logger = SparkJobLogger("validate_property_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Đường dẫn nguồn và đích
    gold_path = get_hdfs_path(
        "/data/realestate/processed/gold", property_type, input_date
    )

    try:
        # Đọc dữ liệu đã hợp nhất
        unified_file = (
            f"{gold_path}/unified_{property_type}_{input_date.replace('-', '')}.parquet"
        )

        if not check_hdfs_path_exists(spark, unified_file):
            error_message = f"Dữ liệu hợp nhất không tồn tại: {unified_file}"
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        unified_df = spark.read.parquet(unified_file)
        logger.log_dataframe_info(unified_df, "input_data")

        # 1. Kiểm tra dữ liệu thiếu và null
        null_counts = {}
        required_fields = ["id", "title", "price", "area", "location", "url"]

        # Đếm số lượng null và NaN cho mỗi cột
        for column in unified_df.columns:
            null_count = unified_df.filter(
                isnull(col(column)) | isnan(col(column))
            ).count()
            null_counts[column] = null_count

        # Lọc dữ liệu theo các trường bắt buộc
        filtered_df = unified_df
        for field in required_fields:
            filtered_df = filtered_df.filter(col(field).isNotNull())

        # 2. Kiểm tra phạm vi giá trị
        outlier_counts = {}

        # Kiểm tra phạm vi giá
        price_stats = filtered_df.select(
            mean("price").alias("price_mean"),
            stddev("price").alias("price_stddev"),
            min("price").alias("price_min"),
            max("price").alias("price_max"),
        ).first()

        price_mean = price_stats["price_mean"]
        price_stddev = price_stats["price_stddev"]
        price_min = price_stats["price_min"]
        price_max = price_stats["price_max"]

        # Xác định outlier dựa trên z-score
        price_low_threshold = (
            price_mean - 3 * price_stddev if price_stddev else price_min
        )
        price_high_threshold = (
            price_mean + 3 * price_stddev if price_stddev else price_max
        )

        price_outliers = filtered_df.filter(
            (col("price") < price_low_threshold) | (col("price") > price_high_threshold)
        ).count()

        outlier_counts["price"] = price_outliers

        # Kiểm tra phạm vi diện tích
        area_stats = filtered_df.select(
            mean("area").alias("area_mean"),
            stddev("area").alias("area_stddev"),
            min("area").alias("area_min"),
            max("area").alias("area_max"),
        ).first()

        area_mean = area_stats["area_mean"]
        area_stddev = area_stats["area_stddev"]
        area_min = area_stats["area_min"]
        area_max = area_stats["area_max"]

        # Xác định outlier dựa trên z-score
        area_low_threshold = area_mean - 3 * area_stddev if area_stddev else area_min
        area_high_threshold = area_mean + 3 * area_stddev if area_stddev else area_max

        area_outliers = filtered_df.filter(
            (col("area") < area_low_threshold) | (col("area") > area_high_threshold)
        ).count()

        outlier_counts["area"] = area_outliers

        # 3. Lọc dữ liệu không hợp lệ
        # Chỉ giữ lại các bản ghi có giá trị hợp lệ cho price và area
        valid_df = filtered_df.filter(
            (col("price") > 0)
            & (col("area") > 0)
            & (col("price") <= price_high_threshold)
            & (col("area") <= area_high_threshold)
        )

        # Tính tỷ lệ dữ liệu hợp lệ
        original_count = unified_df.count()
        valid_count = valid_df.count()
        validity_ratio = valid_count / original_count if original_count > 0 else 0

        # Tổng hợp chỉ số chất lượng dữ liệu
        data_quality_metrics = {
            "original_count": original_count,
            "valid_count": valid_count,
            "validity_ratio": validity_ratio,
            "null_counts": null_counts,
            "outlier_counts": outlier_counts,
            "stats": {
                "price": {
                    "mean": price_mean,
                    "stddev": price_stddev,
                    "min": price_min,
                    "max": price_max,
                    "low_threshold": price_low_threshold,
                    "high_threshold": price_high_threshold,
                },
                "area": {
                    "mean": area_mean,
                    "stddev": area_stddev,
                    "min": area_min,
                    "max": area_max,
                    "low_threshold": area_low_threshold,
                    "high_threshold": area_high_threshold,
                },
            },
        }

        # Log thông tin chất lượng dữ liệu
        logger.logger.info(f"Tỷ lệ dữ liệu hợp lệ: {validity_ratio:.2f}")
        logger.logger.info(f"Số lượng bản ghi ban đầu: {original_count}")
        logger.logger.info(f"Số lượng bản ghi hợp lệ: {valid_count}")

        # Ghi dữ liệu đã lọc
        output_path = f"{gold_path}/validated_{property_type}_{input_date.replace('-', '')}.parquet"
        valid_df.write.mode("overwrite").parquet(output_path)

        # Lưu chỉ số chất lượng dữ liệu
        metrics_path = (
            f"{gold_path}/metrics_{property_type}_{input_date.replace('-', '')}.json"
        )
        metrics_hdfs_path = metrics_path.replace("hdfs://namenode:9870", "")

        with open(metrics_hdfs_path, "w") as f:
            json.dump(data_quality_metrics, f, indent=2)

        logger.logger.info(f"Đã ghi dữ liệu hợp lệ vào {output_path}")
        logger.logger.info(f"Đã ghi chỉ số chất lượng dữ liệu vào {metrics_path}")
        logger.end_job()

        return valid_df, data_quality_metrics

    except Exception as e:
        logger.log_error("Lỗi khi kiểm tra chất lượng dữ liệu", e)
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Validate Property Data")
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-type",
        type=str,
        default="house",
        choices=["house", "other", "all"],
        help="Property type",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Khởi tạo Spark session
    spark = create_spark_session("Validate Property Data")

    try:
        # Kiểm tra chất lượng dữ liệu
        validate_property_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
