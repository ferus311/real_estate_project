"""
Pipeline xử lý dữ liệu hàng ngày
"""

from pyspark.sql import SparkSession
from datetime import datetime
import sys
import os
import argparse

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from jobs.extraction.extract_batdongsan import extract_batdongsan_data
from jobs.extraction.extract_chotot import extract_chotot_data
from jobs.transformation.transform_batdongsan import transform_batdongsan_data
from jobs.transformation.transform_chotot import transform_chotot_data
from jobs.transformation.unify_dataset import unify_property_data
from common.utils.date_utils import get_date_format
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


def run_daily_pipeline(spark: SparkSession, input_date=None, property_types=None):
    """
    Chạy pipeline xử lý dữ liệu hàng ngày

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_types: Danh sách loại bất động sản. Mặc định là None (tức là ["house", "other"]).
    """
    logger = SparkJobLogger("daily_processing_pipeline")

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Xác định loại bất động sản cần xử lý
    if property_types is None:
        property_types = ["house", "other"]
    elif isinstance(property_types, str):
        property_types = [property_types]

    logger.start_job({"input_date": input_date, "property_types": property_types})

    logger.logger.info(
        f"Bắt đầu pipeline xử lý dữ liệu hàng ngày cho ngày: {input_date}"
    )

    try:
        for property_type in property_types:
            logger.logger.info(f"Xử lý loại bất động sản: {property_type}")

            # 1. Trích xuất dữ liệu từ raw
            logger.logger.info("Bước 1: Trích xuất dữ liệu từ raw")
            try:
                batdongsan_raw = extract_batdongsan_data(
                    spark, input_date, property_type
                )
                logger.logger.info(
                    f"Đã trích xuất {batdongsan_raw.count()} bản ghi Batdongsan"
                )
            except Exception as e:
                logger.log_error(f"Lỗi khi trích xuất dữ liệu Batdongsan: {str(e)}")
                batdongsan_raw = None

            try:
                chotot_raw = extract_chotot_data(spark, input_date, property_type)
                logger.logger.info(f"Đã trích xuất {chotot_raw.count()} bản ghi Chotot")
            except Exception as e:
                logger.log_error(f"Lỗi khi trích xuất dữ liệu Chotot: {str(e)}")
                chotot_raw = None

            # 2. Chuyển đổi dữ liệu
            logger.logger.info("Bước 2: Chuyển đổi dữ liệu")
            if batdongsan_raw is not None:
                try:
                    batdongsan_transformed = transform_batdongsan_data(
                        spark, input_date, property_type
                    )
                    logger.logger.info(
                        f"Đã chuyển đổi {batdongsan_transformed.count()} bản ghi Batdongsan"
                    )
                except Exception as e:
                    logger.log_error(f"Lỗi khi chuyển đổi dữ liệu Batdongsan: {str(e)}")

            if chotot_raw is not None:
                try:
                    chotot_transformed = transform_chotot_data(
                        spark, input_date, property_type
                    )
                    logger.logger.info(
                        f"Đã chuyển đổi {chotot_transformed.count()} bản ghi Chotot"
                    )
                except Exception as e:
                    logger.log_error(f"Lỗi khi chuyển đổi dữ liệu Chotot: {str(e)}")

            # 3. Hợp nhất dữ liệu
            logger.logger.info("Bước 3: Hợp nhất dữ liệu")
            try:
                unified_data = unify_property_data(spark, input_date, property_type)
                logger.logger.info(f"Đã hợp nhất {unified_data.count()} bản ghi")
            except Exception as e:
                logger.log_error(f"Lỗi khi hợp nhất dữ liệu: {str(e)}")

        logger.logger.info(
            f"Pipeline xử lý dữ liệu hàng ngày đã hoàn thành cho ngày {input_date}"
        )
        logger.end_job()

    except Exception as e:
        logger.log_error(f"Lỗi trong pipeline xử lý dữ liệu: {str(e)}")
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Run Daily Processing Pipeline")
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-types",
        type=str,
        nargs="+",
        default=["house", "other"],
        choices=["house", "other"],
        help="Property types to process",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Khởi tạo Spark session với cấu hình bổ sung
    config = {
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
        "spark.sql.legacy.timeParserPolicy": "LEGACY",
    }
    spark = create_spark_session("Real Estate Daily Processing Pipeline", config=config)

    try:
        # Chạy pipeline
        run_daily_pipeline(spark, args.date, args.property_types)

    finally:
        spark.stop()
