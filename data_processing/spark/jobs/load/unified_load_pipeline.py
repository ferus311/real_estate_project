"""
Unified Load Stage - Load dữ liệu vào multiple targets (Delta Lake & PostgreSQL)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when
import sys
import os
from datetime import datetime
import argparse

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.utils.date_utils import get_date_format, get_hdfs_path
from common.utils.hdfs_utils import check_hdfs_path_exists, ensure_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session

# Import serving logic từ load_to_serving.py
from jobs.load.load_to_serving import (
    extract_from_gold,
    transform_for_serving,
    load_to_postgres,
)


def unified_load_pipeline(
    spark: SparkSession,
    input_date=None,
    property_type="house",
    load_targets=None,  # ["delta", "postgres", "both"]
    postgres_config=None,
) -> None:
    """
    Unified load pipeline - load vào multiple targets

    Args:
        spark: SparkSession
        input_date: Ngày xử lý
        property_type: Loại bất động sản
        load_targets: List targets to load ["delta", "postgres", "both"]
        postgres_config: PostgreSQL configuration
    """
    logger = SparkJobLogger("unified_load_pipeline")
    logger.start_job(
        {
            "input_date": input_date,
            "property_type": property_type,
            "load_targets": load_targets,
        }
    )

    if load_targets is None:
        load_targets = ["both"]

    if input_date is None:
        input_date = get_date_format()

    try:
        # 1. Extract từ Gold layer
        gold_df = extract_from_gold_layer(spark, input_date, property_type, logger)

        # 2. Load vào các targets được chỉ định
        if "delta" in load_targets or "both" in load_targets:
            logger.logger.info("🔄 Loading to Delta Lake...")
            load_to_delta_lake(spark, gold_df, property_type, logger)

        if "postgres" in load_targets or "both" in load_targets:
            logger.logger.info("🔄 Loading to PostgreSQL using serving logic...")
            # Sử dụng serving transformation và loading logic từ load_to_serving.py
            serving_df = transform_for_serving(gold_df, logger)
            load_to_postgres(serving_df, postgres_config, logger)

        logger.logger.info("✅ Unified load pipeline completed!")
        logger.end_job()

    except Exception as e:
        logger.log_error("❌ Unified load pipeline failed", e)
        raise


def extract_from_gold_layer(
    spark: SparkSession, input_date: str, property_type: str, logger
):
    """Extract từ Gold layer - delegating to shared logic"""
    return extract_from_gold(spark, input_date, property_type, logger)


def load_to_delta_lake(
    spark: SparkSession, gold_df: DataFrame, property_type: str, logger
):
    """Load vào Delta Lake (ML/Analytics purpose)"""

    # Transform cho Delta Lake (giữ nguyên logic cũ)
    delta_df = (
        gold_df.withColumn("update_timestamp", current_timestamp())
        .withColumn("data_version", lit("v1"))
        .withColumn("property_type", lit(property_type))
    )

    delta_path = f"/data/realestate/ml/features/property_data"
    ensure_hdfs_path(spark, delta_path)

    # Ghi vào Delta Lake
    delta_df.write.format("delta").mode("append").partitionBy(
        "property_type", "province"
    ).save(delta_path)

    logger.logger.info(f"✅ Loaded {delta_df.count():,} records to Delta Lake")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Unified Load Pipeline")
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-type", type=str, default="house", choices=["house", "other", "all"]
    )
    parser.add_argument(
        "--targets",
        type=str,
        nargs="+",
        default=["both"],
        choices=["delta", "postgres", "both"],
        help="Load targets",
    )
    parser.add_argument(
        "--postgres-host", type=str, default=os.getenv("POSTGRES_HOST", "localhost")
    )
    parser.add_argument(
        "--postgres-port", type=str, default=os.getenv("POSTGRES_PORT", "5432")
    )
    parser.add_argument(
        "--postgres-db", type=str, default=os.getenv("POSTGRES_DB", "realestate")
    )
    parser.add_argument(
        "--postgres-user", type=str, default=os.getenv("POSTGRES_USER", "postgres")
    )
    parser.add_argument(
        "--postgres-password",
        type=str,
        default=os.getenv("POSTGRES_PASSWORD", "password"),
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    postgres_config = {
        "url": f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}",
        "user": args.postgres_user,
        "password": args.postgres_password,
        "driver": "org.postgresql.Driver",
    }

    # Spark session với cả Delta và PostgreSQL support
    spark = create_spark_session(
        "Unified Load Pipeline",
        config={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.7.0.jar",
        },
    )

    try:
        unified_load_pipeline(
            spark, args.date, args.property_type, args.targets, postgres_config
        )
        print("✅ Unified load completed!")

    finally:
        spark.stop()
