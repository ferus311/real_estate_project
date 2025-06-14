"""
Load dữ liệu vào Serving Layer (PostgreSQL) cho Web API
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    when,
    coalesce,
    row_number,
    desc,
)
from pyspark.sql.window import Window
import sys
import os
from datetime import datetime
import argparse
from dotenv import load_dotenv

load_dotenv()

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.utils.date_utils import get_date_format, get_hdfs_path
from common.utils.hdfs_utils import check_hdfs_path_exists
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session
from common.utils.duplicate_detection import apply_load_deduplication


def load_to_serving_layer(
    spark: SparkSession,
    input_date=None,
    property_type="house",
    postgres_config=None,
) -> None:
    """
    Load dữ liệu từ Gold layer vào PostgreSQL serving layer

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD"
        property_type: Loại bất động sản ("house", "other", "all")
        postgres_config: Cấu hình PostgreSQL connection
    """
    logger = SparkJobLogger("load_to_serving_layer")
    logger.start_job(
        {
            "input_date": input_date,
            "property_type": property_type,
        }
    )

    if input_date is None:
        input_date = get_date_format()

    if postgres_config is None:
        postgres_config = {
            "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'realestate')}",
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver",
        }

    try:
        # 1. Extract từ Gold layer
        gold_df = extract_from_gold(spark, input_date, property_type, logger)

        # 2. Transform cho serving needs
        serving_df = transform_for_serving(gold_df, logger)

        # 3. Apply deduplication
        initial_count = serving_df.count()
        if initial_count == 0:
            logger.logger.info("📊 No data to process")
            logger.end_job()
            return

        logger.logger.info(f"🔍 Applying deduplication to {initial_count:,} records...")
        deduplicated_df = apply_load_deduplication(serving_df, postgres_config)
        final_count = deduplicated_df.count()

        logger.logger.info(
            f"📊 Deduplication result: {final_count:,}/{initial_count:,} records"
        )

        # 4. Load vào PostgreSQL
        if final_count > 0:
            load_to_postgres(deduplicated_df, postgres_config, logger)

        logger.logger.info("✅ Load to serving completed!")
        logger.end_job()

    except Exception as e:
        logger.log_error("❌ Load to serving layer failed", e)
        raise


def extract_from_gold(spark: SparkSession, input_date: str, property_type: str, logger):
    """Extract dữ liệu từ Gold layer"""

    if property_type == "all":
        property_types = ["house", "other"]
    else:
        property_types = [property_type]

    all_dfs = []

    for ptype in property_types:
        date_formatted = input_date.replace("-", "/")
        gold_path = f"/data/realestate/processed/gold/unified/{ptype}/{date_formatted}/unified_*.parquet"

        try:
            df = spark.read.parquet(gold_path)
            logger.logger.info(
                f"📊 Loaded {df.count():,} records from Gold layer for {ptype}"
            )
            all_dfs.append(df)
        except Exception as e:
            logger.logger.warning(f"⚠️ No Gold data found for {ptype}: {e}")

    if not all_dfs:
        raise FileNotFoundError(f"No Gold data found for date {input_date}")

    # Union all dataframes
    combined_df = all_dfs[0]
    for df in all_dfs[1:]:
        combined_df = combined_df.union(df)

    return combined_df


def transform_for_serving(gold_df: DataFrame, logger):
    """Transform dữ liệu từ Gold layer cho PostgreSQL serving layer"""

    logger.logger.info("🔄 Transforming data for serving layer...")

    # Complete mapping từ Gold schema sang PostgreSQL schema
    serving_df = gold_df.select(
        # Primary identifiers
        col("id"),
        col("url"),
        col("source"),
        # Basic property information
        col("title"),
        col("description"),
        col("location"),
        col("data_type"),
        # Location information
        col("province"),
        col("district"),
        col("ward"),
        col("street"),
        # Location IDs
        col("province_id").cast("int"),
        col("district_id").cast("int"),
        col("ward_id").cast("int"),
        col("street_id").cast("int"),
        # Geographic coordinates
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        # Core metrics
        col("price").cast("bigint"),
        col("area").cast("double"),
        col("price_per_m2").cast("bigint"),
        # Property details
        col("bedroom").cast("double"),
        col("bathroom").cast("double"),
        col("floor_count").cast("int"),
        # Dimensions
        col("width").cast("double"),
        col("length").cast("double"),
        col("living_size").cast("double"),
        col("facade_width").cast("double"),
        col("road_width").cast("double"),
        # Property characteristics
        col("house_direction"),
        col("house_direction_code").cast("int"),
        col("legal_status"),
        col("legal_status_code").cast("int"),
        col("interior"),
        col("interior_code").cast("int"),
        col("house_type"),
        col("house_type_code").cast("int"),
        # Timestamps
        col("posted_date").cast("timestamp"),
        col("crawl_timestamp").cast("timestamp"),
        col("processing_timestamp").cast("timestamp"),
        # Data quality
        col("data_quality_score").cast("double"),
        col("processing_id"),
        # Serving metadata
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    ).filter(
        col("id").isNotNull()
        & col("title").isNotNull()
        & col("source").isNotNull()
        & (col("price").isNull() | (col("price") >= 0))
        & (col("area").isNull() | (col("area") > 0))
        & (
            (col("latitude").isNull() & col("longitude").isNull())
            | (col("latitude").between(-90, 90) & col("longitude").between(-180, 180))
        )
    )

    # Dedup by ID with data quality score
    window_spec = Window.partitionBy("id").orderBy(
        desc("data_quality_score"), desc("processing_timestamp"), desc("updated_at")
    )

    serving_df = (
        serving_df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    original_count = gold_df.count()
    final_count = serving_df.count()
    logger.logger.info(
        f"📊 Final result: {final_count:,}/{original_count:,} records for serving"
    )

    return serving_df


def load_to_postgres(serving_df: DataFrame, postgres_config: dict, logger):
    """Load dữ liệu vào PostgreSQL"""

    if postgres_config is None:
        postgres_config = {
            "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'realestate')}",
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver",
        }

    serving_df = serving_df.coalesce(4)

    write_options = {
        "url": postgres_config["url"],
        "user": postgres_config["user"],
        "password": postgres_config["password"],
        "driver": postgres_config["driver"],
        "batchsize": "5000",
        "numPartitions": "4",
        "isolationLevel": "READ_COMMITTED",
        "stringtype": "unspecified",
        "dbtable": "properties",
    }

    try:
        serving_df.write.format("jdbc").options(**write_options).mode("append").save()
        logger.logger.info(
            f"✅ Successfully appended {serving_df.count():,} records to PostgreSQL"
        )
    except Exception as e:
        logger.logger.error(f"❌ PostgreSQL loading failed: {e}")
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Load Data to Serving Layer (PostgreSQL)"
    )
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-type", type=str, default="house", choices=["house", "other", "all"]
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

    spark = create_spark_session(
        "Load Data to Serving Layer",
        config={
            "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.7.0.jar",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
    )

    try:
        load_to_serving_layer(spark, args.date, args.property_type, postgres_config)
        print("✅ Serving layer load completed successfully!")
    finally:
        spark.stop()
