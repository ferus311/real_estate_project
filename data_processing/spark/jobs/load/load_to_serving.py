"""
Load dữ liệu vào Serving Layer (PostgreSQL) cho Web API
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce
import sys
import os
from datetime import datetime
import argparse

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.utils.date_utils import get_date_format, get_hdfs_path
from common.utils.hdfs_utils import check_hdfs_path_exists
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


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

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Default PostgreSQL config
    if postgres_config is None:
        postgres_config = {
            "url": "jdbc:postgresql://postgres:5432/realestate",
            "user": "postgres",
            "password": "password",
            "driver": "org.postgresql.Driver",
        }

    try:
        # 1. Extract từ Gold layer
        gold_df = extract_from_gold(spark, input_date, property_type, logger)

        # 2. Transform cho serving needs
        serving_df = transform_for_serving(gold_df, logger)

        # 3. Load vào PostgreSQL
        load_to_postgres(serving_df, postgres_config, logger)

        logger.logger.info("✅ Load to serving layer completed successfully!")
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
        # Đọc từ unified data trong Gold layer
        gold_path = get_hdfs_path(
            "/data/realestate/processed/gold/unified", ptype, input_date
        )

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
    """Transform dữ liệu cho serving layer"""

    # Select và transform fields cho web serving
    serving_df = gold_df.select(
        # Core property fields
        col("id").alias("property_id"),
        col("title"),
        col("price").cast("bigint"),
        col("area").cast("real"),
        col("bedrooms").cast("int"),
        col("bathrooms").cast("int"),
        col("description"),
        # Location fields
        col("province"),
        col("district"),
        col("ward"),
        col("address"),
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        # Source fields
        col("source"),
        col("url"),
        # Contact fields
        col("contact_name"),
        col("contact_phone"),
        # Computed fields for serving (sẽ được optimize trong PostgreSQL)
        when(col("area") > 0, col("price") / col("area"))
        .otherwise(None)
        .alias("price_per_m2"),
        # Property type classification
        when(col("price") < 1000000000, "budget")
        .when(col("price") < 5000000000, "mid-range")
        .otherwise("luxury")
        .alias("price_tier"),
        # Metadata
        current_timestamp().alias("updated_at"),
        lit(datetime.now().strftime("%Y-%m-%d")).alias("processing_date"),
    ).filter(
        # Basic data quality filters for serving
        col("price").isNotNull()
        & col("area").isNotNull()
        & col("title").isNotNull()
        & (col("price") > 0)
        & (col("area") > 0)
    )

    logger.logger.info(f"📊 Transformed {serving_df.count():,} records for serving")
    return serving_df


def load_to_postgres(serving_df: DataFrame, postgres_config: dict, logger):
    """Load dữ liệu vào PostgreSQL với upsert logic"""

    # Write vào PostgreSQL
    # Mode "append" với duplicate handling sẽ được handle bởi PostgreSQL constraints
    try:
        serving_df.write.format("jdbc").option("url", postgres_config["url"]).option(
            "dbtable", "properties"
        ).option("user", postgres_config["user"]).option(
            "password", postgres_config["password"]
        ).option(
            "driver", postgres_config["driver"]
        ).option(
            "batchsize", "10000"
        ).option(
            "numPartitions", "4"
        ).mode(
            "append"
        ).save()

        logger.logger.info("✅ Successfully loaded data to PostgreSQL")

    except Exception as e:
        # Nếu append failed (có thể do duplicates), thử overwrite
        logger.logger.warning(f"⚠️ Append failed, trying overwrite: {e}")

        serving_df.write.format("jdbc").option("url", postgres_config["url"]).option(
            "dbtable", "properties_staging"
        ).option("user", postgres_config["user"]).option(
            "password", postgres_config["password"]
        ).option(
            "driver", postgres_config["driver"]
        ).mode(
            "overwrite"
        ).save()

        logger.logger.info("✅ Successfully loaded data to PostgreSQL staging table")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Load Data to Serving Layer (PostgreSQL)"
    )
    parser.add_argument("--date", type=str, help="Processing date in YYYY-MM-DD format")
    parser.add_argument(
        "--property-type",
        type=str,
        default="house",
        choices=["house", "other", "all"],
        help="Property type",
    )
    parser.add_argument(
        "--postgres-host", type=str, default="postgres", help="PostgreSQL host"
    )
    parser.add_argument(
        "--postgres-port", type=str, default="5432", help="PostgreSQL port"
    )
    parser.add_argument(
        "--postgres-db", type=str, default="realestate", help="PostgreSQL database"
    )
    parser.add_argument(
        "--postgres-user", type=str, default="postgres", help="PostgreSQL user"
    )
    parser.add_argument(
        "--postgres-password", type=str, default="password", help="PostgreSQL password"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Cấu hình PostgreSQL từ arguments
    postgres_config = {
        "url": f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_db}",
        "user": args.postgres_user,
        "password": args.postgres_password,
        "driver": "org.postgresql.Driver",
    }

    # Khởi tạo Spark session với PostgreSQL driver
    spark = create_spark_session(
        "Load Data to Serving Layer",
        config={
            "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.7.0.jar",  # PostgreSQL JDBC driver
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
    )

    try:
        # Load dữ liệu vào serving layer
        load_to_serving_layer(spark, args.date, args.property_type, postgres_config)
        print("✅ Serving layer load completed successfully!")

    finally:
        spark.stop()
