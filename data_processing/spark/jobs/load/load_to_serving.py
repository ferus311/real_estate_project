"""
Load dữ liệu vào Serving Layer (PostgreSQL) cho Web API
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, coalesce, row_number, desc
from pyspark.sql.window import Window
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

    # Default PostgreSQL config từ environment variables
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
        serving_df = serving_df.dropDuplicates(["id"])
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
        # gold_path = get_hdfs_path(
        #     "/data/realestate/processed/gold/unified", ptype, input_date.replace("-", "/")
        # )
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

    # Debug: Kiểm tra schema của Gold layer
    logger.logger.info("🔍 Gold layer schema:")
    for field in gold_df.schema.fields:
        logger.logger.info(f"  - {field.name}: {field.dataType}")

    # Complete mapping từ Gold schema sang PostgreSQL schema
    serving_df = gold_df.select(
        # Primary identifiers
        col("id"),  # Keep as is - VARCHAR(255) in PostgreSQL
        col("url"),
        col("source"),
        # Basic property information
        col("title"),
        col("description"),
        col("location"),  # full location text
        col("data_type"),
        # Location information (text fields)
        col("province"),
        col("district"),
        col("ward"),
        col("street"),
        # Location IDs for efficient queries
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
        # Property details (keep original names to match Django schema)
        col("bedroom").cast("double"),  # from Gold: bedroom (can be decimal)
        col("bathroom").cast("double"),  # from Gold: bathroom (can be decimal)
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
        # Data quality and processing metadata
        col("data_quality_score").cast("double"),
        col("processing_id"),
        # Serving layer metadata
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    ).filter(
        # Enhanced data quality filters
        col("id").isNotNull()
        & col("title").isNotNull()
        & col("source").isNotNull()
        # Price and area can be null for some property types
        & (col("price").isNull() | (col("price") >= 0))
        & (col("area").isNull() | (col("area") > 0))
        # Coordinate validation
        & (
            (col("latitude").isNull() & col("longitude").isNull())
            | (col("latitude").between(-90, 90) & col("longitude").between(-180, 180))
        )
    )

    # Calculate counts for logging
    original_count = gold_df.count()
    filtered_count = serving_df.count()
    logger.logger.info(
        f"📊 Filtered {filtered_count:,}/{original_count:,} records after quality checks"
    )

    # Xử lý duplicate records dựa trên data_quality_score
    # Nếu trùng ID, giữ lại record có data_quality_score cao hơn
    # Nếu data_quality_score bằng nhau, giữ lại record mới nhất (dựa trên processing_timestamp)
    logger.logger.info("🔍 Handling duplicate IDs based on data_quality_score...")

    # Tạo window để rank records theo ID
    window_spec = Window.partitionBy("id").orderBy(
        desc("data_quality_score"),
        desc("processing_timestamp"),
        desc("updated_at")
    )

    # Thêm row_number để identify record tốt nhất cho mỗi ID
    serving_df_ranked = serving_df.withColumn("row_num", row_number().over(window_spec))

    # Chỉ giữ lại record tốt nhất (row_num = 1)
    serving_df_deduped = serving_df_ranked.filter(col("row_num") == 1).drop("row_num")

    # Log deduplication results
    deduped_count = serving_df_deduped.count()
    if filtered_count > deduped_count:
        logger.logger.info(
            f"� Deduplicated {filtered_count - deduped_count:,} records based on data_quality_score"
        )

    serving_df = serving_df_deduped

    # Log transformation results
    original_count = gold_df.count()
    final_count = serving_df.count()
    logger.logger.info(
        f"📊 Final result: {final_count:,}/{original_count:,} records for serving"
    )

    if filtered_count < original_count:
        logger.logger.info(
            f"🔍 Filtered out {original_count - filtered_count:,} records due to data quality issues"
        )

    return serving_df


def load_to_postgres(serving_df: DataFrame, postgres_config: dict, logger):
    """Load dữ liệu vào PostgreSQL với UPSERT logic"""

    # Tối ưu hóa Spark cho PostgreSQL write
    serving_df = serving_df.coalesce(4)  # Giảm số partitions để tăng batch size

    # PostgreSQL write configuration
    write_options = {
        "url": postgres_config["url"],
        "user": postgres_config["user"],
        "password": postgres_config["password"],
        "driver": postgres_config["driver"],
        "batchsize": "5000",  # Giảm batch size để tránh memory issues
        "numPartitions": "4",
        "isolationLevel": "READ_COMMITTED",
        "stringtype": "unspecified",  # Cho phép PostgreSQL auto-convert types
    }

    try:
        # Strategy 1: Sử dụng staging table cho UPSERT
        logger.logger.info("🔄 Loading data to staging table for UPSERT...")

        # Tạo staging table name
        staging_table = "properties_staging"
        staging_options = write_options.copy()
        staging_options["dbtable"] = staging_table

        # Load vào staging table (overwrite để đảm bảo clean state)
        serving_df.write.format("jdbc").options(**staging_options).mode("overwrite").save()

        logger.logger.info(f"✅ Successfully loaded {serving_df.count():,} records to staging table")

        # Thực hiện UPSERT bằng cách overwrite main table với data đã clean
        logger.logger.info("� Executing UPSERT by overwriting main table...")

        # Load trực tiếp vào main table với overwrite mode
        main_options = write_options.copy()
        main_options["dbtable"] = "properties"

        serving_df.write.format("jdbc").options(**main_options).mode("overwrite").save()
        logger.logger.info(f"✅ Successfully loaded {serving_df.count():,} records to main properties table")

    except Exception as e:
        logger.logger.warning(f"⚠️ Staging table approach failed: {e}")

        # Fallback: Try truncate and load (simple but effective)
        logger.logger.warning("🔄 Falling back to truncate and reload...")
        try:
            # First, truncate the main table
            logger.logger.info("🗑️ Truncating main properties table...")

            # Load new data with overwrite mode
            direct_options = write_options.copy()
            direct_options["dbtable"] = "properties"

            serving_df.write.format("jdbc").options(**direct_options).mode("overwrite").save()
            logger.logger.info(f"✅ Successfully reloaded {serving_df.count():,} records using overwrite mode")

        except Exception as e2:
            logger.logger.error(f"❌ All loading strategies failed: {e2}")
            raise


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
        "--postgres-host",
        type=str,
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="PostgreSQL host",
    )
    parser.add_argument(
        "--postgres-port",
        type=str,
        default=os.getenv("POSTGRES_PORT", "5432"),
        help="PostgreSQL port",
    )
    parser.add_argument(
        "--postgres-db",
        type=str,
        default=os.getenv("POSTGRES_DB", "realestate"),
        help="PostgreSQL database",
    )
    parser.add_argument(
        "--postgres-user",
        type=str,
        default=os.getenv("POSTGRES_USER", "postgres"),
        help="PostgreSQL user",
    )
    parser.add_argument(
        "--postgres-password",
        type=str,
        default=os.getenv("POSTGRES_PASSWORD", "password"),
        help="PostgreSQL password",
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
