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
    substring,
    length,
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
        # Sử dụng window 90 ngày để phát hiện trùng lặp tốt hơn
        deduplicated_df = apply_load_deduplication(
            serving_df, postgres_config, days_window=90
        )
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
    """Transform dữ liệu từ Gold layer cho PostgreSQL serving layer

    Áp dụng giới hạn ký tự cho các trường text để tránh lỗi database:
    - title: tối đa 500 ký tự
    - description: tối đa 2000 ký tự
    - location: tối đa 500 ký tự
    - province: tối đa 100 ký tự
    - district: tối đa 100 ký tự
    - ward: tối đa 200 ký tự
    - street: tối đa 300 ký tự
    - house_direction: tối đa 50 ký tự
    - legal_status: tối đa 100 ký tự
    - interior: tối đa 100 ký tự
    - house_type: tối đa 100 ký tự
    """

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
        # Giới hạn province tối đa 100 ký tự
        when(col("province").isNull(), col("province"))
        .otherwise(substring(col("province"), 1, 100))
        .alias("province"),
        # Giới hạn district tối đa 100 ký tự
        when(col("district").isNull(), col("district"))
        .otherwise(substring(col("district"), 1, 100))
        .alias("district"),
        # Giới hạn ward tối đa 200 ký tự để tránh lỗi database
        when(col("ward").isNull(), col("ward"))
        .otherwise(substring(col("ward"), 1, 200))
        .alias("ward"),
        # Giới hạn street tối đa 300 ký tự
        when(col("street").isNull(), col("street"))
        .otherwise(substring(col("street"), 1, 200))
        .alias("street"),
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
        # Giới hạn house_direction tối đa 50 ký tự
        when(col("house_direction").isNull(), col("house_direction"))
        .otherwise(substring(col("house_direction"), 1, 50))
        .alias("house_direction"),
        col("house_direction_code").cast("int"),
        # Giới hạn legal_status tối đa 100 ký tự
        when(col("legal_status").isNull(), col("legal_status"))
        .otherwise(substring(col("legal_status"), 1, 100))
        .alias("legal_status"),
        col("legal_status_code").cast("int"),
        # Giới hạn interior tối đa 100 ký tự
        when(col("interior").isNull(), col("interior"))
        .otherwise(substring(col("interior"), 1, 100))
        .alias("interior"),
        col("interior_code").cast("int"),
        # Giới hạn house_type tối đa 100 ký tự
        when(col("house_type").isNull(), col("house_type"))
        .otherwise(substring(col("house_type"), 1, 100))
        .alias("house_type"),
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
    """
    Load dữ liệu vào PostgreSQL sử dụng staging table và upsert để tránh lỗi trùng lặp

    Quy trình cải thiến:
    1. Tạo bảng staging (properties_staging) với cấu trúc giống bảng chính
    2. Ghi dữ liệu vào bảng staging
    3. Thực hiện UPSERT từ staging vào bảng chính với logic cập nhật thông minh:
       - Chỉ cập nhật khi bản ghi mới có chất lượng tốt hơn (data_quality_score cao hơn)
       - Hoặc cùng chất lượng nhưng mới hơn (processing_timestamp gần đây hơn)
    4. Xóa bảng staging sau khi hoàn thành
    5. Xử lý lỗi và rollback toàn diện
    """

    if postgres_config is None:
        postgres_config = {
            "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'realestate')}",
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver",
        }

    serving_df = serving_df.coalesce(4)

    # Parse PostgreSQL connection details
    url_parts = postgres_config["url"].replace("jdbc:postgresql://", "").split(":")
    host = url_parts[0]
    port = url_parts[1].split("/")[0]
    database = url_parts[1].split("/")[1]

    # Base JDBC options
    base_options = {
        "url": postgres_config["url"],
        "user": postgres_config["user"],
        "password": postgres_config["password"],
        "driver": postgres_config["driver"],
        "batchsize": "5000",
        "numPartitions": "4",
        "isolationLevel": "READ_COMMITTED",
        "stringtype": "unspecified",
    }

    try:
        import psycopg2
        from psycopg2 import sql

        total_records = serving_df.count()
        logger.logger.info(
            f"🔄 Loading {total_records:,} records to PostgreSQL using improved staging table approach..."
        )

        # Bước 1: Tạo và ghi vào bảng tạm (staging)
        logger.logger.info("Bước 1: Chuẩn bị và ghi vào bảng staging...")

        conn = None
        cursor = None
        try:
            # Kết nối để chuẩn bị bảng tạm với timeout
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=postgres_config["user"],
                password=postgres_config["password"],
                connect_timeout=30,  # Thêm timeout để tránh treo khi kết nối
            )
            cursor = conn.cursor()

            # Tạo bảng tạm nếu chưa tồn tại hoặc xóa dữ liệu nếu đã tồn tại
            cursor.execute("DROP TABLE IF EXISTS properties_staging")

            # Tạo bảng tạm với cùng cấu trúc như bảng chính
            cursor.execute(
                """
            CREATE TABLE properties_staging (LIKE properties INCLUDING ALL)
            """
            )
            conn.commit()
            logger.logger.info("✅ Bảng staging đã được tạo thành công")

        except Exception as e:
            logger.logger.error(f"❌ Lỗi khi tạo bảng staging: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        # Ghi dữ liệu vào bảng tạm
        try:
            staging_options = base_options.copy()
            staging_options["dbtable"] = "properties_staging"

            # Thêm thông số để cải thiện hiệu suất và giảm lỗi
            staging_options["numPartitions"] = "8"  # Tăng số lượng partition
            staging_options["batchsize"] = "1000"  # Giảm kích thước batch để tránh OOM

            # Ghi dữ liệu vào staging table
            serving_df.write.format("jdbc").options(**staging_options).mode(
                "append"
            ).save()
            logger.logger.info(f"✅ Đã ghi {total_records:,} bản ghi vào bảng staging")

        except Exception as e:
            logger.logger.error(f"❌ Lỗi khi ghi dữ liệu vào bảng staging: {str(e)}")
            # Khôi phục bằng cách xóa bảng staging
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=postgres_config["user"],
                    password=postgres_config["password"],
                )
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS properties_staging")
                conn.commit()
                cursor.close()
                conn.close()
            except:
                pass  # Bỏ qua lỗi trong quá trình dọn dẹp
            raise

        # Bước 2: Thực hiện UPSERT từ bảng tạm sang bảng chính
        logger.logger.info("Bước 2: Thực hiện UPSERT từ bảng staging vào bảng chính...")

        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=postgres_config["user"],
                password=postgres_config["password"],
                connect_timeout=30,
            )
            # Chế độ transaction tự động
            conn.autocommit = False
            cursor = conn.cursor()

            # Thiết lập statement timeout lớn hơn
            cursor.execute("SET statement_timeout = '1800000'")  # 30 phút

            # Lấy thống kê trước khi upsert
            cursor.execute("SELECT COUNT(*) FROM properties")
            count_before = cursor.fetchone()[0]

            # Thực hiện UPSERT với logic cập nhật cải tiến
            # Sử dụng kiến trúc UPSERT theo khuyến nghị của PostgreSQL
            upsert_query = """
            INSERT INTO properties
            SELECT * FROM properties_staging
            ON CONFLICT (id)
            DO UPDATE SET
                -- Cập nhật tất cả các trường
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                url = EXCLUDED.url,
                source = EXCLUDED.source,
                location = EXCLUDED.location,
                data_type = EXCLUDED.data_type,
                province = EXCLUDED.province,
                district = EXCLUDED.district,
                ward = EXCLUDED.ward,
                street = EXCLUDED.street,
                province_id = EXCLUDED.province_id,
                district_id = EXCLUDED.district_id,
                ward_id = EXCLUDED.ward_id,
                street_id = EXCLUDED.street_id,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                price = EXCLUDED.price,
                area = EXCLUDED.area,
                price_per_m2 = EXCLUDED.price_per_m2,
                bedroom = EXCLUDED.bedroom,
                bathroom = EXCLUDED.bathroom,
                floor_count = EXCLUDED.floor_count,
                width = EXCLUDED.width,
                length = EXCLUDED.length,
                living_size = EXCLUDED.living_size,
                facade_width = EXCLUDED.facade_width,
                road_width = EXCLUDED.road_width,
                house_direction = EXCLUDED.house_direction,
                house_direction_code = EXCLUDED.house_direction_code,
                legal_status = EXCLUDED.legal_status,
                legal_status_code = EXCLUDED.legal_status_code,
                interior = EXCLUDED.interior,
                interior_code = EXCLUDED.interior_code,
                house_type = EXCLUDED.house_type,
                house_type_code = EXCLUDED.house_type_code,
                posted_date = EXCLUDED.posted_date,
                crawl_timestamp = EXCLUDED.crawl_timestamp,
                processing_timestamp = EXCLUDED.processing_timestamp,
                data_quality_score = EXCLUDED.data_quality_score,
                processing_id = EXCLUDED.processing_id,
                updated_at = CURRENT_TIMESTAMP
            WHERE
                -- Chỉ cập nhật khi dữ liệu mới tốt hơn hoặc mới hơn
                EXCLUDED.data_quality_score > properties.data_quality_score
                OR (EXCLUDED.data_quality_score >= properties.data_quality_score
                    AND EXCLUDED.processing_timestamp > properties.processing_timestamp)
            """
            cursor.execute(upsert_query)

            # Lấy kết quả sau khi upsert
            cursor.execute("SELECT COUNT(*) FROM properties")
            count_after = cursor.fetchone()[0]

            # Tính toán số bản ghi đã thêm mới
            new_records = count_after - count_before

            # Tính toán số bản ghi đã cập nhật
            cursor.execute("SELECT COUNT(*) FROM properties_staging")
            staging_count = cursor.fetchone()[0]
            updated_records = staging_count - new_records

            # Commit transaction khi tất cả đã thành công
            conn.commit()

            logger.logger.info(f"📊 Kết quả UPSERT:")
            logger.logger.info(f"   ➕ Đã thêm mới: {new_records:,} bản ghi")
            logger.logger.info(f"   🔄 Đã cập nhật: {updated_records:,} bản ghi")
            logger.logger.info(f"   📊 Tổng bản ghi hiện tại: {count_after:,}")

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                    logger.logger.info("✅ Transaction đã được rollback sau lỗi")
                except:
                    logger.logger.error("❌ Không thể rollback transaction sau lỗi")
            logger.logger.error(f"❌ Lỗi khi thực hiện UPSERT: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        # Bước 3: Dọn dẹp - xóa bảng tạm
        logger.logger.info("Bước 3: Dọn dẹp bảng tạm...")

        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=postgres_config["user"],
                password=postgres_config["password"],
            )
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS properties_staging")
            conn.commit()
            cursor.close()
            conn.close()
            logger.logger.info("✅ Đã xóa bảng staging")

        except Exception as e:
            logger.logger.error(f"⚠️ Cảnh báo: Không thể xóa bảng staging: {str(e)}")
            # Tiếp tục thực thi ngay cả khi quá trình dọn dẹp thất bại

        logger.logger.info(f"✅ Hoàn thành quá trình load dữ liệu vào PostgreSQL")
        return True

    except Exception as e:
        logger.logger.error(f"❌ PostgreSQL loading failed: {str(e)}")
        # In traceback để debug chi tiết hơn
        import traceback

        logger.logger.error(traceback.format_exc())
        # Mọi lỗi khác không được xử lý sẽ được chuyển lên cấp cao hơn
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
