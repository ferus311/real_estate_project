"""
Hợp nhất dữ liệu từ nhiều nguồn
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    regexp_replace,
    concat,
    md5,
    trim,
    monotonically_increasing_id,
    when,
    lower,
    element_at,
    split,
    size,
)
import sys
import os
from datetime import datetime
import argparse
import uuid

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.schema.common_schema import get_unified_property_schema
from common.utils.date_utils import (
    get_date_format,
    get_hdfs_path,
    generate_processing_id,
)
from common.utils.hdfs_utils import (
    check_hdfs_path_exists,
    ensure_hdfs_path,
    list_hdfs_files,
)
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


def unify_property_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """
    Hợp nhất dữ liệu bất động sản từ nhiều nguồn

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_type: Loại bất động sản ("house" hoặc "other"). Mặc định là "house".

    Returns:
        DataFrame đã được hợp nhất
    """
    logger = SparkJobLogger("unify_property_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Tạo ID xử lý
    processing_id = generate_processing_id("property_unify")

    # Đường dẫn nguồn và đích
    silver_path = get_hdfs_path(
        "/data/realestate/processed/silver", "all", property_type, input_date
    )
    gold_path = get_hdfs_path(
        "/data/realestate/processed/gold", "unified", property_type, input_date
    )

    # Đảm bảo đường dẫn đích tồn tại
    ensure_hdfs_path(spark, gold_path)

    # Lấy schema thống nhất
    unified_schema = get_unified_property_schema()

    try:
        # Đường dẫn các file silver từ từng data source
        batdongsan_silver_path = get_hdfs_path(
            "/data/realestate/processed/silver", "batdongsan", property_type, input_date
        )
        chotot_silver_path = get_hdfs_path(
            "/data/realestate/processed/silver", "chotot", property_type, input_date
        )

        batdongsan_file = (
            f"{batdongsan_silver_path}/batdongsan_{input_date.replace('-', '')}.parquet"
        )
        chotot_file = (
            f"{chotot_silver_path}/chotot_{input_date.replace('-', '')}.parquet"
        )

        data_sources = []

        # Đọc dữ liệu Batdongsan nếu có
        if check_hdfs_path_exists(spark, batdongsan_file):
            batdongsan_df = spark.read.parquet(batdongsan_file)
            logger.log_dataframe_info(batdongsan_df, "batdongsan_transformed")
            data_sources.append(batdongsan_df)
        else:
            logger.logger.warning(
                f"Không tìm thấy dữ liệu Batdongsan: {batdongsan_file}"
            )

        # Đọc dữ liệu Chotot nếu có
        if check_hdfs_path_exists(spark, chotot_file):
            chotot_df = spark.read.parquet(chotot_file)
            logger.log_dataframe_info(chotot_df, "chotot_transformed")
            data_sources.append(chotot_df)
        else:
            logger.logger.warning(f"Không tìm thấy dữ liệu Chotot: {chotot_file}")

        # Kiểm tra xem có dữ liệu không
        if not data_sources:
            error_message = (
                f"Không tìm thấy dữ liệu nào để hợp nhất cho ngày {input_date}"
            )
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        # Lấy danh sách các trường trong schema thống nhất
        schema_fields = [field.name for field in unified_schema.fields]

        # Tạo ID duy nhất và chuẩn hóa schema cho mỗi DataFrame trước khi hợp nhất
        normalized_dfs = []
        for i, df in enumerate(data_sources):
            # Determine source name from actual data (now using 'source' field)
            source_name = "unknown"
            if "source" in df.columns:
                source_sample = df.select("source").limit(1).collect()
                if source_sample and source_sample[0]["source"]:
                    source_name = source_sample[0]["source"]
            elif "data_source" in df.columns:
                # Fallback to old field name if still exists
                source_sample = df.select("data_source").limit(1).collect()
                if source_sample and source_sample[0]["data_source"]:
                    source_name = source_sample[0]["data_source"]
                # Rename the field for consistency
                df = df.withColumnRenamed("data_source", "source")

            logger.logger.info(f"Processing source: {source_name}")

            # Tạo ID duy nhất dựa trên url và source (handle missing fields gracefully)
            if "url" in df.columns and "source" in df.columns:
                df = df.withColumn("id", md5(concat(col("url"), col("source"))))
            elif "url" in df.columns:
                df = df.withColumn("id", md5(col("url")))
            else:
                # Fallback to generated ID if no URL
                df = df.withColumn(
                    "id",
                    md5(
                        concat(
                            lit(source_name),
                            monotonically_increasing_id().cast("string"),
                        )
                    ),
                )

            # Log thông tin về schema của nguồn dữ liệu
            logger.logger.info(f"Chuẩn hóa schema cho nguồn: {source_name}")
            logger.logger.info(f"Các cột hiện có: {', '.join(df.columns)}")
            logger.logger.info(f"Số lượng cột hiện có: {len(df.columns)}")

            # 1. Thêm các cột còn thiếu với giá trị null
            current_fields = df.columns
            missing_columns = [
                field for field in schema_fields if field not in current_fields
            ]
            if missing_columns:
                logger.logger.info(f"Các cột cần thêm: {', '.join(missing_columns)}")

            for field_name in schema_fields:
                if field_name not in current_fields:
                    matching_field = next(
                        (
                            field
                            for field in unified_schema.fields
                            if field.name == field_name
                        ),
                        None,
                    )
                    if matching_field:
                        df = df.withColumn(
                            field_name, lit(None).cast(matching_field.dataType)
                        )

            # 2. Chọn các cột theo đúng thứ tự trong schema thống nhất
            # Chỉ chọn các cột có trong schema
            select_fields = [field for field in schema_fields if field in df.columns]
            df = df.select(*select_fields)

            # 3. Thêm các cột còn thiếu sau khi select (để đảm bảo thứ tự đúng)
            for field_name in schema_fields:
                if field_name not in df.columns:
                    matching_field = next(
                        (
                            field
                            for field in unified_schema.fields
                            if field.name == field_name
                        ),
                        None,
                    )
                    if matching_field:
                        df = df.withColumn(
                            field_name, lit(None).cast(matching_field.dataType)
                        )

            # 4. Sắp xếp lại các cột theo đúng thứ tự của schema thống nhất
            df = df.select(schema_fields)

            # Log thông tin sau khi chuẩn hóa
            logger.logger.info(
                f"Chuẩn hóa hoàn tất. Số lượng cột sau chuẩn hóa: {len(df.columns)}"
            )

            normalized_dfs.append(df)

        # Hợp nhất tất cả dữ liệu đã được chuẩn hóa
        if len(normalized_dfs) > 1:
            # Nếu có nhiều nguồn, sử dụng UNION
            unified_df = normalized_dfs[0]
            for df in normalized_dfs[1:]:
                unified_df = unified_df.union(df)
        else:
            # Nếu chỉ có một nguồn
            unified_df = normalized_dfs[0]

        # Tạo property_type dựa vào data_type và house_type (nếu có)
        # Improved logic to handle different field availability
        unified_df = unified_df.withColumn(
            "property_type",
            when(
                col("house_type").isNotNull() & (col("house_type") != ""),
                col("house_type"),
            )
            .when(
                col("data_type").isNotNull() & (col("data_type") != ""),
                col("data_type"),
            )
            .when(col("bedroom").isNotNull() & (col("bedroom") > 0), lit("HOUSE"))
            .when(col("area").isNotNull() & (col("area") > 0), lit("LAND"))
            .otherwise(lit("UNKNOWN")),
        )

        # Trích xuất địa chỉ thành các thành phần (cải thiện logic)
        if "location" in unified_df.columns:
            unified_df = (
                unified_df.withColumn(
                    "province",
                    when(
                        col("location").isNotNull() & (col("location") != ""),
                        trim(element_at(split(col("location"), ","), -1)),
                    ).otherwise(lit(None)),
                )
                .withColumn(
                    "district",
                    when(
                        col("location").isNotNull()
                        & (col("location") != "")
                        & (size(split(col("location"), ",")) >= 2),
                        trim(element_at(split(col("location"), ","), -2)),
                    ).otherwise(lit(None)),
                )
                .withColumn(
                    "ward",
                    when(
                        col("location").isNotNull()
                        & (col("location") != "")
                        & (size(split(col("location"), ",")) >= 3),
                        trim(element_at(split(col("location"), ","), -3)),
                    ).otherwise(lit(None)),
                )
            )

        # Thêm thông tin xử lý unify (chỉ processing_id, timestamp đã có từ transform jobs)
        unified_df = unified_df.withColumn("processing_id", lit(processing_id))

        # Không cần áp dụng lại schema vì đã chuẩn hóa DataFrames trước khi hợp nhất
        # Các cột đã được chuẩn hóa và sắp xếp theo thứ tự của schema thống nhất

        # Log thông tin sau khi hợp nhất
        logger.log_dataframe_info(unified_df, "unified_data")

        # Ghi dữ liệu ra
        output_path = os.path.join(
            gold_path, f"unified_{property_type}_{input_date.replace('-', '')}.parquet"
        )
        unified_df.write.mode("overwrite").parquet(output_path)

        logger.logger.info(f"Đã ghi {unified_df.count()} bản ghi vào {output_path}")
        logger.end_job()

        return unified_df

    except Exception as e:
        logger.log_error("Lỗi khi hợp nhất dữ liệu", e)
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Unify Property Data")
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
    spark = create_spark_session("Unify Property Data")

    try:
        # Hợp nhất dữ liệu
        unify_property_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
