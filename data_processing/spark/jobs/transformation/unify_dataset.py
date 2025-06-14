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
    count,
    sum as spark_sum,
    avg,
    stddev,
    min as spark_min,
    max as spark_max,
    percentile_approx,
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
from common.utils.data_quality_scoring import (
    calculate_data_quality_score,
    add_data_quality_issues_flag,
    add_quality_level,
)
from common.config.spark_config import create_spark_session
from common.utils.duplicate_detection import apply_unify_deduplication


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

        # Tạo property_type dựa vào data_type (cột chính)
        # data_type là cột chính cho property_type, house_type chỉ là metadata của Chotot
        unified_df = unified_df.withColumn(
            "property_type",
            when(
                col("data_type").isNotNull() & (col("data_type") != ""),
                col("data_type"),
            )
            .when(col("bedroom").isNotNull() & (col("bedroom") > 0), lit("HOUSE"))
            .when(col("area").isNotNull() & (col("area") > 0), lit("LAND"))
            .otherwise(lit("UNKNOWN")),
        )

        # ✅ ADDRESS PARSING ĐÃ ĐƯỢC THỰC HIỆN Ở TRANSFORM STEP
        # Không cần xử lý lại ở đây, các trường street, ward, district, province đã có sẵn

        # Thêm thông tin xử lý unify (chỉ processing_id, timestamp đã có từ transform jobs)
        unified_df = unified_df.withColumn("processing_id", lit(processing_id))

        # === TÍNH ĐIỂM CHẤT LƯỢNG THỐNG NHẤT ===
        logger.logger.info("Tính điểm chất lượng dữ liệu thống nhất...")

        # Tính điểm chất lượng sử dụng module chung
        unified_df = calculate_data_quality_score(unified_df)

        # Thêm flags về vấn đề chất lượng
        unified_df = add_data_quality_issues_flag(unified_df)

        # Thêm level chất lượng
        unified_df = add_quality_level(unified_df)

        # Clean up helper columns
        unified_df = unified_df.drop(
            "has_valid_price",
            "has_valid_area",
            "has_valid_location",
            "has_coordinates",
            "has_valid_bedroom",
            "has_valid_bathroom",
        )

        logger.logger.info("Hoàn thành tính điểm chất lượng thống nhất!")

        # === Duplication Detection ===
        logger.logger.info("🔍 Bắt đầu phát hiện và loại bỏ trùng lặp...")

        # Log count trước khi deduplication
        pre_dedup_count = unified_df.count()
        logger.logger.info(f"📊 Records trước deduplication: {pre_dedup_count:,}")

        # Apply comprehensive deduplication
        unified_df = apply_unify_deduplication(unified_df)

        # Log kết quả deduplication
        post_dedup_count = unified_df.count()
        duplicates_removed = pre_dedup_count - post_dedup_count
        logger.logger.info(f"✅ Deduplication hoàn thành:")
        logger.logger.info(f"   📤 Records sau deduplication: {post_dedup_count:,}")
        logger.logger.info(f"   🗑️ Duplicates loại bỏ: {duplicates_removed:,}")
        logger.logger.info(
            f"   📉 Reduction rate: {duplicates_removed/pre_dedup_count*100:.1f}%"
        )

        # === THỐNG KÊ TOÀN DIỆN SAU KHI UNIFY ===
        logger.logger.info("🎯 Tính thống kê toàn diện cho dữ liệu đã unify...")

        # Cache để tối ưu performance
        unified_df.cache()

        total_records = unified_df.count()
        logger.logger.info(f"📊 TỔNG KẾT UNIFIED DATASET:")
        logger.logger.info(f"   Tổng số records: {total_records:,}")

        # Thống kê theo nguồn dữ liệu
        source_stats = unified_df.groupBy("source").count().collect()
        logger.logger.info(f"📋 THỐNG KÊ THEO NGUỒN DỮ LIỆU:")
        for row in source_stats:
            logger.logger.info(f"   {row['source']}: {row['count']:,} records")

        # Thống kê theo loại property
        property_stats = unified_df.groupBy("property_type").count().collect()
        logger.logger.info(f"🏠 THỐNG KÊ THEO LOẠI BẤT ĐỘNG SẢN:")
        for row in property_stats:
            logger.logger.info(f"   {row['property_type']}: {row['count']:,} records")

        # Thống kê theo quality level
        quality_stats = unified_df.groupBy("quality_level").count().collect()
        logger.logger.info(f"⭐ THỐNG KÊ CHẤT LƯỢNG DỮ LIỆU:")
        for row in quality_stats:
            logger.logger.info(f"   {row['quality_level']}: {row['count']:,} records")

        # Thống kê missing data
        missing_stats = unified_df.select(
            count("*").alias("total"),
            spark_sum(when(col("price").isNull(), 1).otherwise(0)).alias(
                "missing_price"
            ),
            spark_sum(when(col("area").isNull(), 1).otherwise(0)).alias("missing_area"),
            spark_sum(when(col("bedroom").isNull(), 1).otherwise(0)).alias(
                "missing_bedroom"
            ),
            spark_sum(when(col("bathroom").isNull(), 1).otherwise(0)).alias(
                "missing_bathroom"
            ),
            spark_sum(
                when(col("latitude").isNull() | col("longitude").isNull(), 1).otherwise(
                    0
                )
            ).alias("missing_coordinates"),
            spark_sum(
                when(col("location").isNull() | (col("location") == ""), 1).otherwise(0)
            ).alias("missing_location"),
        ).collect()[0]

        logger.logger.info(f"❌ THỐNG KÊ DỮ LIỆU THIẾU:")
        logger.logger.info(
            f"   Missing price: {missing_stats['missing_price']:,} ({missing_stats['missing_price']/total_records*100:.1f}%)"
        )
        logger.logger.info(
            f"   Missing area: {missing_stats['missing_area']:,} ({missing_stats['missing_area']/total_records*100:.1f}%)"
        )
        logger.logger.info(
            f"   Missing bedroom: {missing_stats['missing_bedroom']:,} ({missing_stats['missing_bedroom']/total_records*100:.1f}%)"
        )
        logger.logger.info(
            f"   Missing bathroom: {missing_stats['missing_bathroom']:,} ({missing_stats['missing_bathroom']/total_records*100:.1f}%)"
        )
        logger.logger.info(
            f"   Missing coordinates: {missing_stats['missing_coordinates']:,} ({missing_stats['missing_coordinates']/total_records*100:.1f}%)"
        )
        logger.logger.info(
            f"   Missing location: {missing_stats['missing_location']:,} ({missing_stats['missing_location']/total_records*100:.1f}%)"
        )

        # Thống kê giá trị
        # Lọc dữ liệu có giá hợp lệ để tính stats
        valid_price_df = unified_df.filter(
            col("price").isNotNull() & (col("price") > 0)
        )
        valid_area_df = unified_df.filter(col("area").isNotNull() & (col("area") > 0))

        if valid_price_df.count() > 0:
            price_stats = valid_price_df.select(
                avg("price").alias("avg_price"),
                stddev("price").alias("stddev_price"),
                spark_min("price").alias("min_price"),
                spark_max("price").alias("max_price"),
                percentile_approx("price", 0.5).alias("median_price"),
            ).collect()[0]

            logger.logger.info(
                f"💰 THỐNG KÊ GIÁ ({valid_price_df.count():,} records có giá hợp lệ):"
            )
            logger.logger.info(
                f"   Giá trung bình: {price_stats['avg_price']/1_000_000_000:.2f} tỷ VND"
            )
            logger.logger.info(
                f"   Giá median: {price_stats['median_price']/1_000_000_000:.2f} tỷ VND"
            )
            logger.logger.info(
                f"   Giá min: {price_stats['min_price']/1_000_000:.0f} triệu VND"
            )
            logger.logger.info(
                f"   Giá max: {price_stats['max_price']/1_000_000_000:.1f} tỷ VND"
            )
            logger.logger.info(
                f"   Độ lệch chuẩn: {price_stats['stddev_price']/1_000_000_000:.2f} tỷ VND"
            )

        if valid_area_df.count() > 0:
            area_stats = valid_area_df.select(
                avg("area").alias("avg_area"),
                stddev("area").alias("stddev_area"),
                spark_min("area").alias("min_area"),
                spark_max("area").alias("max_area"),
                percentile_approx("area", 0.5).alias("median_area"),
            ).collect()[0]

            logger.logger.info(
                f"📐 THỐNG KÊ DIỆN TÍCH ({valid_area_df.count():,} records có diện tích hợp lệ):"
            )
            logger.logger.info(
                f"   Diện tích trung bình: {area_stats['avg_area']:.1f} m²"
            )
            logger.logger.info(
                f"   Diện tích median: {area_stats['median_area']:.1f} m²"
            )
            logger.logger.info(f"   Diện tích min: {area_stats['min_area']:.0f} m²")
            logger.logger.info(f"   Diện tích max: {area_stats['max_area']:.0f} m²")
            logger.logger.info(f"   Độ lệch chuẩn: {area_stats['stddev_area']:.1f} m²")

        # Thống kê data quality score
        score_stats = unified_df.select(
            avg("data_quality_score").alias("avg_score"),
            stddev("data_quality_score").alias("stddev_score"),
            spark_min("data_quality_score").alias("min_score"),
            spark_max("data_quality_score").alias("max_score"),
            percentile_approx("data_quality_score", 0.5).alias("median_score"),
        ).collect()[0]

        logger.logger.info(f"🎯 THỐNG KÊ ĐIỂM CHẤT LƯỢNG (scale 0-100):")
        logger.logger.info(f"   Điểm trung bình: {score_stats['avg_score']:.1f}")
        logger.logger.info(f"   Điểm median: {score_stats['median_score']:.1f}")
        logger.logger.info(f"   Điểm min: {score_stats['min_score']:.0f}")
        logger.logger.info(f"   Điểm max: {score_stats['max_score']:.0f}")
        logger.logger.info(f"   Độ lệch chuẩn: {score_stats['stddev_score']:.1f}")

        # Thống kê theo tỉnh/thành phố (top 10)
        if "province" in unified_df.columns:
            province_stats = (
                unified_df.filter(col("province").isNotNull() & (col("province") != ""))
                .groupBy("province")
                .count()
                .orderBy(col("count").desc())
                .limit(10)
                .collect()
            )

            logger.logger.info(f"🌍 TOP 10 TỈNH/THÀNH PHỐ:")
            for row in province_stats:
                logger.logger.info(f"   {row['province']}: {row['count']:,} records")

        logger.logger.info("✅ Hoàn thành thống kê toàn diện!")

        # === FINAL SUMMARY ===
        final_count = unified_df.count()
        logger.logger.info(f"🏁 UNIFY PIPELINE COMPLETED SUCCESSFULLY!")
        logger.logger.info(f"📊 FINAL SUMMARY:")
        logger.logger.info(f"   📥 Raw input records: {pre_dedup_count:,}")
        logger.logger.info(f"   🔄 After deduplication: {final_count:,}")
        logger.logger.info(
            f"   📉 Total duplicates removed: {pre_dedup_count - final_count:,}"
        )
        logger.logger.info(
            f"   ⚡ Processing efficiency: {final_count/pre_dedup_count*100:.1f}% data retained"
        )

        # Không cần áp dụng lại schema vì đã chuẩn hóa DataFrames trước khi hợp nhất
        # Các cột đã được chuẩn hóa và sắp xếp theo thứ tự của schema thống nhất

        # Log thông tin sau khi hợp nhất
        logger.log_dataframe_info(unified_df, "unified_data_final")

        # Ghi dữ liệu ra
        output_path = os.path.join(
            gold_path, f"unified_{property_type}_{input_date.replace('-', '')}.parquet"
        )
        logger.logger.info(f"💾 Writing data to: {output_path}")
        unified_df.write.mode("overwrite").parquet(output_path)

        logger.logger.info(
            f"✅ Successfully wrote {final_count:,} records to Gold layer"
        )
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
