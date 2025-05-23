"""
Làm giàu dữ liệu với thông tin địa lý
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    split,
    trim,
    when,
    lit,
    lower,
    upper,
    concat_ws,
    regexp_extract,
    regexp_replace,
)
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


def enrich_with_geo_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """
    Làm giàu dữ liệu bất động sản với thông tin địa lý

    Args:
        spark: SparkSession
        input_date: Ngày xử lý, định dạng "YYYY-MM-DD". Mặc định là None (ngày hôm nay).
        property_type: Loại bất động sản ("house", "other", hoặc "all"). Mặc định là "house".

    Returns:
        DataFrame đã được làm giàu
    """
    logger = SparkJobLogger("enrich_with_geo_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    # Xác định ngày xử lý
    if input_date is None:
        input_date = get_date_format()

    # Đường dẫn nguồn và đích
    gold_path = get_hdfs_path(
        "/data/realestate/processed/analytics", property_type, input_date
    )

    try:
        # Đọc dữ liệu đã được kiểm tra chất lượng
        validated_file = f"{gold_path}/validated_{property_type}_{input_date.replace('-', '')}.parquet"

        if not check_hdfs_path_exists(spark, validated_file):
            error_message = (
                f"Dữ liệu đã kiểm tra chất lượng không tồn tại: {validated_file}"
            )
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        validated_df = spark.read.parquet(validated_file)
        logger.log_dataframe_info(validated_df, "input_data")

        # Chuẩn hóa dữ liệu địa lý
        # 1. Chuẩn hóa tên tỉnh/thành phố
        province_mapping = {
            "hcm": "Hồ Chí Minh",
            "tphcm": "Hồ Chí Minh",
            "ho chi minh": "Hồ Chí Minh",
            "tp hcm": "Hồ Chí Minh",
            "tp.hcm": "Hồ Chí Minh",
            "tp. hcm": "Hồ Chí Minh",
            "tp. ho chi minh": "Hồ Chí Minh",
            "tp.ho chi minh": "Hồ Chí Minh",
            "ha noi": "Hà Nội",
            "hn": "Hà Nội",
            "tp ha noi": "Hà Nội",
            "tp.ha noi": "Hà Nội",
            "tp. ha noi": "Hà Nội",
            "da nang": "Đà Nẵng",
            "tp da nang": "Đà Nẵng",
            "tp.da nang": "Đà Nẵng",
            "tp. da nang": "Đà Nẵng",
        }

        # Tạo điều kiện khi cho việc chuẩn hóa tên tỉnh/thành phố
        province_conditions = when(col("province").isNull(), lit(None))
        for key, value in province_mapping.items():
            province_conditions = province_conditions.when(
                lower(trim(col("province"))).contains(key), lit(value)
            )
        province_conditions = province_conditions.otherwise(trim(col("province")))

        # Áp dụng chuẩn hóa
        enriched_df = validated_df.withColumn(
            "province_normalized", province_conditions
        )

        # 2. Tách thông tin địa chỉ chi tiết hơn nếu có thể
        location_parts = split(col("location"), ",")

        enriched_df = (
            enriched_df.withColumn("full_address", trim(col("location")))
            .withColumn("province", trim(col("province_normalized")))
            .withColumn(
                "district",
                when(col("district").isNotNull(), trim(col("district"))).otherwise(
                    trim(element_at(location_parts, -2))
                ),
            )
            .withColumn(
                "ward",
                when(col("ward").isNotNull(), trim(col("ward"))).otherwise(
                    trim(element_at(location_parts, -3))
                ),
            )
            .withColumn("street", trim(element_at(location_parts, 1)))
            .drop("province_normalized")
        )

        # 3. Thêm thông tin vùng miền
        region_mapping = {
            "Hà Nội": "Miền Bắc",
            "Hải Phòng": "Miền Bắc",
            "Bắc Giang": "Miền Bắc",
            "Bắc Kạn": "Miền Bắc",
            "Cao Bằng": "Miền Bắc",
            "Hà Giang": "Miền Bắc",
            "Lạng Sơn": "Miền Bắc",
            "Phú Thọ": "Miền Bắc",
            "Quảng Ninh": "Miền Bắc",
            "Thái Nguyên": "Miền Bắc",
            "Tuyên Quang": "Miền Bắc",
            "Lào Cai": "Miền Bắc",
            "Yên Bái": "Miền Bắc",
            "Điện Biên": "Miền Bắc",
            "Hòa Bình": "Miền Bắc",
            "Lai Châu": "Miền Bắc",
            "Sơn La": "Miền Bắc",
            "Bắc Ninh": "Miền Bắc",
            "Hà Nam": "Miền Bắc",
            "Hải Dương": "Miền Bắc",
            "Hưng Yên": "Miền Bắc",
            "Nam Định": "Miền Bắc",
            "Ninh Bình": "Miền Bắc",
            "Thái Bình": "Miền Bắc",
            "Vĩnh Phúc": "Miền Bắc",
            "Đà Nẵng": "Miền Trung",
            "Quảng Nam": "Miền Trung",
            "Quảng Ngãi": "Miền Trung",
            "Bình Định": "Miền Trung",
            "Phú Yên": "Miền Trung",
            "Khánh Hòa": "Miền Trung",
            "Ninh Thuận": "Miền Trung",
            "Bình Thuận": "Miền Trung",
            "Thanh Hóa": "Miền Trung",
            "Nghệ An": "Miền Trung",
            "Hà Tĩnh": "Miền Trung",
            "Quảng Bình": "Miền Trung",
            "Quảng Trị": "Miền Trung",
            "Thừa Thiên Huế": "Miền Trung",
            "Hồ Chí Minh": "Miền Nam",
            "Bà Rịa - Vũng Tàu": "Miền Nam",
            "Bình Dương": "Miền Nam",
            "Bình Phước": "Miền Nam",
            "Đồng Nai": "Miền Nam",
            "Tây Ninh": "Miền Nam",
            "An Giang": "Miền Nam",
            "Bạc Liêu": "Miền Nam",
            "Bến Tre": "Miền Nam",
            "Cà Mau": "Miền Nam",
            "Cần Thơ": "Miền Nam",
            "Đồng Tháp": "Miền Nam",
            "Hậu Giang": "Miền Nam",
            "Kiên Giang": "Miền Nam",
            "Long An": "Miền Nam",
            "Sóc Trăng": "Miền Nam",
            "Tiền Giang": "Miền Nam",
            "Trà Vinh": "Miền Nam",
            "Vĩnh Long": "Miền Nam",
        }

        # Tạo điều kiện when cho việc xác định vùng miền
        region_conditions = when(col("province").isNull(), lit(None))
        for province, region in region_mapping.items():
            region_conditions = region_conditions.when(
                col("province") == province, lit(region)
            )
        region_conditions = region_conditions.otherwise(lit("Chưa xác định"))

        # Áp dụng phân loại vùng miền
        enriched_df = enriched_df.withColumn("region", region_conditions)

        # 4. Cập nhật các giá trị NULL thành "Chưa xác định" cho các trường địa lý quan trọng
        enriched_df = (
            enriched_df.withColumn(
                "province",
                when(col("province").isNull(), lit("Chưa xác định")).otherwise(
                    col("province")
                ),
            )
            .withColumn(
                "district",
                when(col("district").isNull(), lit("Chưa xác định")).otherwise(
                    col("district")
                ),
            )
            .withColumn(
                "ward",
                when(col("ward").isNull(), lit("Chưa xác định")).otherwise(col("ward")),
            )
        )

        # Log thông tin sau khi làm giàu dữ liệu
        logger.log_dataframe_info(enriched_df, "enriched_data")

        # Ghi dữ liệu đã làm giàu
        output_path = f"{gold_path}/enriched_{property_type}_{input_date.replace('-', '')}.parquet"
        enriched_df.write.mode("overwrite").parquet(output_path)

        logger.logger.info(
            f"Đã ghi {enriched_df.count()} bản ghi đã được làm giàu vào {output_path}"
        )
        logger.end_job()

        return enriched_df

    except Exception as e:
        logger.log_error("Lỗi khi làm giàu dữ liệu", e)
        raise


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Enrich Property Data with Geo Information"
    )
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
    spark = create_spark_session("Enrich Property Data with Geo Information")

    try:
        # Làm giàu dữ liệu
        enrich_with_geo_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
