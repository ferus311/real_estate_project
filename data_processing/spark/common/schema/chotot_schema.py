from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)


def get_chotot_raw_schema():
    """
    Định nghĩa schema cho dữ liệu thô từ nguồn Chotot

    Returns:
        StructType: Schema của dữ liệu thô Chotot
    """
    return StructType(
        [
            StructField("area", StringType(), True),
            StructField("bathroom", StringType(), True),
            StructField("bedroom", StringType(), True),
            StructField("crawl_timestamp", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("floor_count", StringType(), True),
            StructField("house_direction", StringType(), True),
            StructField("house_type", StringType(), True),
            StructField("interior", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("legal_status", StringType(), True),
            StructField("length", StringType(), True),
            StructField("living_size", StringType(), True),
            StructField("location", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("posted_date", StringType(), True),
            StructField("price", StringType(), True),
            StructField("price_per_m2", StringType(), True),
            StructField("seller_info", StringType(), True),
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("width", StringType(), True),
            # StructField("street", StringType(), True),
            # StructField("ward", StringType(), True),
            # StructField("district", StringType(), True),
            # StructField("province", StringType(), True),
        ]
    )


def get_chotot_processed_schema():
    """
    Định nghĩa schema cho dữ liệu đã xử lý từ nguồn Chotot

    Returns:
        StructType: Schema của dữ liệu đã xử lý Chotot
    """
    return StructType(
        [
            StructField("area", DoubleType(), True),
            StructField("bathroom", DoubleType(), True),
            StructField("bedroom", DoubleType(), True),
            StructField("crawl_timestamp", TimestampType(), True),
            StructField("data_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("floor_count", DoubleType(), True),
            StructField("house_direction", StringType(), True),
            StructField("house_direction_id", IntegerType(), True),
            StructField("house_type", StringType(), True),
            StructField("house_type_id", IntegerType(), True),
            StructField("interior", StringType(), True),
            StructField("interior_id", IntegerType(), True),
            StructField("legal_status", StringType(), True),
            StructField("legal_status_id", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("length", DoubleType(), True),
            StructField("living_size", DoubleType(), True),
            StructField("location", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("posted_date", TimestampType(), True),
            StructField("price", DoubleType(), True),
            StructField("price_per_m2", DoubleType(), True),
            StructField("street", StringType(), True),
            StructField("ward", StringType(), True),
            StructField("district", StringType(), True),
            StructField("province", StringType(), True),
            StructField("province_id", IntegerType(), True),
            StructField("district_id", IntegerType(), True),
            StructField("ward_id", IntegerType(), True),
            StructField("street_id", IntegerType(), True),
            # StructField("seller_info", StringType(), True),
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("width", DoubleType(), True),
            StructField("processing_date", TimestampType(), True),
            StructField("processing_id", StringType(), True),
        ]
    )
