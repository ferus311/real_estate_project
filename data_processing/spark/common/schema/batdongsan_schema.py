from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


def get_batdongsan_raw_schema():
    """
    Định nghĩa schema cho dữ liệu thô từ nguồn Batdongsan

    Returns:
        StructType: Schema của dữ liệu thô Batdongsan
    """
    return StructType(
        [
            StructField("area", StringType(), True),
            StructField("bathroom", StringType(), True),
            StructField("bedroom", StringType(), True),
            StructField("crawl_timestamp", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("facade_width", StringType(), True),
            StructField("floor_count", StringType(), True),
            StructField("house_direction", StringType(), True),
            StructField("interior", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("legal_status", StringType(), True),
            StructField("location", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("posted_date", StringType(), True),
            StructField("price", StringType(), True),
            StructField("price_per_m2", StringType(), True),
            StructField("road_width", StringType(), True),
            StructField("seller_info", StringType(), True),
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )


def get_batdongsan_processed_schema():
    """
    Định nghĩa schema cho dữ liệu đã xử lý từ nguồn Batdongsan

    Returns:
        StructType: Schema của dữ liệu đã xử lý Batdongsan
    """
    return StructType(
        [
            StructField("area", DoubleType(), True),
            StructField("bathroom", DoubleType(), True),
            StructField("bedroom", DoubleType(), True),
            StructField("crawl_timestamp", TimestampType(), True),
            StructField("data_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("facade_width", DoubleType(), True),
            StructField("floor_count", DoubleType(), True),
            StructField("house_direction", StringType(), True),
            StructField("interior", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("legal_status", StringType(), True),
            StructField("location", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("posted_date", TimestampType(), True),
            StructField("price", DoubleType(), True),
            StructField("price_per_m2", DoubleType(), True),
            StructField("road_width", DoubleType(), True),
            StructField("seller_info", StringType(), True),
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("street", StringType(), True),
            StructField("ward", StringType(), True),
            StructField("district", StringType(), True),
            StructField("province", StringType(), True),
            StructField("url", StringType(), True),
            StructField("processing_date", TimestampType(), True),
            StructField("processing_id", StringType(), True),
        ]
    )
