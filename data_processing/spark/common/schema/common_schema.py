from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


def get_unified_property_schema():
    """
    Định nghĩa schema thống nhất cho dữ liệu bất động sản từ tất cả các nguồn

    Returns:
        StructType: Schema thống nhất cho dữ liệu bất động sản
    """
    return StructType(
        [
            # Thông tin cơ bản
            StructField("id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("property_type", StringType(), True),
            StructField("description", StringType(), True),
            # Thông tin giá và diện tích
            StructField("price", DoubleType(), True),
            StructField("price_per_m2", DoubleType(), True),
            StructField("area", DoubleType(), True),
            StructField("living_size", DoubleType(), True),
            # Đặc điểm bất động sản
            StructField("bedroom", DoubleType(), True),
            StructField("bathroom", DoubleType(), True),
            StructField("floor_count", DoubleType(), True),
            StructField("house_direction", StringType(), True),
            StructField("interior", StringType(), True),
            StructField("legal_status", StringType(), True),
            StructField("facade_width", DoubleType(), True),
            StructField("road_width", DoubleType(), True),
            StructField("length", DoubleType(), True),
            StructField("width", DoubleType(), True),
            # Thông tin vị trí
            StructField("location", StringType(), True),
            StructField("province", StringType(), True),
            StructField("district", StringType(), True),
            StructField("ward", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            # Thông tin người bán
            StructField("seller_info", StringType(), True),
            StructField("seller_type", StringType(), True),
            StructField("seller_name", StringType(), True),
            StructField("seller_phone", StringType(), True),
            # Metadata
            StructField("source", StringType(), True),
            StructField("posted_date", TimestampType(), True),
            StructField("crawl_timestamp", TimestampType(), True),
            StructField("processing_date", TimestampType(), True),
            StructField("processing_id", StringType(), True),
        ]
    )
