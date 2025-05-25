from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


def get_unified_property_schema():
    """
    Schema thống nhất cho dữ liệu bất động sản từ Silver layer
    Dựa trên output thực tế của transform jobs
    """
    return StructType(
        [
            # ID và source info
            StructField("id", StringType(), False),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),  # Field name từ transform jobs
            # Thông tin cơ bản
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("location", StringType(), True),
            StructField("data_type", StringType(), True),  # Available in both sources
            # Địa chỉ chi tiết (trích xuất từ location)
            StructField("province", StringType(), True),
            StructField("district", StringType(), True),
            StructField("ward", StringType(), True),
            # Tọa độ (có sẵn trong cả 2 nguồn)
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            # Giá và diện tích (core metrics)
            StructField("price", DoubleType(), True),
            StructField("area", DoubleType(), True),
            StructField("price_per_m2", DoubleType(), True),
            # Chi tiết nhà (có trong cả 2 nguồn)
            StructField("bedroom", DoubleType(), True),
            StructField("bathroom", DoubleType(), True),
            StructField("floor_count", DoubleType(), True),
            # Đặc điểm nhà (có trong cả 2 nguồn)
            StructField("house_direction", StringType(), True),
            StructField("legal_status", StringType(), True),
            StructField("interior", StringType(), True),
            StructField("house_type", StringType(), True),
            # Dimension fields (gộp chung thay vì tách riêng từng nguồn)
            StructField("width", DoubleType(), True),  # Chiều rộng
            StructField("length", DoubleType(), True),  # Chiều dài
            StructField("living_size", DoubleType(), True),  # Diện tích sử dụng
            StructField("facade_width", DoubleType(), True),  # Mặt tiền
            StructField("road_width", DoubleType(), True),  # Đường vào
            # Timestamp fields
            StructField("posted_date", TimestampType(), True),
            StructField("crawl_timestamp", TimestampType(), True),
            # Quality and processing metadata
            StructField("data_quality_score", DoubleType(), True),
            StructField("processing_timestamp", TimestampType(), True),
            StructField("processing_id", StringType(), True),
        ]
    )
