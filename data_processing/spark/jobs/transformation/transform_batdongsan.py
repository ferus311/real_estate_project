"""
Chuyển đổi và chuẩn hóa dữ liệu Batdongsan (Bronze → Silver)
Focus: Data type conversion, standardization, and normalization only
"""

import sys
import os
import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    lit,
    regexp_replace,
    trim,
    when,
    upper,
    lower,
    split,
    element_at,
    round as spark_round,
    length,
    regexp_extract,
    concat,
    avg,
    count,
)
from pyspark.sql.types import StringType, DoubleType, BooleanType
import re

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from common.schema.batdongsan_schema import get_batdongsan_processed_schema
from common.utils.date_utils import (
    get_date_format,
    get_hdfs_path,
    generate_processing_id,
)
from common.utils.hdfs_utils import check_hdfs_path_exists, ensure_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session
from common.utils.address_parser import add_address_parsing_to_dataframe
from common.utils.field_mappings import (
    HOUSE_DIRECTION_MAPPING,
    LEGAL_STATUS_MAPPING,
    INTERIOR_MAPPING,
    HOUSE_TYPE_MAPPING,
    UNKNOWN_TEXT,
    UNKNOWN_ID,
)

# ===================== BATDONGSAN SPECIFIC MAPPING FUNCTIONS =====================


def map_batdongsan_house_direction_comprehensive(value):
    """Comprehensive mapping for house direction based on statistics analysis"""
    if value is None or str(value).strip() == "":
        return (UNKNOWN_TEXT, UNKNOWN_ID)

    value_clean = str(value).lower().strip().replace(" ", "").replace("-", "")

    # Direct mappings based on statistics
    if value_clean in ["đông", "dong"]:
        return ("Đông", HOUSE_DIRECTION_MAPPING["Đông"])
    elif value_clean in ["tây", "tay"]:
        return ("Tây", HOUSE_DIRECTION_MAPPING["Tây"])
    elif value_clean in ["nam"]:
        return ("Nam", HOUSE_DIRECTION_MAPPING["Nam"])
    elif value_clean in ["bắc", "bac"]:
        return ("Bắc", HOUSE_DIRECTION_MAPPING["Bắc"])
    elif value_clean in ["đôngbắc", "dongbac", "đông-bắc", "dong-bac"]:
        return ("Đông Bắc", HOUSE_DIRECTION_MAPPING["Đông Bắc"])
    elif value_clean in ["đôngnam", "dongnam", "đông-nam", "dong-nam"]:
        return ("Đông Nam", HOUSE_DIRECTION_MAPPING["Đông Nam"])
    elif value_clean in ["tâybắc", "taybac", "tây-bắc", "tay-bac"]:
        return ("Tây Bắc", HOUSE_DIRECTION_MAPPING["Tây Bắc"])
    elif value_clean in ["tâynam", "taynam", "tây-nam", "tay-nam"]:
        return ("Tây Nam", HOUSE_DIRECTION_MAPPING["Tây Nam"])
    else:
        return (UNKNOWN_TEXT, UNKNOWN_ID)


def map_batdongsan_legal_status_comprehensive(value):
    """Comprehensive mapping for legal status based on statistics analysis"""
    if value is None or str(value).strip() == "":
        return (UNKNOWN_TEXT, UNKNOWN_ID)

    value_clean = str(value).lower().strip()

    # Mapping based on user statistics - prioritize most common patterns
    # Group 1: "Đã có sổ" (Red book/Pink book related)
    red_book_patterns = [
        "sổ đỏ",
        "sổ hồng",
        "có sổ",
        "đã có sổ",
        "sổ đỏ/sổ hồng",
        "sổ hồng riêng",
        "sổ hồng chính chủ",
        "sổ đỏ chính chủ",
        "shr",
        "shcc",
        "sđcc",
        "sdcc",
        "bìa đỏ",
        "sổ đẹp",
        "sổ vuông vắn",
        "sổ sẵn",
        "sổ riêng",
        "sổ hồng cá nhân",
    ]

    for pattern in red_book_patterns:
        if pattern in value_clean:
            return ("Đã có sổ", LEGAL_STATUS_MAPPING["Đã có sổ"])

    # Group 2: "Đang chờ sổ"
    waiting_patterns = ["đang chờ sổ", "chờ sổ", "làm sổ"]
    for pattern in waiting_patterns:
        if pattern in value_clean:
            return ("Đang chờ sổ", LEGAL_STATUS_MAPPING["Đang chờ sổ"])

    # Group 3: "Không có sổ"
    no_legal_patterns = ["không pháp lý", "ko sổ", "không"]
    for pattern in no_legal_patterns:
        if pattern in value_clean:
            return ("Không có sổ", LEGAL_STATUS_MAPPING["Không có sổ"])

    # Group 4: "Sổ chung / Vi bằng"
    shared_patterns = [
        "hợp đồng mua bán",
        "hđmb",
        "vi bằng",
        "vbcn",
        "mua bán vi bằng",
        "ccvb",
        "giấy tay",
        "giấy tờ viết tay",
        "sổ chung",
    ]
    for pattern in shared_patterns:
        if pattern in value_clean:
            return ("Sổ chung / Vi bằng", LEGAL_STATUS_MAPPING["Sổ chung / Vi bằng"])

    # Group 5: "Giấy tờ viết tay" - handled above in shared patterns

    # If no match found
    return (UNKNOWN_TEXT, UNKNOWN_ID)


def map_batdongsan_interior_comprehensive(value):
    """Comprehensive mapping for interior based on statistics analysis"""
    if value is None or str(value).strip() == "":
        return (UNKNOWN_TEXT, UNKNOWN_ID)

    value_clean = str(value).lower().strip()

    # Group 1: "Cao cấp" - luxury keywords
    luxury_patterns = [
        "cao cấp",
        "nội thất cao cấp",
        "sang trọng",
        "xịn",
        "luxury",
        "5 sao",
        "nhập khẩu",
        "gỗ cao cấp",
        "full nội thất cao cấp",
        "nội thất sang trọng",
        "trang thiết bị cao cấp",
        "xịn sò",
        "cực kỳ đẹp",
        "nội thất xịn",
        "nội thất đẹp",
    ]

    for pattern in luxury_patterns:
        if pattern in value_clean:
            return ("Cao cấp", INTERIOR_MAPPING["Cao cấp"])

    # Group 2: "Đầy đủ" - fully furnished
    full_patterns = [
        "đầy đủ",
        "full",
        "nội thất đầy đủ",
        "full nội thất",
        "để lại toàn bộ nội thất",
        "trang bị đầy đủ",
        "nội thất",
        "đầy đủ tiện nghi",
        "hoàn thiện",
        "tặng nội thất",
    ]

    for pattern in full_patterns:
        if pattern in value_clean and not any(
            lux in value_clean for lux in luxury_patterns
        ):
            return ("Đầy đủ", INTERIOR_MAPPING["Đầy đủ"])

    # Group 3: "Cơ bản" - basic furnishing
    basic_patterns = [
        "cơ bản",
        "nội thất cơ bản",
        "một số nội thất",
        "điều hòa",
        "tủ lạnh",
        "giường",
        "tủ",
    ]

    for pattern in basic_patterns:
        if pattern in value_clean:
            return ("Cơ bản", INTERIOR_MAPPING["Cơ bản"])

    # Group 4: "Bàn giao thô" - unfurnished
    unfurnished_patterns = [
        "không nội thất",
        "nhà thô",
        "bàn giao thô",
        "thô",
        "giao thô",
        "hoàn thiện bên ngoài",
    ]

    for pattern in unfurnished_patterns:
        if pattern in value_clean:
            return ("Bàn giao thô", INTERIOR_MAPPING["Bàn giao thô"])

    # If no clear match but has content, default to basic
    if value_clean and value_clean != "k":
        return ("Đầy đủ", INTERIOR_MAPPING["Đầy đủ"])  # Most common case

    return (UNKNOWN_TEXT, UNKNOWN_ID)


def apply_batdongsan_comprehensive_mapping(df: DataFrame) -> DataFrame:
    """Apply comprehensive mapping for batdongsan text data using Spark SQL expressions"""
    logger = SparkJobLogger("batdongsan_comprehensive_mapping")

    # Use case-when expressions instead of UDFs to avoid serialization issues
    mapped_df = df

    # Map house_direction using CASE WHEN expressions
    mapped_df = mapped_df.withColumn(
        "house_direction_text",
        when(
            lower(trim(col("house_direction"))).rlike("(^|\\s)(đông|dong)(\\s|$)")
            & ~lower(trim(col("house_direction"))).rlike("(nam|bắc|bac)"),
            lit("Đông"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike("(^|\\s)(tây|tay)(\\s|$)")
            & ~lower(trim(col("house_direction"))).rlike("(nam|bắc|bac)"),
            lit("Tây"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike("(^|\\s)nam(\\s|$)")
            & ~lower(trim(col("house_direction"))).rlike("(đông|dong|tây|tay)"),
            lit("Nam"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike("(^|\\s)(bắc|bac)(\\s|$)")
            & ~lower(trim(col("house_direction"))).rlike("(đông|dong|tây|tay)"),
            lit("Bắc"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike(
                "(đông|dong).*(bắc|bac)|(bắc|bac).*(đông|dong)"
            ),
            lit("Đông Bắc"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike(
                "(đông|dong).*nam|nam.*(đông|dong)"
            ),
            lit("Đông Nam"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike(
                "(tây|tay).*(bắc|bac)|(bắc|bac).*(tây|tay)"
            ),
            lit("Tây Bắc"),
        )
        .when(
            lower(trim(col("house_direction"))).rlike("(tây|tay).*nam|nam.*(tây|tay)"),
            lit("Tây Nam"),
        )
        .otherwise(lit(UNKNOWN_TEXT)),
    ).withColumn(
        "house_direction_code",
        when(
            col("house_direction_text") == "Đông", lit(HOUSE_DIRECTION_MAPPING["Đông"])
        )
        .when(col("house_direction_text") == "Tây", lit(HOUSE_DIRECTION_MAPPING["Tây"]))
        .when(col("house_direction_text") == "Nam", lit(HOUSE_DIRECTION_MAPPING["Nam"]))
        .when(col("house_direction_text") == "Bắc", lit(HOUSE_DIRECTION_MAPPING["Bắc"]))
        .when(
            col("house_direction_text") == "Đông Bắc",
            lit(HOUSE_DIRECTION_MAPPING["Đông Bắc"]),
        )
        .when(
            col("house_direction_text") == "Đông Nam",
            lit(HOUSE_DIRECTION_MAPPING["Đông Nam"]),
        )
        .when(
            col("house_direction_text") == "Tây Bắc",
            lit(HOUSE_DIRECTION_MAPPING["Tây Bắc"]),
        )
        .when(
            col("house_direction_text") == "Tây Nam",
            lit(HOUSE_DIRECTION_MAPPING["Tây Nam"]),
        )
        .otherwise(lit(UNKNOWN_ID)),
    )

    # Map legal_status using CASE WHEN expressions
    mapped_df = mapped_df.withColumn(
        "legal_status_text",
        when(
            lower(trim(col("legal_status"))).rlike(
                "(sổ đỏ|sổ hồng|có sổ|đã có sổ|shr|shcc|sđcc|sdcc|bìa đỏ)"
            ),
            lit("Đã có sổ"),
        )
        .when(
            lower(trim(col("legal_status"))).rlike("(đang chờ sổ|chờ sổ|làm sổ)"),
            lit("Đang chờ sổ"),
        )
        .when(
            lower(trim(col("legal_status"))).rlike("(không pháp lý|ko sổ|không)"),
            lit("Không có sổ"),
        )
        .when(
            lower(trim(col("legal_status"))).rlike(
                "(hợp đồng mua bán|hđmb|vi bằng|vbcn|giấy tay|giấy tờ viết tay|sổ chung)"
            ),
            lit("Sổ chung / Vi bằng"),
        )
        .otherwise(lit(UNKNOWN_TEXT)),
    ).withColumn(
        "legal_status_code",
        when(
            col("legal_status_text") == "Đã có sổ",
            lit(LEGAL_STATUS_MAPPING["Đã có sổ"]),
        )
        .when(
            col("legal_status_text") == "Đang chờ sổ",
            lit(LEGAL_STATUS_MAPPING["Đang chờ sổ"]),
        )
        .when(
            col("legal_status_text") == "Không có sổ",
            lit(LEGAL_STATUS_MAPPING["Không có sổ"]),
        )
        .when(
            col("legal_status_text") == "Sổ chung / Vi bằng",
            lit(LEGAL_STATUS_MAPPING["Sổ chung / Vi bằng"]),
        )
        .otherwise(lit(UNKNOWN_ID)),
    )

    # Map interior using CASE WHEN expressions
    mapped_df = mapped_df.withColumn(
        "interior_text",
        when(
            lower(trim(col("interior"))).rlike(
                "(cao cấp|sang trọng|xịn|luxury|5 sao|nhập khẩu)"
            ),
            lit("Cao cấp"),
        )
        .when(
            lower(trim(col("interior"))).rlike(
                "(không nội thất|nhà thô|bàn giao thô|thô|giao thô)"
            ),
            lit("Bàn giao thô"),
        )
        .when(
            lower(trim(col("interior"))).rlike(
                "(cơ bản|nội thất cơ bản|một số nội thất|điều hòa|tủ lạnh|giường|tủ)"
            ),
            lit("Cơ bản"),
        )
        .when(
            lower(trim(col("interior"))).rlike(
                "(đầy đủ|full|nội thất|trang bị|tiện nghi|hoàn thiện|tặng)"
            )
            & ~lower(trim(col("interior"))).rlike("(cao cấp|sang trọng|xịn|luxury)"),
            lit("Đầy đủ"),
        )
        .when(
            trim(col("interior")).isNotNull()
            & (trim(col("interior")) != "")
            & (trim(col("interior")) != "k"),
            lit("Đầy đủ"),  # Default for non-empty values
        )
        .otherwise(lit(UNKNOWN_TEXT)),
    ).withColumn(
        "interior_code",
        when(col("interior_text") == "Cao cấp", lit(INTERIOR_MAPPING["Cao cấp"]))
        .when(col("interior_text") == "Đầy đủ", lit(INTERIOR_MAPPING["Đầy đủ"]))
        .when(col("interior_text") == "Cơ bản", lit(INTERIOR_MAPPING["Cơ bản"]))
        .when(
            col("interior_text") == "Bàn giao thô",
            lit(INTERIOR_MAPPING["Bàn giao thô"]),
        )
        .otherwise(lit(UNKNOWN_ID)),
    )

    # Create final dataframe with updated column names
    final_df = (
        mapped_df.drop("house_direction", "legal_status", "interior")
        .withColumnRenamed("house_direction_text", "house_direction")
        .withColumnRenamed("legal_status_text", "legal_status")
        .withColumnRenamed("interior_text", "interior")
    )

    logger.logger.info(
        "Completed comprehensive batdongsan text to standardized mapping"
    )
    return final_df


# ===================== PRICE PROCESSING =====================


# ===================== PRICE PROCESSING =====================


def process_price_data(df: DataFrame) -> DataFrame:
    """Process and standardize price information"""
    logger = SparkJobLogger("price_processing")

    # Process main price field
    df_price = df.withColumn("price_text", trim(col("price")))
    df_price = df_price.withColumn("price_per_m2_text", trim(col("price_per_m2")))

    # Detect swapped fields based on units
    # price should contain "tỷ" or "triệu" without "/m²"
    # price_per_m2 should contain "/m²" or be much smaller value
    df_price = df_price.withColumn(
        "price_has_per_m2_unit",
        lower(col("price_text")).contains("/m²")
        | lower(col("price_text")).contains("/m2")
        | lower(col("price_text")).contains("trên m²")
        | lower(col("price_text")).contains("trên m2"),
    ).withColumn(
        "price_per_m2_has_total_unit",
        (
            lower(col("price_per_m2_text")).contains("tỷ")
            | lower(col("price_per_m2_text")).contains("ty")
        )
        & ~(lower(col("price_per_m2_text")).contains("/m²"))
        & ~(lower(col("price_per_m2_text")).contains("/m2")),
    )

    # Swap fields when detected
    df_price = df_price.withColumn(
        "price_text_corrected",
        when(
            col("price_has_per_m2_unit") & col("price_per_m2_has_total_unit"),
            col("price_per_m2_text"),  # Use price_per_m2 as price
        ).otherwise(col("price_text")),
    ).withColumn(
        "price_per_m2_text_corrected",
        when(
            col("price_has_per_m2_unit") & col("price_per_m2_has_total_unit"),
            col("price_text"),  # Use price as price_per_m2
        ).otherwise(col("price_per_m2_text")),
    )

    # Update working columns
    df_price = df_price.withColumn("price_text", col("price_text_corrected"))
    df_price = df_price.withColumn(
        "price_per_m2_text", col("price_per_m2_text_corrected")
    )

    # Check for negotiable prices and invalid units
    df_price = df_price.withColumn(
        "is_negotiable",
        when(
            lower(col("price_text")).contains("thỏa thuận")
            | lower(col("price_text")).contains("thoathuan"),
            lit(True),
        ).otherwise(lit(False)),
    ).withColumn(
        "has_invalid_unit",
        when(
            lower(col("price_text")).contains("nghìn")
            | lower(col("price_text")).contains("nghin")
            | lower(col("price_text")).contains("k ")
            | lower(col("price_text")).rlike("\\d+k$"),  # số + k ở cuối
            lit(True),
        ).otherwise(lit(False)),
    )

    # Helper function to normalize decimal separator and extract numeric value
    # For Batdongsan: Vietnamese format where "," is decimal separator and "." is thousand separator
    # Remove dots (thousand separators) and convert commas to decimal points
    df_price = df_price.withColumn(
        "price_normalized",
        when(col("is_negotiable"), lit(None)).otherwise(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        col("price_text"), "\\.", ""
                    ),  # Remove dots (thousand separators)
                    ",",
                    ".",  # Convert comma to decimal point
                ),
                "[^0-9\\.]",
                "",  # Remove all non-numeric except decimal point
            )
        ),
    )

    # Convert price based on unit (tỷ, triệu, etc.)
    df_price = df_price.withColumn(
        "price",
        when(col("is_negotiable"), lit(None))
        .when(
            lower(col("price_text")).contains("tỷ")
            | lower(col("price_text")).contains("ty"),
            col("price_normalized").cast("double") * 1_000_000_000,
        )
        .when(
            lower(col("price_text")).contains("triệu")
            | lower(col("price_text")).contains("trieu"),
            col("price_normalized").cast("double") * 1_000_000,
        )
        .otherwise(col("price_normalized").cast("double")),
    )

    # Process price per m2 field
    df_price = df_price.withColumn("price_per_m2_text", trim(col("price_per_m2")))

    # Helper function to normalize decimal separator for price_per_m2
    # For Batdongsan: Vietnamese format where "," is decimal separator and "." is thousand separator
    # Remove dots (thousand separators) and convert commas to decimal points
    df_price = df_price.withColumn(
        "price_per_m2_normalized",
        # Step 1: Remove dots (thousand separators)
        # Step 2: Replace commas with dots (for decimal)
        # Step 3: Remove any remaining non-numeric characters except decimal point
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    col("price_per_m2_text"), "\\.", ""
                ),  # Remove dots (thousand separators)
                ",",
                ".",  # Convert comma to decimal point
            ),
            "[^0-9\\.]",
            "",  # Remove all non-numeric except decimal point
        ),
    )

    df_price = df_price.withColumn(
        "price_per_m2",
        when(
            lower(col("price_per_m2_text")).contains("tỷ")
            | lower(col("price_per_m2_text")).contains("ty"),
            col("price_per_m2_normalized").cast("double") * 1_000_000_000,
        )
        .when(
            lower(col("price_per_m2_text")).contains("triệu")
            | lower(col("price_per_m2_text")).contains("trieu"),
            col("price_per_m2_normalized").cast("double") * 1_000_000,
        )
        .otherwise(col("price_per_m2_normalized").cast("double")),
    )

    # Calculate price per m2 if both price and area are available and price_per_m2 is null
    df_price = df_price.withColumn(
        "price_per_m2",
        when(
            col("price_per_m2").isNull()
            & col("price").isNotNull()
            & col("area").isNotNull()
            & (col("area") > 0),
            spark_round(col("price") / col("area"), 2),
        ).otherwise(col("price_per_m2")),
    )

    logger.logger.info("Price processing completed")

    # Filter out records with invalid units and drop helper columns
    df_cleaned = df_price.filter(
        ~col("has_invalid_unit")  # Filter out records with "nghìn" units
    ).drop(
        "price_text",
        "price_per_m2_text",
        "price_normalized",
        "price_per_m2_normalized",
        "price_text_corrected",
        "price_per_m2_text_corrected",
        "price_has_per_m2_unit",
        "price_per_m2_has_total_unit",
        "has_invalid_unit",
        "is_negotiable",
    )

    return df_cleaned


# ===================== COORDINATE VALIDATION =====================


def validate_coordinates(df: DataFrame) -> DataFrame:
    """Validate and clean coordinate data for Vietnam region"""
    logger = SparkJobLogger("coordinate_validation")

    # Valid coordinates for Vietnam: Lat 8.0-23.5, Lon 102.0-110.0
    df_validated = df.withColumn(
        "latitude",
        when(
            (col("latitude") >= 8.0) & (col("latitude") <= 23.5), col("latitude")
        ).otherwise(lit(None)),
    ).withColumn(
        "longitude",
        when(
            (col("longitude") >= 102.0) & (col("longitude") <= 110.0), col("longitude")
        ).otherwise(lit(None)),
    )

    logger.logger.info("Coordinate validation completed")
    return df_validated


# ===================== DATA TYPE CONVERSION =====================


def convert_data_types(df: DataFrame) -> DataFrame:
    """Convert and clean data types for standardization"""
    logger = SparkJobLogger("data_type_conversion")

    # Convert numeric fields with proper cleaning
    # Special handling for area: For Batdongsan Vietnamese format - remove dots, convert commas to decimal points
    numeric_df = (
        df.withColumn(
            "area",
            # Step 1: Remove dots (thousand separators)
            # Step 2: Replace commas with dots (for decimal)
            # Step 3: Remove any remaining non-numeric characters except decimal point
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        col("area"), "\\.", ""
                    ),  # Remove dots (thousand separators)
                    ",",
                    ".",  # Convert comma to decimal point
                ),
                "[^0-9\\.]",
                "",  # Remove all non-numeric except decimal point
            ).cast("double"),
        )
        .withColumn(
            "bathroom", regexp_replace(col("bathroom"), "[^0-9]", "").cast("double")
        )
        .withColumn(
            "bedroom", regexp_replace(col("bedroom"), "[^0-9]", "").cast("double")
        )
        .withColumn(
            "floor_count",
            regexp_replace(col("floor_count"), "[^0-9]", "").cast("double"),
        )
        .withColumn(
            "facade_width",
            # For Batdongsan Vietnamese format - remove dots, convert commas to decimal points
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        col("facade_width"), "\\.", ""
                    ),  # Remove dots (thousand separators)
                    ",",
                    ".",  # Convert comma to decimal point
                ),
                "[^0-9\\.]",
                "",  # Remove all non-numeric except decimal point
            ).cast("double"),
        )
        .withColumn(
            "road_width",
            # For Batdongsan Vietnamese format - remove dots, convert commas to decimal points
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        col("road_width"), "\\.", ""
                    ),  # Remove dots (thousand separators)
                    ",",
                    ".",  # Convert comma to decimal point
                ),
                "[^0-9\\.]",
                "",  # Remove all non-numeric except decimal point
            ).cast("double"),
        )
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
    )

    logger.logger.info("Data type conversion completed")
    return numeric_df


# ===================== MAIN TRANSFORMATION =====================


def transform_batdongsan_data(
    spark: SparkSession, input_date=None, property_type="house"
) -> DataFrame:
    """Transform Batdongsan data from Bronze to Silver layer"""
    logger = SparkJobLogger("transform_batdongsan_data")
    logger.start_job({"input_date": input_date, "property_type": property_type})

    if input_date is None:
        input_date = get_date_format()

    processing_id = generate_processing_id("batdongsan_transform")

    # Define paths
    bronze_path = get_hdfs_path(
        "/data/realestate/processed/bronze", "batdongsan", property_type, input_date
    )
    silver_path = get_hdfs_path(
        "/data/realestate/processed/silver", "batdongsan", property_type, input_date
    )

    logger.logger.info(f"Reading bronze data from: {bronze_path}")
    logger.logger.info(f"Writing silver data to: {silver_path}")

    ensure_hdfs_path(spark, silver_path)

    try:
        # Read bronze data
        bronze_file = f"{bronze_path}/batdongsan_{input_date.replace('-', '')}.parquet"
        if not check_hdfs_path_exists(spark, bronze_file):
            error_message = f"Bronze data not found: {bronze_file}"
            logger.log_error(error_message)
            raise FileNotFoundError(error_message)

        bronze_df = spark.read.parquet(bronze_file)
        logger.log_dataframe_info(bronze_df, "bronze_data")

        # Step 1: Data type conversions
        logger.logger.info("Step 1: Converting data types...")
        numeric_df = convert_data_types(bronze_df)

        # Cache to break lineage and prevent code generation issues
        numeric_df.cache()
        logger.logger.info(f"Cached numeric conversion: {numeric_df.count():,} records")

        # Step 2: Price processing
        logger.logger.info("Step 2: Processing price data...")
        price_df = process_price_data(numeric_df)

        # Step 3: Comprehensive categorical mappings with ID assignment
        logger.logger.info("Step 3: Applying comprehensive categorical mappings...")

        # Apply comprehensive mapping for batdongsan text data
        mapped_df = apply_batdongsan_comprehensive_mapping(price_df)

        # Cache after categorical mapping to break lineage
        mapped_df.cache()
        logger.logger.info(
            f"Cached comprehensive categorical mapping: {mapped_df.count():,} records"
        )

        # Step 4: Apply coordinate validation
        logger.logger.info("Step 4: Validating coordinates...")
        validated_df = validate_coordinates(mapped_df)

        # Step 5: Convert timestamps
        timestamp_df = validated_df.withColumn(
            "crawl_timestamp", to_timestamp(col("crawl_timestamp"))
        ).withColumn("posted_date", to_timestamp(col("posted_date")))

        # Step 6: Add metadata and quality scoring
        logger.logger.info("Step 6: Adding quality scoring...")

        # Handle source column - either use existing source or rename data_source to source
        pre_final_df = timestamp_df
        if (
            "source" not in timestamp_df.columns
            and "data_source" in timestamp_df.columns
        ):
            pre_final_df = timestamp_df.withColumnRenamed("data_source", "source")
        elif "source" not in timestamp_df.columns:
            # If neither exists, add source column with default value
            pre_final_df = timestamp_df.withColumn("source", lit("batdongsan"))

        # Add missing fields that are expected by common schema
        pre_final_df = (
            pre_final_df.withColumn(
                "width", lit(None).cast(DoubleType())  # Chotot specific
            )
            .withColumn("length", lit(None).cast(DoubleType()))  # Chotot specific
            .withColumn("living_size", lit(None).cast(DoubleType()))  # Chotot specific
        )

        final_df = pre_final_df.withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn("processing_id", lit(processing_id))

        # Step 7: ADDRESS PARSING (add before filtering)
        logger.logger.info("Step 7: Parsing addresses...")

        # ===================== ADDRESS PARSING =====================
        logger.logger.info("Parsing addresses...")

        # Parse addresses - Batdongsan chỉ có location, không có separate fields
        # json_path sẽ dùng default path trong address_parser.py
        address_parsed_df = add_address_parsing_to_dataframe(
            df=final_df, location_col="location"
        )

        logger.log_dataframe_info(address_parsed_df, "after_address_parsing")

        # address_parsed_df now contains both names and IDs for all address components
        logger.logger.info("Address parsing with IDs completed")

        # Step 8: Simple quality filter (detailed scoring will be done in unify step)
        final_filtered_df = address_parsed_df.filter(
            col("area").isNotNull()
            & (col("area") > 0)
            & (
                (col("price").isNotNull() & (col("price") > 0))
                | col("price_per_m2").isNotNull()
            )
            & col("location").isNotNull()
            & (length(col("location")) > 5)
        )

        logger.log_dataframe_info(final_filtered_df, "silver_data")

        # Step 9: Write silver data
        output_path = os.path.join(
            silver_path, f"batdongsan_{input_date.replace('-', '')}.parquet"
        )

        final_filtered_df.write.mode("overwrite").parquet(output_path)

        final_count = final_filtered_df.count()
        logger.logger.info(
            f"Successfully processed {final_count:,} silver records to {output_path}"
        )

        # Simple logging without complex quality statistics
        logger.logger.info(
            f"Records with valid area: {final_filtered_df.filter(col('area').isNotNull()).count():,}"
        )
        logger.logger.info(
            f"Records with valid price: {final_filtered_df.filter(col('price').isNotNull()).count():,}"
        )
        logger.logger.info("Detailed quality scoring will be performed in unify step")

        logger.end_job()
        return final_filtered_df

    except Exception as e:
        logger.log_error("Error processing data", e)
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Transform Batdongsan Data to Silver")
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

    # Initialize Spark session
    spark = create_spark_session("Transform Batdongsan Data")

    try:
        # Transform data
        transform_batdongsan_data(spark, args.date, args.property_type)

    finally:
        spark.stop()
