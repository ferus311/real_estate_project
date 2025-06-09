"""
Unified Data Quality Scoring System
Tính điểm chất lượng dữ liệu thống nhất cho tất cả nguồn
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    length,
)
from pyspark.sql.types import DoubleType


def calculate_data_quality_score(df: DataFrame) -> DataFrame:
    """
    Tính điểm chất lượng dữ liệu thống nhất cho tất cả nguồn

    Args:
        df: DataFrame cần tính điểm

    Returns:
        DataFrame với các cột điểm chất lượng được thêm vào
    """

    # Step 1: Tạo các boolean flags cho data quality
    df_with_flags = (
        df.withColumn("has_valid_price", col("price").isNotNull() & (col("price") > 0))
        .withColumn("has_valid_area", col("area").isNotNull() & (col("area") > 0))
        .withColumn(
            "has_valid_location",
            col("location").isNotNull() & (length(col("location")) > 5),
        )
        .withColumn(
            "has_coordinates",
            col("latitude").isNotNull() & col("longitude").isNotNull(),
        )
        .withColumn(
            "has_valid_bedroom", col("bedroom").isNotNull() & (col("bedroom") > 0)
        )
        .withColumn(
            "has_valid_bathroom", col("bathroom").isNotNull() & (col("bathroom") > 0)
        )
    )

    # Step 2: Tính điểm từng thành phần (tránh CodeGenerator issues)
    df_scored = (
        df_with_flags.withColumn(
            "area_score", when(col("has_valid_area"), lit(20)).otherwise(lit(0))
        )
        .withColumn(
            "price_score", when(col("has_valid_price"), lit(20)).otherwise(lit(0))
        )
        .withColumn(
            "bedroom_score", when(col("has_valid_bedroom"), lit(15)).otherwise(lit(0))
        )
        .withColumn(
            "bathroom_score", when(col("has_valid_bathroom"), lit(15)).otherwise(lit(0))
        )
        .withColumn(
            "location_score", when(col("has_valid_location"), lit(15)).otherwise(lit(0))
        )
        .withColumn(
            "coords_score", when(col("has_coordinates"), lit(15)).otherwise(lit(0))
        )
    )

    # Step 3: Tính tổng điểm chất lượng
    df_final_score = df_scored.withColumn(
        "data_quality_score",
        col("area_score")
        + col("price_score")
        + col("bedroom_score")
        + col("bathroom_score")
        + col("location_score")
        + col("coords_score"),
    )

    # Step 4: Tính traditional quality score (tương thích với Chotot logic cũ)
    df_traditional = df_final_score.withColumn(
        "traditional_quality_score",
        col("area_score")
        + col("price_score")
        + col("bedroom_score")
        + col("bathroom_score")
        + col("location_score")
        + col("coords_score"),
    )

    # Step 5: Cleanup - drop intermediate scoring columns
    df_clean = df_traditional.drop(
        "area_score",
        "price_score",
        "bedroom_score",
        "bathroom_score",
        "location_score",
        "coords_score",
    )

    return df_clean


def add_data_quality_issues_flag(df: DataFrame) -> DataFrame:
    """
    Thêm cột data_quality_issues để flag các vấn đề chất lượng
    Tương thích với logic cũ của Chotot

    Args:
        df: DataFrame đã có quality score

    Returns:
        DataFrame với cột data_quality_issues
    """

    df_with_issues = df.withColumn(
        "data_quality_issues",
        when(
            col("has_valid_price")
            & col("has_valid_area")
            & col("has_valid_location")
            & col("has_coordinates")
            & col("has_valid_bedroom")
            & col("has_valid_bathroom"),
            lit("NO_ISSUES"),
        )
        .when(~col("has_valid_price"), lit("MISSING_PRICE"))
        .when(~col("has_valid_area"), lit("MISSING_AREA"))
        .when(~col("has_valid_location"), lit("MISSING_LOCATION"))
        .otherwise(lit("PARTIAL_DATA")),
    )

    return df_with_issues


def get_quality_level(score: int) -> str:
    """
    Xác định level chất lượng dựa trên điểm số

    Args:
        score: Điểm chất lượng (0-100)

    Returns:
        String mô tả level chất lượng
    """
    if score >= 90:
        return "PREMIUM"
    elif score >= 80:
        return "HIGH"
    elif score >= 70:
        return "GOOD"
    elif score >= 50:
        return "FAIR"
    else:
        return "LOW"


def add_quality_level(df: DataFrame) -> DataFrame:
    """
    Thêm cột quality_level dựa trên data_quality_score

    Args:
        df: DataFrame có cột data_quality_score

    Returns:
        DataFrame với cột quality_level
    """

    df_with_level = df.withColumn(
        "quality_level",
        when(col("data_quality_score") >= 90, lit("PREMIUM"))
        .when(col("data_quality_score") >= 80, lit("HIGH"))
        .when(col("data_quality_score") >= 70, lit("GOOD"))
        .when(col("data_quality_score") >= 50, lit("FAIR"))
        .otherwise(lit("LOW")),
    )

    return df_with_level
