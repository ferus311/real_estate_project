"""
🧹 ML Data Cleaning Utilities
============================

Comprehensive data cleaning, validation, and preprocessing utilities
for real estate ML pipelines.

Functions:
- Data type optimization
- Missing value handling
- Duplicate removal
- Outlier detection and removal
- Range validation
- Data quality scoring

Author: ML Team
Date: June 2025
"""

import logging
from typing import Dict, List, Tuple, Any, Optional, Union
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    isnan,
    isnull,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    stddev,
    variance,
    mean,
    sqrt,
    log,
    regexp_replace,
    length,
    split,
    size,
    year,
    month,
    dayofmonth,
    expr,
    approx_count_distinct,
    abs as spark_abs,
    broadcast,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    FloatType,
    LongType,
    BooleanType,
    TimestampType,
    DateType,
)
from pyspark.sql.window import Window
from pyspark.ml.feature import (
    QuantileDiscretizer,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaner:
    """
    🧹 Comprehensive Data Cleaning and Preprocessing

    Handles all data cleaning operations including:
    - Data type optimization
    - Missing value imputation
    - Duplicate removal
    - Outlier detection/removal
    - Range validation
    """

    def __init__(self, spark: SparkSession):
        """Initialize the data cleaner."""
        self.spark = spark
        logger.info("🧹 Data Cleaner initialized")

    def full_cleaning_pipeline(
        self, df: DataFrame, config: Dict[str, Any]
    ) -> DataFrame:
        """
        🚀 Run complete data cleaning pipeline

        Args:
            df: Input DataFrame
            config: Cleaning configuration

        Returns:
            DataFrame: Cleaned data
        """
        logger.info("🚀 Starting full data cleaning pipeline")

        # Step 0: Debug initial data quality
        debug_info = self.debug_data_quality(df)
        logger.info(f"📊 Initial data: {debug_info['total_records']:,} records")

        # Step 1: Basic validation
        df = self.validate_basic_data(df)

        # Step 2: Data type optimization
        df = self.optimize_data_types(df)

        # Debug after type conversion
        logger.info("🔍 Data quality after type conversion:")
        self.debug_data_quality(df)

        # Step 3: Remove duplicates
        df = self.remove_duplicates(df)

        # Step 4: Handle missing values
        df = self.handle_missing_values(df, config)

        # Step 5: Validate numeric ranges
        df = self.validate_numeric_ranges(df, config)

        # Step 6: Remove outliers
        df = self.remove_outliers(df, config)

        # Step 7: Add data quality score
        df = self.add_data_quality_score(df)

        # Final debug check
        final_debug = self.debug_data_quality(df)
        logger.info(
            f"✅ Data cleaning completed: {final_debug['total_records']:,} records"
        )

        # Validate that we still have target variable
        if "price" in df.columns:
            price_count = df.filter(col("price").isNotNull()).count()
            logger.info(
                f"🎯 Final dataset has {price_count:,} records with valid price"
            )
        else:
            logger.error("🚨 CRITICAL: Price column missing from final dataset!")

        return df

    def validate_basic_data(self, df: DataFrame) -> DataFrame:
        """✅ Basic data validation and cleaning"""
        logger.info("✅ Validating basic data quality")

        initial_count = df.count()
        logger.info(f"📊 Starting validation with {initial_count:,} records")

        # Remove records with critical missing values (with data type-aware filtering)
        critical_columns = ["price", "area", "latitude", "longitude"]

        for col_name in critical_columns:
            if col_name in df.columns:
                before_count = df.count()
                col_data_type = dict(df.dtypes)[col_name]

                # Apply data type-aware filtering
                if col_data_type == "string":
                    df = df.filter(
                        col(col_name).isNotNull()
                        & (col(col_name) != "")
                        & (col(col_name) != "null")
                        & (col(col_name) != "NULL")
                    )
                else:
                    if col_name == "area":
                        df = df.filter(col(col_name).isNotNull() & (col(col_name) >= 0))
                    else:
                        df = df.filter(col(col_name).isNotNull() & (col(col_name) > 0))

                after_count = df.count()
                if before_count != after_count:
                    logger.info(
                        f"🔄 Removed {before_count - after_count:,} records with invalid {col_name}"
                    )

        logger.info(f"📊 After basic validation: {df.count():,} records")
        return df

    def optimize_data_types(self, df: DataFrame) -> DataFrame:
        """🔧 Optimize data types for better performance and proper calculations"""
        logger.info("🔧 Optimizing data types")

        # Define columns that should be converted from string to numeric
        numeric_columns = {
            "price": DoubleType(),
            "area": DoubleType(),
            "latitude": DoubleType(),
            "longitude": DoubleType(),
            "price_per_m2": DoubleType(),
            "bedroom": IntegerType(),
            "bathroom": IntegerType(),
            "floor_count": IntegerType(),
            "width": DoubleType(),
            "length": DoubleType(),
            "living_size": DoubleType(),
            "facade_width": DoubleType(),
            "road_width": DoubleType(),
        }

        for col_name, target_type in numeric_columns.items():
            if col_name in df.columns:
                current_type = dict(df.dtypes)[col_name]

                if current_type == "string":
                    logger.info(
                        f"🔄 Converting {col_name}: {current_type} → {target_type.typeName()}"
                    )

                    # Sample data first to understand format
                    sample_data = (
                        df.filter(col(col_name).isNotNull())
                        .select(col_name)
                        .limit(5)
                        .collect()
                    )
                    if sample_data:
                        sample_values = [str(row[col_name]) for row in sample_data]
                        logger.info(f"🔍 Sample {col_name} values: {sample_values}")

                    # Enhanced conversion for scientific notation and regular numbers
                    df = df.withColumn(
                        col_name,
                        when(
                            # Check if it's a scientific notation (e.g., 9.0E9, 3.9E10)
                            col(col_name).rlike("^[0-9]*\\.?[0-9]+[Ee][+-]?[0-9]+$"),
                            col(col_name).cast(
                                target_type
                            ),  # Direct cast works for scientific notation
                        )
                        .when(
                            # Regular number with possible commas/spaces
                            regexp_replace(col(col_name), "[^0-9.]", "").rlike(
                                "^[0-9]+\\.?[0-9]*$"
                            ),
                            regexp_replace(col(col_name), "[^0-9.]", "").cast(
                                target_type
                            ),
                        )
                        .when(
                            # Handle null/empty strings
                            col(col_name).isNull() | (col(col_name) == ""),
                            lit(None).cast(target_type),
                        )
                        .otherwise(
                            lit(None).cast(
                                target_type
                            )  # Default to null for unparseable values
                        ),
                    )

                    # Count successful conversions and show samples
                    converted_count = df.filter(col(col_name).isNotNull()).count()
                    logger.info(
                        f"✅ Successfully converted {converted_count:,} {col_name} values"
                    )

                    # Show sample converted values
                    if converted_count > 0:
                        sample_converted = (
                            df.filter(col(col_name).isNotNull())
                            .select(col_name)
                            .limit(3)
                            .collect()
                        )
                        converted_values = [row[col_name] for row in sample_converted]
                        logger.info(
                            f"📊 Sample converted {col_name}: {converted_values}"
                        )

                        # Show min/max for validation
                        if col_name in ["price", "price_per_m2"]:
                            price_range = df.select(
                                spark_min(col_name).alias("min"),
                                spark_max(col_name).alias("max"),
                            ).collect()[0]
                            logger.info(
                                f"📏 {col_name} range: {price_range['min']:,.0f} - {price_range['max']:,.0f}"
                            )

                else:
                    logger.info(f"✅ {col_name} already {current_type} type")

        logger.info("✅ Data type optimization completed")
        return df

    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """🔄 Remove duplicate records"""
        logger.info("🔄 Removing duplicate records")

        initial_count = df.count()

        # Remove duplicates based on ID and date
        if "id" in df.columns and "data_date" in df.columns:
            df = df.dropDuplicates(["id", "data_date"])
        elif "id" in df.columns:
            df = df.dropDuplicates(["id"])
        else:
            logger.warning("⚠️ No ID column found for duplicate removal")

        final_count = df.count()
        removed = initial_count - final_count

        if removed > 0:
            logger.info(f"🔄 Removed {removed:,} duplicate records")
        else:
            logger.info("✅ No duplicates found")

        return df

    def handle_missing_values(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """🔄 Intelligent missing value imputation"""
        logger.info("🔄 Handling missing values")

        total_count = df.count()
        if total_count == 0:
            logger.warning("⚠️ No records to process for missing values")
            return df

        missing_threshold = config.get("missing_threshold", 0.3)

        # Define protected columns that should NEVER be dropped (target variables, essential features)
        protected_columns = {
            "price",
            "price_per_m2",
            "area",
            "latitude",
            "longitude",
            "id",
        }

        # Calculate missing percentages
        missing_stats = {}
        for col_name in df.columns:
            missing_count = df.filter(col(col_name).isNull()).count()
            missing_pct = missing_count / total_count if total_count > 0 else 0
            missing_stats[col_name] = missing_pct

            # Log missing percentage for all columns
            if missing_pct > 0:
                status = "🛡️ PROTECTED" if col_name in protected_columns else ""
                logger.info(f"📊 {col_name}: {missing_pct:.1%} missing {status}")

        # Drop columns with too many missing values (but protect target variables and essential columns)
        columns_to_drop = [
            col_name
            for col_name, pct in missing_stats.items()
            if pct > missing_threshold and col_name not in protected_columns
        ]

        if columns_to_drop:
            logger.info(
                f"🗑️ Dropping columns with >{missing_threshold*100}% missing: {columns_to_drop}"
            )
            df = df.drop(*columns_to_drop)

        # Log if any protected columns have high missing values (but don't drop them)
        high_missing_protected = [
            col_name
            for col_name in protected_columns
            if col_name in missing_stats and missing_stats[col_name] > missing_threshold
        ]
        if high_missing_protected:
            logger.warning(
                f"⚠️ Protected columns with >{missing_threshold*100}% missing (keeping anyway): {high_missing_protected}"
            )

        # Impute remaining missing values with special handling for protected columns
        for col_name in df.columns:
            if missing_stats.get(col_name, 0) > 0:
                col_type = dict(df.dtypes)[col_name]

                # Special handling for target variables - remove records instead of imputing
                if col_name in ["price", "price_per_m2"]:
                    before_count = df.count()
                    df = df.filter(col(col_name).isNotNull())
                    after_count = df.count()
                    if before_count != after_count:
                        logger.info(
                            f"🎯 Removed {before_count - after_count:,} records with missing target variable '{col_name}'"
                        )

                # Special handling for essential location/property features
                elif col_name in ["latitude", "longitude", "area"]:
                    if col_type in ["double", "float", "integer", "long"]:
                        # For essential features, use median imputation
                        median_val = df.approxQuantile(col_name, [0.5], 0.1)[0]
                        df = df.fillna({col_name: median_val})
                        logger.info(
                            f"🔢 Imputed essential feature {col_name} with median: {median_val}"
                        )

                # Regular imputation for other columns
                elif col_type in ["double", "float", "integer", "long"]:
                    # Numeric: use median
                    median_val = df.approxQuantile(col_name, [0.5], 0.1)[0]
                    df = df.fillna({col_name: median_val})
                    logger.info(
                        f"🔢 Imputed {col_name} (numeric) with median: {median_val}"
                    )
                else:
                    # String: use "Unknown"
                    df = df.fillna({col_name: "Unknown"})
                    logger.info(f"📝 Imputed {col_name} (string) with 'Unknown'")

        return df

    def validate_numeric_ranges(
        self, df: DataFrame, config: Dict[str, Any]
    ) -> DataFrame:
        """🔢 Validate numeric ranges with detailed logging"""
        logger.info("🔢 Validating numeric ranges")

        initial_count = df.count()

        # Filter valid prices with detailed logging
        if "price" in df.columns:
            # More realistic price ranges for Vietnamese real estate
            price_min = config.get(
                "price_min", 100000000
            )  # 100M VND (reasonable minimum)
            price_max = config.get(
                "price_max", 100000000000
            )  # 100B VND (very high-end)

            # Check current price statistics BEFORE filtering
            price_stats = df.select(
                spark_min("price").alias("min_price"),
                spark_max("price").alias("max_price"),
                avg("price").alias("avg_price"),
                count("price").alias("count_price"),
            ).collect()[0]

            logger.info(f"📊 Current price statistics (before range filter):")
            if price_stats["min_price"]:
                logger.info(f"   - Min: {price_stats['min_price']:,.0f} VND")
            if price_stats["max_price"]:
                logger.info(f"   - Max: {price_stats['max_price']:,.0f} VND")
            if price_stats["avg_price"]:
                logger.info(f"   - Avg: {price_stats['avg_price']:,.0f} VND")
            logger.info(f"   - Count: {price_stats['count_price']:,}")

            logger.info(
                f"📏 Applying price filter range: {price_min:,.0f} - {price_max:,.0f} VND"
            )

            before_count = df.count()
            df = df.filter(
                (col("price") >= price_min)
                & (col("price") <= price_max)
                & (col("price").isNotNull())
            )
            after_count = df.count()

            if before_count != after_count:
                logger.info(
                    f"🔄 Price filter: removed {before_count - after_count:,} records"
                )
                if after_count == 0:
                    logger.error(
                        "🚨 CRITICAL: All records filtered out by price range!"
                    )
                    logger.error(
                        f"🚨 Consider adjusting price_min ({price_min:,}) or price_max ({price_max:,})"
                    )
            else:
                logger.info("✅ All records pass price range validation")

        # Filter valid areas
        if "area" in df.columns:
            area_min = config.get("area_min", 5)  # 5 sqm
            area_max = config.get("area_max", 50000)  # 50,000 sqm

            before_count = df.count()
            df = df.filter(
                (col("area") >= area_min)
                & (col("area") <= area_max)
                & (col("area").isNotNull())
            )
            after_count = df.count()

            if before_count != after_count:
                logger.info(
                    f"🔄 Area filter: removed {before_count - after_count:,} records"
                )

        final_count = df.count()
        logger.info(f"📊 Range validation: {initial_count:,} → {final_count:,} records")
        return df

    def remove_outliers(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """🎯 Remove outliers using configurable methods"""
        logger.info("🎯 Removing outliers")

        method = config.get("outlier_method", "iqr")

        if method == "iqr":
            return self._remove_outliers_iqr(df, config)
        elif method == "zscore":
            return self._remove_outliers_zscore(df, config)
        elif method == "none":
            logger.info("⏭️ Outlier removal disabled")
            return df
        else:
            logger.warning(f"⚠️ Unknown outlier method: {method}, using IQR")
            return self._remove_outliers_iqr(df, config)

    def _remove_outliers_iqr(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """Remove outliers using IQR method"""
        logger.info("📊 Removing outliers using IQR method")

        outlier_columns = ["price", "area", "price_per_m2"]
        multiplier = config.get("iqr_multiplier", 1.5)
        initial_count = df.count()

        for col_name in outlier_columns:
            if col_name in df.columns:
                # Calculate quantiles
                quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.1)
                if len(quantiles) == 2:
                    q1, q3 = quantiles
                    iqr = q3 - q1
                    lower_bound = q1 - multiplier * iqr
                    upper_bound = q3 + multiplier * iqr

                    before_count = df.count()
                    df = df.filter(
                        (col(col_name) >= lower_bound) & (col(col_name) <= upper_bound)
                    )
                    after_count = df.count()

                    if before_count != after_count:
                        logger.info(
                            f"🎯 {col_name} IQR filter: removed {before_count - after_count:,} outliers"
                        )

        final_count = df.count()
        logger.info(
            f"📊 IQR outlier removal: {initial_count:,} → {final_count:,} records"
        )
        return df

    def _remove_outliers_zscore(
        self, df: DataFrame, config: Dict[str, Any]
    ) -> DataFrame:
        """Remove outliers using Z-score method"""
        logger.info("📊 Removing outliers using Z-score method")

        outlier_columns = ["price", "area", "price_per_m2"]
        threshold = config.get("outlier_threshold", 3.0)
        initial_count = df.count()

        for col_name in outlier_columns:
            if col_name in df.columns:
                # Calculate mean and std
                stats = df.select(
                    avg(col_name).alias("mean"), stddev(col_name).alias("std")
                ).collect()[0]
                mean_val = stats["mean"]
                std_val = stats["std"]

                if std_val and std_val > 0:  # Avoid division by zero
                    before_count = df.count()
                    df = df.filter(
                        spark_abs((col(col_name) - mean_val) / std_val) <= threshold
                    )
                    after_count = df.count()

                    if before_count != after_count:
                        logger.info(
                            f"🎯 {col_name} Z-score filter: removed {before_count - after_count:,} outliers"
                        )

        final_count = df.count()
        logger.info(
            f"📊 Z-score outlier removal: {initial_count:,} → {final_count:,} records"
        )
        return df

    def add_data_quality_score(self, df: DataFrame) -> DataFrame:
        """📊 Add data quality score to each record"""
        logger.info("📊 Adding data quality scores")

        # Calculate quality score based on completeness and reasonableness
        quality_conditions = []

        # Core fields completeness (40% weight)
        core_fields = ["price", "area", "latitude", "longitude"]
        for field in core_fields:
            if field in df.columns:
                quality_conditions.append(
                    when(col(field).isNotNull() & (col(field) > 0), 0.1).otherwise(0.0)
                )

        # Additional fields completeness (30% weight)
        additional_fields = ["bedroom", "bathroom", "district", "ward"]
        for field in additional_fields:
            if field in df.columns:
                quality_conditions.append(
                    when(col(field).isNotNull(), 0.075).otherwise(0.0)
                )

        # Data reasonableness (30% weight)
        if "price" in df.columns and "area" in df.columns:
            quality_conditions.append(
                when(
                    (col("price") / col("area")).between(500000, 500000000), 0.15
                ).otherwise(
                    0.0
                )  # Reasonable price per sqm
            )
            quality_conditions.append(
                when(col("area").between(10, 10000), 0.15).otherwise(
                    0.0
                )  # Reasonable area
            )

        # Calculate total score
        if quality_conditions:
            total_score = quality_conditions[0]
            for condition in quality_conditions[1:]:
                total_score = total_score + condition

            df = df.withColumn("data_quality_score", total_score)
        else:
            df = df.withColumn("data_quality_score", lit(0.5))  # Default score

        logger.info("✅ Data quality scores added")
        return df

    def debug_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """🔍 Debug data quality and identify conversion issues"""
        logger.info("🔍 Debugging data quality issues")

        debug_info = {
            "total_records": df.count(),
            "column_info": {},
            "sample_data": {},
            "missing_stats": {},
            "data_type_issues": [],
        }

        # Analyze each column
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]

            # Basic stats
            total_count = df.count()
            null_count = df.filter(col(col_name).isNull()).count()
            non_null_count = total_count - null_count
            missing_pct = null_count / total_count if total_count > 0 else 0

            debug_info["column_info"][col_name] = {
                "type": col_type,
                "total": total_count,
                "non_null": non_null_count,
                "null": null_count,
                "missing_pct": missing_pct,
            }

            debug_info["missing_stats"][col_name] = missing_pct

            # Sample data (first 5 non-null values)
            if non_null_count > 0:
                sample_values = (
                    df.filter(col(col_name).isNotNull())
                    .select(col_name)
                    .limit(5)
                    .collect()
                )
                debug_info["sample_data"][col_name] = [
                    row[col_name] for row in sample_values
                ]

            # Special analysis for important columns
            if col_name in ["price", "price_per_m2", "area"]:
                if col_type == "string":
                    # Analyze string patterns for numeric columns that should be converted
                    logger.warning(
                        f"⚠️ {col_name} is still string type - analyzing patterns"
                    )

                    if non_null_count > 0:
                        # Sample string values to understand format
                        string_samples = (
                            df.filter(col(col_name).isNotNull() & (col(col_name) != ""))
                            .select(col_name)
                            .limit(10)
                            .collect()
                        )

                        patterns = [str(row[col_name]) for row in string_samples]
                        debug_info["data_type_issues"].append(
                            {
                                "column": col_name,
                                "issue": "string_numeric_column",
                                "sample_patterns": patterns,
                            }
                        )

                        logger.info(f"🔍 {col_name} string patterns: {patterns[:5]}")

        # Log summary
        logger.info("🔍 Data Quality Debug Summary:")
        logger.info(f"📊 Total records: {debug_info['total_records']:,}")

        for col_name, missing_pct in debug_info["missing_stats"].items():
            if missing_pct > 0.1:  # Log columns with >10% missing
                logger.info(f"⚠️ {col_name}: {missing_pct:.1%} missing")

        if debug_info["data_type_issues"]:
            logger.warning(
                f"🚨 Found {len(debug_info['data_type_issues'])} data type issues"
            )
            for issue in debug_info["data_type_issues"]:
                logger.warning(f"   - {issue['column']}: {issue['issue']}")

        return debug_info
