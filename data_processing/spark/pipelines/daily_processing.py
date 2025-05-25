"""
Pipeline xử lý dữ liệu hàng ngày đơn giản và hiệu quả
Raw → Bronze → Silver → Gold
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys
import os
import argparse

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from jobs.extraction.extract_batdongsan import extract_batdongsan_data
from jobs.extraction.extract_chotot import extract_chotot_data
from jobs.transformation.transform_batdongsan import transform_batdongsan_data
from jobs.transformation.transform_chotot import transform_chotot_data
from jobs.transformation.unify_dataset import unify_property_data
from common.utils.date_utils import get_date_format, get_hdfs_path
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


def check_data_availability(spark: SparkSession, path: str) -> tuple[bool, int]:
    """Check if data exists at path and return count"""
    try:
        print(f"🔍 Checking path: {path}")

        # First try to check if path exists using basic file system check
        try:
            # Try to list files in directory
            files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
            print(f"📁 Found {len(files)} files/directories in path")

            # Check if any .parquet files exist and get their sizes
            parquet_files = []
            total_size = 0

            for f in files:
                file_path = str(f.getPath())
                if file_path.endswith(".parquet"):
                    file_size = f.getLen()
                    total_size += file_size
                    parquet_files.append((file_path, file_size))
                    print(f"📄 Parquet file: {file_path} ({file_size} bytes)")
                elif f.isDirectory():
                    # Check subdirectories for parquet files
                    try:
                        sub_files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                            spark._jsc.hadoopConfiguration()
                        ).listStatus(f.getPath())

                        for sub_f in sub_files:
                            sub_path = str(sub_f.getPath())
                            if sub_path.endswith(".parquet"):
                                file_size = sub_f.getLen()
                                total_size += file_size
                                parquet_files.append((sub_path, file_size))
                                print(
                                    f"📄 Parquet file: {sub_path} ({file_size} bytes)"
                                )
                    except Exception:
                        pass  # Skip subdirectories that can't be read

            print(
                f"📄 Found {len(parquet_files)} parquet files, total size: {total_size} bytes"
            )

            # If no parquet files or all files are empty
            if not parquet_files or total_size == 0:
                print(f"⚠️ No valid parquet files found or all files are empty")
                return False, 0

        except Exception as fs_e:
            print(f"⚠️ File system check failed: {str(fs_e)}")
            return False, 0

        # Now try to read parquet data with schema inference handling
        try:
            # First attempt: normal read with schema inference
            df = spark.read.parquet(path)
            count = df.count()
            print(f"✅ Successfully read {count:,} records from {path}")
            return True, count

        except Exception as schema_error:
            if "UNABLE_TO_INFER_SCHEMA" in str(schema_error):
                print(f"⚠️ Schema inference failed, trying alternative approaches...")

                # Try to read with option to handle corrupted files
                try:
                    df = (
                        spark.read.option("mergeSchema", "true")
                        .option("ignoreCorruptFiles", "true")
                        .parquet(path)
                    )
                    count = df.count()
                    print(
                        f"✅ Successfully read {count:,} records with merge schema option"
                    )
                    return True, count

                except Exception as merge_error:
                    print(f"⚠️ Merge schema approach failed: {str(merge_error)}")

                    # Try to read individual parquet files
                    try:
                        valid_files = []
                        for file_path, file_size in parquet_files:
                            if file_size > 0:  # Only try non-empty files
                                try:
                                    test_df = spark.read.parquet(file_path)
                                    test_count = test_df.count()
                                    if test_count > 0:
                                        valid_files.append(file_path)
                                        print(
                                            f"✅ Valid file: {file_path} ({test_count:,} records)"
                                        )
                                except Exception:
                                    print(f"⚠️ Corrupted file: {file_path}")

                        if valid_files:
                            # Read only valid files
                            combined_df = spark.read.parquet(*valid_files)
                            count = combined_df.count()
                            print(
                                f"✅ Successfully read {count:,} records from {len(valid_files)} valid files"
                            )
                            return True, count
                        else:
                            print(f"❌ No valid parquet files found")
                            return False, 0

                    except Exception as individual_error:
                        print(
                            f"❌ Individual file reading failed: {str(individual_error)}"
                        )
                        return False, 0
            else:
                # Re-raise non-schema related errors
                raise schema_error

    except Exception as e:
        print(f"❌ Error reading data from {path}: {str(e)}")
        print(f"❌ Exception type: {type(e).__name__}")
        return False, 0


def run_extraction_stage(
    spark: SparkSession, input_date: str, property_type: str, logger
) -> dict:
    """Stage 1: Extraction (Raw → Bronze)"""
    logger.logger.info("🔄 Stage 1: Extraction")

    results = {"batdongsan": False, "chotot": False}

    # Extract Batdongsan
    try:
        start_time = datetime.now()
        batdongsan_df = extract_batdongsan_data(spark, input_date, property_type)
        count = batdongsan_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        logger.logger.info(f"✅ Batdongsan: {count:,} records ({duration:.1f}s)")
        results["batdongsan"] = True

        # Unpersist to free memory
        batdongsan_df.unpersist()

    except Exception as e:
        logger.log_error(f"❌ Batdongsan extraction failed: {str(e)}")

    # Extract Chotot
    try:
        start_time = datetime.now()
        chotot_df = extract_chotot_data(spark, input_date, property_type)
        count = chotot_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        logger.logger.info(f"✅ Chotot: {count:,} records ({duration:.1f}s)")
        results["chotot"] = True

        # Unpersist to free memory
        chotot_df.unpersist()

    except Exception as e:
        logger.log_error(f"❌ Chotot extraction failed: {str(e)}")

    return results


def run_transformation_stage(
    spark: SparkSession, input_date: str, property_type: str, logger
) -> dict:
    """Stage 2: Transformation (Bronze → Silver)"""
    logger.logger.info("🔄 Stage 2: Transformation")

    results = {"batdongsan": False, "chotot": False}

    # Transform Batdongsan
    try:
        start_time = datetime.now()
        batdongsan_silver_df = transform_batdongsan_data(
            spark, input_date, property_type
        )
        count = batdongsan_silver_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        logger.logger.info(f"✅ Batdongsan Silver: {count:,} records ({duration:.1f}s)")
        results["batdongsan"] = True

        # Unpersist to free memory
        batdongsan_silver_df.unpersist()

    except Exception as e:
        logger.log_error(f"❌ Batdongsan transformation failed: {str(e)}")

    # Transform Chotot
    try:
        start_time = datetime.now()
        chotot_silver_df = transform_chotot_data(spark, input_date, property_type)
        count = chotot_silver_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        logger.logger.info(f"✅ Chotot Silver: {count:,} records ({duration:.1f}s)")
        results["chotot"] = True

        # Unpersist to free memory
        chotot_silver_df.unpersist()

    except Exception as e:
        logger.log_error(f"❌ Chotot transformation failed: {str(e)}")

    return results


def run_unification_stage(
    spark: SparkSession, input_date: str, property_type: str, logger
) -> bool:
    """Stage 3: Unification (Silver → Gold)"""
    logger.logger.info("🔄 Stage 3: Unification")

    try:
        start_time = datetime.now()
        unified_df = unify_property_data(spark, input_date, property_type)
        count = unified_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        logger.logger.info(f"✅ Unified Gold: {count:,} records ({duration:.1f}s)")

        # Unpersist to free memory
        unified_df.unpersist()

        return True

    except Exception as e:
        logger.log_error(f"❌ Unification failed: {str(e)}")
        return False


def run_daily_pipeline(
    spark: SparkSession,
    input_date=None,
    property_types=None,
    extract_only=False,
    transform_only=False,
    unify_only=False,
    skip_extraction=False,
    skip_transformation=False,
    skip_unification=False,
):
    """
    Chạy pipeline xử lý dữ liệu hàng ngày với các tùy chọn stage

    Args:
        spark: SparkSession
        input_date: Ngày xử lý (YYYY-MM-DD)
        property_types: Danh sách loại BDS ["house", "other"]
        extract_only: Chỉ chạy extraction stage
        transform_only: Chỉ chạy transformation stage
        unify_only: Chỉ chạy unification stage
        skip_extraction: Bỏ qua extraction stage
        skip_transformation: Bỏ qua transformation stage
        skip_unification: Bỏ qua unification stage
    """
    # Default values
    if input_date is None:
        input_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if property_types is None:
        property_types = ["house", "other"]

    logger = SparkJobLogger("daily_processing_pipeline")
    logger.start_job({"input_date": input_date, "property_types": property_types})

    # Validate stage arguments
    stage_count = sum([extract_only, transform_only, unify_only])
    if stage_count > 1:
        logger.log_error(
            "❌ Error: Only one of --extract-only, --transform-only, --unify-only can be specified"
        )
        return False

    # Determine which stages to run
    if extract_only:
        stages_to_run = ["extraction"]
    elif transform_only:
        stages_to_run = ["transformation"]
    elif unify_only:
        stages_to_run = ["unification"]
    else:
        stages_to_run = []
        if not skip_extraction:
            stages_to_run.append("extraction")
        if not skip_transformation:
            stages_to_run.append("transformation")
        if not skip_unification:
            stages_to_run.append("unification")

    logger.logger.info(f"🚀 Starting Daily Pipeline - {input_date}")
    logger.logger.info(f"📋 Property types: {property_types}")
    logger.logger.info(f"🔧 Stages to run: {stages_to_run}")

    pipeline_start_time = datetime.now()
    overall_success = True

    try:
        for property_type in property_types:
            logger.logger.info(f"\n{'='*60}")
            logger.logger.info(f"🏠 Processing Property Type: {property_type.upper()}")
            logger.logger.info(f"{'='*60}")

            property_success = True

            # Stage 1: Extraction (Raw → Bronze)
            if "extraction" in stages_to_run:
                extraction_results = run_extraction_stage(
                    spark, input_date, property_type, logger
                )
                if not any(extraction_results.values()):
                    logger.log_error(f"❌ No data extracted for {property_type}")
                    property_success = False

            # Stage 2: Transformation (Bronze → Silver)
            if "transformation" in stages_to_run:
                # Check if bronze data exists before transformation
                if not skip_extraction and "extraction" not in stages_to_run:
                    # Validate bronze data availability using proper HDFS paths
                    bronze_paths = [
                        get_hdfs_path(
                            "/data/realestate/processed/bronze",
                            "batdongsan",
                            property_type,
                            input_date,
                        ),
                        get_hdfs_path(
                            "/data/realestate/processed/bronze",
                            "chotot",
                            property_type,
                            input_date,
                        ),
                    ]

                    bronze_available = True
                    # for path in bronze_paths:
                    #     exists, count = check_data_availability(spark, path)
                    #     if exists and count > 0:
                    #         bronze_available = True
                    #         logger.logger.info(
                    #             f"✅ Bronze data found: {path} ({count:,} records)"
                    #         )
                    #     else:
                    #         logger.logger.warning(f"⚠️ No bronze data: {path}")

                    # if not bronze_available:
                    #     logger.log_error(
                    #         f"❌ No bronze data available for transformation for {property_type}"
                    #     )
                    #     property_success = False
                    #     continue

                transformation_results = run_transformation_stage(
                    spark, input_date, property_type, logger
                )
                if not any(transformation_results.values()):
                    logger.log_error(f"❌ No data transformed for {property_type}")
                    property_success = False

            # Stage 3: Unification (Silver → Gold)
            if "unification" in stages_to_run:
                # Check if silver data exists before unification
                if not skip_transformation and "transformation" not in stages_to_run:
                    # Validate silver data availability using proper HDFS paths
                    silver_paths = [
                        get_hdfs_path(
                            "/data/realestate/processed/silver",
                            "batdongsan",
                            property_type,
                            input_date,
                        ),
                        get_hdfs_path(
                            "/data/realestate/processed/silver",
                            "chotot",
                            property_type,
                            input_date,
                        ),
                    ]

                    # silver_available = False
                    # for path in silver_paths:
                    #     exists, count = check_data_availability(spark, path)
                    #     if exists and count > 0:
                    #         silver_available = True
                    #         logger.logger.info(
                    #             f"✅ Silver data found: {path} ({count:,} records)"
                    #         )
                    #     else:
                    #         logger.logger.warning(f"⚠️ No silver data: {path}")

                    # if not silver_available:
                    #     logger.log_error(
                    #         f"❌ No silver data available for unification for {property_type}"
                    #     )
                    #     property_success = False
                    #     continue

                unification_success = run_unification_stage(
                    spark, input_date, property_type, logger
                )
                if not unification_success:
                    logger.log_error(f"❌ Unification failed for {property_type}")
                    property_success = False

            if not property_success:
                overall_success = False

    except Exception as e:
        logger.log_error(f"❌ Pipeline failed: {str(e)}")
        overall_success = False

    pipeline_duration = datetime.now() - pipeline_start_time

    if overall_success:
        logger.logger.info(
            f"\n🎉 Pipeline completed successfully! Duration: {pipeline_duration}"
        )
    else:
        logger.logger.error(
            f"\n💥 Pipeline completed with errors! Duration: {pipeline_duration}"
        )

    return overall_success


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run Daily Processing Pipeline with Stage Control"
    )

    # Basic arguments
    parser.add_argument("--date", type=str, help="Processing date (YYYY-MM-DD)")
    parser.add_argument(
        "--property-types",
        type=str,
        nargs="+",
        default=["house"],
        choices=["house", "other"],
        help="Property types to process",
    )

    # Stage control arguments - only one of these can be used
    stage_group = parser.add_mutually_exclusive_group()
    stage_group.add_argument(
        "--extract-only",
        action="store_true",
        help="Only run extraction stage (Raw → Bronze)",
    )
    stage_group.add_argument(
        "--transform-only",
        action="store_true",
        help="Only run transformation stage (Bronze → Silver)",
    )
    stage_group.add_argument(
        "--unify-only",
        action="store_true",
        help="Only run unification stage (Silver → Gold)",
    )

    # Skip stage arguments - can be combined
    parser.add_argument(
        "--skip-extraction", action="store_true", help="Skip extraction stage"
    )
    parser.add_argument(
        "--skip-transformation", action="store_true", help="Skip transformation stage"
    )
    parser.add_argument(
        "--skip-unification", action="store_true", help="Skip unification stage"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Create Spark session với cấu hình tối ưu
    config = {
        "spark.sql.shuffle.partitions": "50",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
    }

    spark = create_spark_session("Real Estate Daily Pipeline", config=config)

    try:
        success = run_daily_pipeline(
            spark=spark,
            input_date=args.date,
            property_types=args.property_types,
            extract_only=args.extract_only,
            transform_only=args.transform_only,
            unify_only=args.unify_only,
            skip_extraction=args.skip_extraction,
            skip_transformation=args.skip_transformation,
            skip_unification=args.skip_unification,
        )

        if success:
            print("✅ Pipeline completed successfully!")
        else:
            print("❌ Pipeline completed with errors!")
            exit(1)

    finally:
        spark.stop()
