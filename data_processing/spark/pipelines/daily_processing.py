"""
Pipeline xử lý dữ liệu hàng ngày theo kiến trúc Medallion (Raw → Bronze → Silver → Gold)
"""

from pyspark.sql import SparkSession
from datetime import datetime
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
from common.utils.date_utils import get_date_format
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_spark_session


def run_extraction_stage(
    spark: SparkSession, input_date: str, property_types: list
) -> dict:
    """Chạy giai đoạn Extract (Raw → Bronze)"""
    logger = SparkJobLogger("extraction_stage")
    logger.start_job({"input_date": input_date, "property_types": property_types})

    results = {"batdongsan": {}, "chotot": {}}

    try:
        for property_type in property_types:
            logger.logger.info(f"🔄 Extracting {property_type} data...")

            # Extract Batdongsan
            try:
                start_time = datetime.now()
                batdongsan_df = extract_batdongsan_data(
                    spark, input_date, property_type
                )
                batdongsan_count = batdongsan_df.count()
                duration = (datetime.now() - start_time).total_seconds()

                results["batdongsan"][property_type] = {
                    "success": True,
                    "count": batdongsan_count,
                    "duration": duration,
                    "dataframe": batdongsan_df,
                }
                logger.logger.info(
                    f"✅ Batdongsan {property_type}: {batdongsan_count:,} records ({duration:.1f}s)"
                )

            except Exception as e:
                logger.log_error(
                    f"❌ Batdongsan {property_type} extraction failed: {str(e)}"
                )
                results["batdongsan"][property_type] = {
                    "success": False,
                    "error": str(e),
                }

            # Extract Chotot
            try:
                start_time = datetime.now()
                chotot_df = extract_chotot_data(spark, input_date, property_type)
                chotot_count = chotot_df.count()
                duration = (datetime.now() - start_time).total_seconds()

                results["chotot"][property_type] = {
                    "success": True,
                    "count": chotot_count,
                    "duration": duration,
                    "dataframe": chotot_df,
                }
                logger.logger.info(
                    f"✅ Chotot {property_type}: {chotot_count:,} records ({duration:.1f}s)"
                )

            except Exception as e:
                logger.log_error(
                    f"❌ Chotot {property_type} extraction failed: {str(e)}"
                )
                results["chotot"][property_type] = {"success": False, "error": str(e)}

        logger.end_job()
        return results

    except Exception as e:
        logger.log_error(f"💥 Extraction stage failed: {str(e)}")
        raise


def run_transformation_stage(
    spark: SparkSession, input_date: str, property_types: list, extraction_results: dict
) -> dict:
    """Chạy giai đoạn Transform (Bronze → Silver)"""
    logger = SparkJobLogger("transformation_stage")
    logger.start_job({"input_date": input_date, "property_types": property_types})

    results = {"batdongsan": {}, "chotot": {}}

    try:
        for property_type in property_types:
            logger.logger.info(f"🔄 Transforming {property_type} data...")

            # Transform Batdongsan
            if extraction_results["batdongsan"][property_type]["success"]:
                try:
                    start_time = datetime.now()
                    batdongsan_df = transform_batdongsan_data(
                        spark, input_date, property_type
                    )

                    # Cache nếu dataset lớn
                    if batdongsan_df.count() > 50000:
                        batdongsan_df.cache()

                    batdongsan_count = batdongsan_df.count()
                    duration = (datetime.now() - start_time).total_seconds()

                    results["batdongsan"][property_type] = {
                        "success": True,
                        "count": batdongsan_count,
                        "duration": duration,
                        "dataframe": batdongsan_df,
                    }
                    logger.logger.info(
                        f"✅ Batdongsan {property_type} transformed: {batdongsan_count:,} records ({duration:.1f}s)"
                    )

                except Exception as e:
                    logger.log_error(
                        f"❌ Batdongsan {property_type} transformation failed: {str(e)}"
                    )
                    results["batdongsan"][property_type] = {
                        "success": False,
                        "error": str(e),
                    }
            else:
                logger.logger.warning(
                    f"⏭️  Skipping Batdongsan {property_type} (extraction failed)"
                )
                results["batdongsan"][property_type] = {
                    "success": False,
                    "error": "Extraction failed",
                }

            # Transform Chotot
            if extraction_results["chotot"][property_type]["success"]:
                try:
                    start_time = datetime.now()
                    chotot_df = transform_chotot_data(spark, input_date, property_type)

                    # Cache nếu dataset lớn
                    if chotot_df.count() > 50000:
                        chotot_df.cache()

                    chotot_count = chotot_df.count()
                    duration = (datetime.now() - start_time).total_seconds()

                    results["chotot"][property_type] = {
                        "success": True,
                        "count": chotot_count,
                        "duration": duration,
                        "dataframe": chotot_df,
                    }
                    logger.logger.info(
                        f"✅ Chotot {property_type} transformed: {chotot_count:,} records ({duration:.1f}s)"
                    )

                except Exception as e:
                    logger.log_error(
                        f"❌ Chotot {property_type} transformation failed: {str(e)}"
                    )
                    results["chotot"][property_type] = {
                        "success": False,
                        "error": str(e),
                    }
            else:
                logger.logger.warning(
                    f"⏭️  Skipping Chotot {property_type} (extraction failed)"
                )
                results["chotot"][property_type] = {
                    "success": False,
                    "error": "Extraction failed",
                }

        logger.end_job()
        return results

    except Exception as e:
        logger.log_error(f"💥 Transformation stage failed: {str(e)}")
        raise


def run_unification_stage(
    spark: SparkSession,
    input_date: str,
    property_types: list,
    transformation_results: dict,
) -> dict:
    """Chạy giai đoạn Unify (Silver → Gold)"""
    logger = SparkJobLogger("unification_stage")
    logger.start_job({"input_date": input_date, "property_types": property_types})

    results = {}

    try:
        for property_type in property_types:
            logger.logger.info(f"🔄 Unifying {property_type} data...")

            # Check if we have data to unify
            has_batdongsan = transformation_results["batdongsan"][property_type][
                "success"
            ]
            has_chotot = transformation_results["chotot"][property_type]["success"]

            if has_batdongsan or has_chotot:
                try:
                    start_time = datetime.now()
                    unified_df = unify_property_data(spark, input_date, property_type)
                    unified_df.persist()  # Persist final result

                    unified_count = unified_df.count()
                    duration = (datetime.now() - start_time).total_seconds()

                    results[property_type] = {
                        "success": True,
                        "count": unified_count,
                        "duration": duration,
                        "sources": {"batdongsan": has_batdongsan, "chotot": has_chotot},
                    }
                    logger.logger.info(
                        f"✅ Unified {property_type}: {unified_count:,} records ({duration:.1f}s)"
                    )

                except Exception as e:
                    logger.log_error(
                        f"❌ Unification failed for {property_type}: {str(e)}"
                    )
                    results[property_type] = {"success": False, "error": str(e)}
            else:
                logger.logger.warning(f"⏭️  No data to unify for {property_type}")
                results[property_type] = {
                    "success": False,
                    "error": "No transformed data",
                }

        logger.end_job()
        return results

    except Exception as e:
        logger.log_error(f"💥 Unification stage failed: {str(e)}")
        raise


def generate_summary(
    input_date: str,
    property_types: list,
    extraction_results: dict,
    transformation_results: dict,
    unification_results: dict,
):
    """Tạo báo cáo tổng kết ngắn gọn"""
    logger = SparkJobLogger("pipeline_summary")

    logger.logger.info("=" * 50)
    logger.logger.info(f"📊 SUMMARY FOR {input_date}")
    logger.logger.info("=" * 50)

    total_records = 0
    for property_type in property_types:
        logger.logger.info(f"\n🏠 {property_type.upper()}:")

        # Extraction
        batdongsan_count = extraction_results["batdongsan"][property_type].get(
            "count", 0
        )
        chotot_count = extraction_results["chotot"][property_type].get("count", 0)
        extract_total = batdongsan_count + chotot_count

        # Unification
        unified_count = unification_results[property_type].get("count", 0)

        logger.logger.info(
            f"  📥 Extracted: {extract_total:,} records (BDS: {batdongsan_count:,}, CHT: {chotot_count:,})"
        )
        logger.logger.info(f"  🎯 Unified: {unified_count:,} records")

        total_records += unified_count

    logger.logger.info(f"\n🚀 TOTAL PROCESSED: {total_records:,} records")
    logger.logger.info("=" * 50)


def run_daily_pipeline(
    spark: SparkSession, input_date=None, property_types=None, skip_stages=None
):
    """Chạy pipeline xử lý dữ liệu hàng ngày"""
    logger = SparkJobLogger("daily_processing_pipeline")

    # Set defaults
    if input_date is None:
        input_date = get_date_format()
    if property_types is None:
        property_types = ["house", "other"]
    elif isinstance(property_types, str):
        property_types = [property_types]
    if skip_stages is None:
        skip_stages = []

    logger.start_job(
        {
            "input_date": input_date,
            "property_types": property_types,
            "skip_stages": skip_stages,
        }
    )

    logger.logger.info(f"🚀 Starting pipeline for {input_date}")
    logger.logger.info(f"📋 Property types: {property_types}")
    if skip_stages:
        logger.logger.info(f"⏭️  Skip stages: {skip_stages}")

    total_start = datetime.now()

    try:
        # Stage 1: Extraction
        if "extract" not in skip_stages:
            logger.logger.info("\n🔄 Stage 1: EXTRACTION")
            extraction_results = run_extraction_stage(spark, input_date, property_types)
        else:
            logger.logger.info("\n⏭️  Stage 1: EXTRACTION - SKIPPED")
            extraction_results = {"batdongsan": {}, "chotot": {}}

        # Stage 2: Transformation
        if "transform" not in skip_stages:
            logger.logger.info("\n🔄 Stage 2: TRANSFORMATION")
            transformation_results = run_transformation_stage(
                spark, input_date, property_types, extraction_results
            )
        else:
            logger.logger.info("\n⏭️  Stage 2: TRANSFORMATION - SKIPPED")
            transformation_results = {"batdongsan": {}, "chotot": {}}

        # Stage 3: Unification
        if "unify" not in skip_stages:
            logger.logger.info("\n🔄 Stage 3: UNIFICATION")
            unification_results = run_unification_stage(
                spark, input_date, property_types, transformation_results
            )
        else:
            logger.logger.info("\n⏭️  Stage 3: UNIFICATION - SKIPPED")
            unification_results = {}

        # Summary
        if not skip_stages:
            generate_summary(
                input_date,
                property_types,
                extraction_results,
                transformation_results,
                unification_results,
            )

        total_duration = (datetime.now() - total_start).total_seconds()
        logger.logger.info(f"\n✅ Pipeline completed in {total_duration:.1f}s")
        logger.end_job()

        return {
            "extraction": extraction_results,
            "transformation": transformation_results,
            "unification": unification_results,
            "duration": total_duration,
        }

    except Exception as e:
        duration = (datetime.now() - total_start).total_seconds()
        logger.log_error(f"❌ Pipeline failed after {duration:.1f}s: {str(e)}")
        raise


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run Daily Processing Pipeline")
    parser.add_argument("--date", type=str, help="Processing date (YYYY-MM-DD)")
    parser.add_argument(
        "--property-types",
        type=str,
        nargs="+",
        default=["house", "other"],
        choices=["house", "other"],
        help="Property types to process",
    )
    parser.add_argument(
        "--skip-stages",
        type=str,
        nargs="*",
        default=[],
        choices=["extract", "transform", "unify"],
        help="Stages to skip",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Create Spark session đơn giản
    spark = create_spark_session("Real Estate Daily Pipeline")

    try:
        run_daily_pipeline(spark, args.date, args.property_types, args.skip_stages)
    finally:
        spark.stop()
