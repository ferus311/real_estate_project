"""
Pipeline xử lý Machine Learning riêng biệt
Gold → Data Preparation → Trained Models

Tách biệt hoàn toàn khỏi ETL pipeline để:
- Độc lập về tài nguyên và cấu hình
- Lịch chạy khác nhau (ETL hàng ngày, ML theo tuần/tháng)
- Dễ bảo trì và phát triển
- Tách biệt team ownership

Pipeline có 2 stages chính:
1. Data Preparation: Gold → Cleaned Data + Feature Engineering
2. Model Training: Prepared Data → Trained Models
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys
import os
import argparse

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from common.utils.date_utils import get_date_format
from common.utils.logging_utils import SparkJobLogger
from common.config.spark_config import create_optimized_ml_spark_session

# Import correct ML pipeline modules from ml/pipelines/
from ml.pipelines.data_preparation import MLDataPreprocessor
from ml.pipelines.model_training import MLTrainer


def run_data_preparation_stage(
    spark: SparkSession, input_date: str, property_type: str, logger
) -> bool:
    """Stage 1: Data Preparation (Gold → Cleaned Data + Feature Engineering)"""
    logger.logger.info("🔧 Stage 1: Data Preparation (Cleaning + Feature Engineering)")

    try:
        start_time = datetime.now()

        # Initialize data preparation pipeline
        data_prep = MLDataPreprocessor(spark=spark)

        # Run full data preparation pipeline
        prepared_df = data_prep.run_full_pipeline(input_date, property_type)

        duration = (datetime.now() - start_time).total_seconds()
        record_count = prepared_df.count()

        logger.logger.info(f"✅ Data Preparation completed: {duration:.1f}s")
        logger.logger.info(
            f"📊 Prepared {record_count:,} records with features for training"
        )
        return True

    except Exception as e:
        logger.log_error(f"❌ Data Preparation failed: {str(e)}")
        return False


def run_model_training_stage(
    spark: SparkSession, input_date: str, property_type: str, logger
) -> bool:
    """Stage 2: Model Training (Prepared Data → Trained Models)"""
    logger.logger.info("🤖 Stage 2: Model Training")

    try:
        start_time = datetime.now()

        # Apply performance optimizations to Spark session
        try:
            from data_processing.ml.utils.performance_utils import (
                apply_performance_optimizations,
            )

            optimized_spark = apply_performance_optimizations(spark)
            logger.logger.info("⚡ Performance optimizations applied")
        except ImportError:
            logger.logger.warning(
                "⚠️  Performance utils not found, using default Spark session"
            )
            optimized_spark = spark

        # Create ML trainer instance
        ml_trainer = MLTrainer(spark_session=optimized_spark)

        # Run ML training pipeline
        result = ml_trainer.run_training_pipeline(
            date=input_date, property_type=property_type
        )

        duration = (datetime.now() - start_time).total_seconds()

        # Check if training was successful
        if result and result.get("success", False):
            logger.logger.info(f"✅ Model Training completed: {duration:.1f}s")
            logger.logger.info(f"🏆 Best model: {result.get('best_model', 'N/A')}")
            logger.logger.info(f"📊 R²: {result.get('metrics', {}).get('r2', 0):.3f}")
            return True
        else:
            logger.log_error("❌ Model Training returned unsuccessful result")
            return False

    except Exception as e:
        logger.log_error(f"❌ Model Training failed: {str(e)}")
        return False


def validate_gold_data_exists(
    spark: SparkSession, input_date: str, property_type: str, logger
) -> bool:
    """Kiểm tra xem dữ liệu Gold có tồn tại không"""
    from common.utils.hdfs_utils import check_hdfs_path_exists

    def get_gold_path(property_type: str, date: str) -> str:
        """Tạo đường dẫn Gold data đơn giản"""
        date_formatted = date.replace("-", "")
        date_path = date.replace("-", "/")
        return f"/data/realestate/processed/gold/unified/{property_type}/{date_path}/unified_{property_type}_{date_formatted}.parquet"

    # Use unified path manager for consistent paths
    gold_path = get_gold_path(property_type, input_date)

    if check_hdfs_path_exists(spark, gold_path):
        logger.logger.info(f"✅ Gold data found: {gold_path}")
        return True
    else:
        logger.log_error(f"❌ Gold data not found: {gold_path}")
        logger.logger.error(
            "💡 Suggestion: Run ETL pipeline first to generate Gold data"
        )
        return False


def run_ml_pipeline(
    spark: SparkSession,
    input_date=None,
    property_types=None,
    feature_only=False,
    training_only=False,
    skip_features=False,
    skip_training=False,
    validate_gold=True,
):
    """
    Chạy pipeline ML hoàn chỉnh

    Args:
        spark: SparkSession
        input_date: Ngày xử lý (YYYY-MM-DD)
        property_types: Danh sách loại BDS ["house", "other"]
        feature_only: Chỉ chạy data preparation
        training_only: Chỉ chạy model training
        skip_features: Bỏ qua data preparation
        skip_training: Bỏ qua model training
        validate_gold: Kiểm tra dữ liệu Gold trước khi chạy
    """
    # Default values
    if input_date is None:
        input_date = datetime.now().strftime("%Y-%m-%d")

    if property_types is None:
        property_types = ["house", "other"]

    logger = SparkJobLogger("ml_processing_pipeline")
    logger.start_job(
        {
            "input_date": input_date,
            "property_types": property_types,
            "feature_only": feature_only,
            "training_only": training_only,
        }
    )

    # Validate stage arguments
    if feature_only and training_only:
        logger.log_error(
            "❌ Error: Cannot specify both --feature-only and --training-only"
        )
        return False

    # Determine which stages to run
    if feature_only:
        stages_to_run = ["data_preparation"]
    elif training_only:
        stages_to_run = ["model_training"]
    else:
        stages_to_run = []
        if not skip_features:
            stages_to_run.append("data_preparation")
        if not skip_training:
            stages_to_run.append("model_training")

    logger.logger.info(f"🚀 Starting ML Pipeline - {input_date}")
    logger.logger.info(f"📋 Property types: {property_types}")
    logger.logger.info(f"🔧 ML stages to run: {stages_to_run}")

    pipeline_start_time = datetime.now()
    overall_success = True

    try:
        for property_type in property_types:
            logger.logger.info(f"\n{'='*60}")
            logger.logger.info(
                f"🏠 Processing ML for Property Type: {property_type.upper()}"
            )
            logger.logger.info(f"{'='*60}")

            property_success = True

            # Stage 1: Data Preparation (Gold → Cleaned Data + Feature Engineering)
            if "data_preparation" in stages_to_run:
                preparation_success = run_data_preparation_stage(
                    spark, input_date, property_type, logger
                )
                if not preparation_success:
                    logger.log_error(f"❌ Data Preparation failed for {property_type}")
                    property_success = False
                    # Don't continue to training if preparation failed
                    continue

            # Stage 2: Model Training (Prepared Data → Trained Models)
            if "model_training" in stages_to_run:
                # Validate prepared data exists if not just created
                if "data_preparation" not in stages_to_run:
                    # TODO: Add validation for prepared data existence
                    pass

                training_success = run_model_training_stage(
                    spark, input_date, property_type, logger
                )
                if not training_success:
                    logger.log_error(f"❌ ML Training failed for {property_type}")
                    property_success = False

            if not property_success:
                overall_success = False

    except Exception as e:
        logger.log_error(f"❌ ML Pipeline failed: {str(e)}")
        overall_success = False

    pipeline_duration = datetime.now() - pipeline_start_time

    if overall_success:
        logger.logger.info(
            f"\n🎉 ML Pipeline completed successfully! Duration: {pipeline_duration}"
        )
    else:
        logger.logger.error(
            f"\n💥 ML Pipeline completed with errors! Duration: {pipeline_duration}"
        )

    return overall_success


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run ML Processing Pipeline (Gold → Data Preparation → Trained Models)"
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

    # ML stage control arguments - only one of these can be used
    stage_group = parser.add_mutually_exclusive_group()
    stage_group.add_argument(
        "--feature-only",
        action="store_true",
        help="Only run data preparation (Gold → Cleaned Data + Features)",
    )
    stage_group.add_argument(
        "--training-only",
        action="store_true",
        help="Only run model training (Prepared Data → Trained Models)",
    )

    # Skip stage arguments - can be combined
    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="Skip data preparation stage",
    )
    parser.add_argument(
        "--skip-training", action="store_true", help="Skip model training stage"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Create ML-optimized Spark session (không cần config riêng nữa)
    spark = create_optimized_ml_spark_session("Real Estate ML Pipeline")

    try:
        # Run the ML pipeline
        success = run_ml_pipeline(
            spark,
            input_date=args.date,
            property_types=args.property_types,
            feature_only=args.feature_only,
            training_only=args.training_only,
            skip_features=args.skip_features,
            skip_training=args.skip_training,
        )

        if success:
            print("✅ ML Pipeline completed successfully!")
        else:
            print("❌ ML Pipeline completed with errors!")
            exit(1)

    finally:
        spark.stop()
