#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Airflow DAG cho ML Training Pipeline
Chạy hàng ngày để retrain models với dữ liệu mới
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import logging

# Default arguments
default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "catchup": False,
}

# DAG definition
dag = DAG(
    "ml_training_pipeline",
    default_args=default_args,
    description="Daily ML model training pipeline for real estate price prediction",
    schedule_interval="0 6 * * *",  # Run daily at 6 AM
    max_active_runs=1,
    tags=["ml", "training", "real_estate", "daily"],
)

# Configuration
TRAINING_CONFIG = {
    "training": {"lookback_days": 30, "test_ratio": 0.2},
    "model": {"type": "random_forest"},
    "data": {"gold_path": "/data/real_estate/gold"},
    "model_registry_path": "/data/models",
}


def check_data_quality(**context):
    """
    Kiểm tra chất lượng dữ liệu trước khi training
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, when, isnan, isnull

    execution_date = context["ds"]
    logging.info(f"Checking data quality for {execution_date}")

    spark = (
        SparkSession.builder.appName("Data Quality Check")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        # Load data from Gold layer
        gold_path = "/data/real_estate/gold/unified_properties"
        df = spark.read.parquet(gold_path)

        # Filter recent data
        recent_df = df.filter(
            col("crawl_date")
            >= (
                datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=30)
            ).strftime("%Y-%m-%d")
        )

        total_records = recent_df.count()

        # Quality checks
        checks = {
            "total_records": total_records,
            "valid_price": recent_df.filter(
                (col("price_cleaned").isNotNull()) & (col("price_cleaned") > 0)
            ).count(),
            "valid_area": recent_df.filter(
                (col("area_cleaned").isNotNull()) & (col("area_cleaned") > 0)
            ).count(),
            "complete_records": recent_df.filter(
                (col("price_cleaned").isNotNull())
                & (col("price_cleaned") > 0)
                & (col("area_cleaned").isNotNull())
                & (col("area_cleaned") > 0)
                & (col("city").isNotNull())
            ).count(),
        }

        # Quality thresholds
        min_records = 1000
        min_completeness = 0.7

        quality_score = (
            checks["complete_records"] / total_records if total_records > 0 else 0
        )

        logging.info(f"Data quality report: {json.dumps(checks, indent=2)}")
        logging.info(f"Quality score: {quality_score:.2%}")

        # Validation
        if total_records < min_records:
            raise ValueError(f"Insufficient data: {total_records} < {min_records}")

        if quality_score < min_completeness:
            raise ValueError(
                f"Data quality too low: {quality_score:.2%} < {min_completeness:.2%}"
            )

        logging.info("✅ Data quality check passed")

        # Store quality metrics for monitoring
        context["task_instance"].xcom_push(key="data_quality", value=checks)

    finally:
        spark.stop()


def prepare_training_config(**context):
    """
    Chuẩn bị configuration cho training
    """
    execution_date = context["ds"]

    # Dynamic configuration based on data volume or other factors
    config = TRAINING_CONFIG.copy()

    # Get data quality info
    data_quality = context["task_instance"].xcom_pull(
        key="data_quality", task_ids="check_data_quality"
    )

    if data_quality:
        # Adjust lookback days based on data availability
        if data_quality["total_records"] > 10000:
            config["training"]["lookback_days"] = 45
        elif data_quality["total_records"] > 5000:
            config["training"]["lookback_days"] = 30
        else:
            config["training"]["lookback_days"] = 21

    # Save config for training task
    config_path = f"/tmp/ml_training_config_{execution_date}.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    logging.info(f"Training config prepared: {json.dumps(config, indent=2)}")

    return config_path


def monitor_training_results(**context):
    """
    Monitor training results và gửi alerts
    """
    execution_date = context["ds"]

    # This would typically connect to your monitoring system
    # For now, we'll just log the results

    logging.info(f"Monitoring training results for {execution_date}")

    # You could add logic here to:
    # - Check if model performance degraded
    # - Send alerts to Slack/email
    # - Update model monitoring dashboard
    # - Trigger rollback if needed

    return "monitoring_completed"


# Task Groups
with TaskGroup("data_validation", dag=dag) as data_validation_group:

    # Wait for ETL pipeline to complete
    wait_for_etl = FileSensor(
        task_id="wait_for_etl_completion",
        filepath="/data/real_estate/gold/unified_properties/_SUCCESS",
        fs_conn_id="fs_default",
        poke_interval=300,  # Check every 5 minutes
        timeout=3600,  # Timeout after 1 hour
        dag=dag,
    )

    # Data quality check
    data_quality_check = PythonOperator(
        task_id="check_data_quality", python_callable=check_data_quality, dag=dag
    )

    wait_for_etl >> data_quality_check


with TaskGroup("model_training", dag=dag) as model_training_group:

    # Prepare training configuration
    prep_config = PythonOperator(
        task_id="prepare_training_config",
        python_callable=prepare_training_config,
        dag=dag,
    )

    # Run ML training pipeline
    run_training = DockerOperator(
        task_id="run_ml_training",
        image="spark-processor:latest",
        command="python /app/ml/training_pipeline.py --date {{ ds }} --config /tmp/ml_training_config_{{ ds }}.json",
        network_mode="hdfs_network",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[{"source": "/tmp", "target": "/tmp", "type": "bind"}],
        environment={
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
            "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
        },
        docker_url="unix://var/run/docker.sock",
        dag=dag,
    )

    prep_config >> run_training


with TaskGroup("model_validation", dag=dag) as model_validation_group:

    # Model performance validation
    validate_model = BashOperator(
        task_id="validate_model_performance",
        bash_command="""
        python /opt/airflow/dags/scripts/validate_model.py \
            --date {{ ds }} \
            --model-name price_prediction
        """,
        dag=dag,
    )

    # Monitor results
    monitor_results = PythonOperator(
        task_id="monitor_training_results",
        python_callable=monitor_training_results,
        dag=dag,
    )

    validate_model >> monitor_results


# Cleanup task
cleanup_temp_files = BashOperator(
    task_id="cleanup_temp_files",
    bash_command="rm -f /tmp/ml_training_config_{{ ds }}.json",
    dag=dag,
)

# Define task dependencies
(
    data_validation_group
    >> model_training_group
    >> model_validation_group
    >> cleanup_temp_files
)
