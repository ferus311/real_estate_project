from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
from airflow import DAG
from airflow.models import TaskInstance
from datetime import timedelta
import re


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "depends_on_past": False,
}

dag = DAG(
    "etl_processing_pipeline",
    default_args=default_args,
    description="DAG xử lý dữ liệu ETL bất động sản (Raw → Bronze → Silver → Gold)",
    schedule_interval="0 2 * * *",  # Chạy hàng ngày lúc 2:00 AM
    start_date=days_ago(1),
    tags=["etl", "data_processing", "realestate"],
)

# Common Docker environment for ETL processing
etl_docker_env = {
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
    "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
    "PYTHONPATH": "/app",
}

# ETL Processing: Raw → Bronze → Silver → Gold
etl_processing_task = DockerOperator(
    task_id="run_etl_processing",
    image="spark-processor:latest",
    command="python /app/pipelines/daily_processing.py --skip-load --property-types house other",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment=etl_docker_env,
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

# Optional: Data Quality Check
data_quality_check_task = DockerOperator(
    task_id="data_quality_check",
    image="spark-processor:latest",
    command="python /app/jobs/validation/data_quality_check.py --property-types house other",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment=etl_docker_env,
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

# Optional: Generate Data Reports
generate_reports_task = DockerOperator(
    task_id="generate_data_reports",
    image="spark-processor:latest",
    command="python /app/jobs/reporting/generate_daily_report.py --property-types house other",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment=etl_docker_env,
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

# Define task dependencies
etl_processing_task >> data_quality_check_task >> generate_reports_task
