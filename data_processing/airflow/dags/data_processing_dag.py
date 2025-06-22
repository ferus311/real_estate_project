from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
from airflow import DAG
from airflow.models import TaskInstance
from datetime import timedelta, datetime
import re


# Cấu hình chung - có thể thay đổi dễ dàng
PROCESSING_DATE = "2025-05-24"  # Sử dụng execution date của Airflow

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "depends_on_past": False,
}

# Environment chung cho tất cả tasks
COMMON_ENVIRONMENT = {
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
    "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
    # PostgreSQL connection settings
    "POSTGRES_HOST": "db",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "realestate",
    "POSTGRES_USER": "postgres",
    "POSTGRES_PASSWORD": "realestate123",
}

# DAG chính với control từng stage
dag = DAG(
    "realestate_data_processing",
    default_args=default_args,
    description="DAG kiểm soát từng stage riêng biệt cho xử lý dữ liệu BĐS",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["data_processing", "realestate", "stage_control"],
)


# # Stage 1: Extraction (Raw → Bronze)
# extraction_stage = DockerOperator(
#     task_id="stage_1_extraction",
#     image="spark-processor:latest",
#     command=f"python /app/pipelines/daily_processing.py --date {PROCESSING_DATE} --extract-only",
#     network_mode="hdfs_network",
#     api_version="auto",
#     auto_remove=True,
#     mount_tmp_dir=False,
#     environment=COMMON_ENVIRONMENT,
#     docker_url="unix://var/run/docker.sock",
#     dag=dag,
# )

# # Stage 2: Transformation (Bronze → Silver)
# transformation_stage = DockerOperator(
#     task_id="stage_2_transformation",
#     image="spark-processor:latest",
#     command=f"python /app/pipelines/daily_processing.py --date {PROCESSING_DATE} --transform-only",
#     network_mode="hdfs_network",
#     api_version="auto",
#     auto_remove=True,
#     mount_tmp_dir=False,
#     environment=COMMON_ENVIRONMENT,
#     docker_url="unix://var/run/docker.sock",
#     dag=dag,
# )

# # Stage 3: Unification (Silver → Gold)
# unification_stage = DockerOperator(
#     task_id="stage_3_unification",
#     image="spark-processor:latest",
#     command=f"python /app/pipelines/daily_processing.py --date {PROCESSING_DATE} --unify-only",
#     network_mode="hdfs_network",
#     api_version="auto",
#     auto_remove=True,
#     mount_tmp_dir=False,
#     environment=COMMON_ENVIRONMENT,
#     docker_url="unix://var/run/docker.sock",
#     dag=dag,
# )

# # Stage 4: Load (Gold → PostgreSQL)
# load_stage = DockerOperator(
#     task_id="stage_4_load",
#     image="spark-processor:latest",
#     command=f"python /app/pipelines/daily_processing.py --date {PROCESSING_DATE} --load-only ",
#     network_mode="hdfs_network",
#     api_version="auto",
#     auto_remove=True,
#     mount_tmp_dir=False,
#     environment=COMMON_ENVIRONMENT,
#     docker_url="unix://var/run/docker.sock",
#     dag=dag,
# )

# Task chạy full pipeline (để backup)
full_pipeline = DockerOperator(
    task_id="run_full_pipeline",
    image="spark-processor:latest",
    command=f"python /app/pipelines/daily_processing.py --date {PROCESSING_DATE} ",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment=COMMON_ENVIRONMENT,
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

# Thiết lập dependencies cho pipeline tuần tự
# extraction_stage >> transformation_stage >> unification_stage >> load_stage
full_pipeline
