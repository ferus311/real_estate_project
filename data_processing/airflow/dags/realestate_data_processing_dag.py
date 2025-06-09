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
    "realestate_data_processing",
    default_args=default_args,
    description="DAG xử lý dữ liệu bất động sản qua Spark",
    schedule_interval=None,  # DAG này được trigger bởi pipeline_dag
    start_date=days_ago(1),
    tags=["data_processing", "realestate"],
)

property_types = "{{ dag_run.conf.get('property_types', 'house') }}"
date_param = "{{ ds }}"  # Airflow execution date

# Chạy xử lý đầy đủ (Raw → Bronze → Silver → Gold)
run_processing = DockerOperator(
    task_id="run_full_processing",
    image="spark-processor:latest",
    command=f"python /app/pipelines/daily_processing.py --property-types {property_types}",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment={
        "SPARK_MASTER_URL": "spark://spark-master:7077",
        "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
        "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
    },
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)
# Định nghĩa luồng công việc: ETL → Feature Engineering → ML Training
run_processing
