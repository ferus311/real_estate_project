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
    "ml_training_pipeline",
    default_args=default_args,
    description="DAG huấn luyện Machine Learning cho dự đoán giá bất động sản",
    schedule_interval=None,  # DAG này được trigger thủ công hoặc theo lịch tuần
    start_date=days_ago(1),
    tags=["machine_learning", "realestate", "training"],
)

# Common Docker environment for ML processing
ml_docker_env = {
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
    "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
    "PYTHONPATH": "/app",
}


# Step 1: Data Preparation (Gold → Cleaned Data + Feature Engineering)
data_preparation_task = DockerOperator(
    task_id="data_preparation",
    image="spark-processor:latest",
    command="python /app/pipelines/ml_processing.py --feature-only --date 2025-06-12",
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

# Step 2: Model Training (Prepared Data → Trained Models)
model_training_task = DockerOperator(
    task_id="model_training",
    image="spark-processor:latest",
    command="python /app/pipelines/ml_processing.py --training-only",
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


# Define task dependencies
(data_preparation_task >> model_training_task)
