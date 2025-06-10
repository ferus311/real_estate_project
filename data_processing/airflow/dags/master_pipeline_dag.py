from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "depends_on_past": False,
}

dag = DAG(
    "master_realestate_pipeline",
    default_args=default_args,
    description="DAG master điều phối toàn bộ pipeline bất động sản (ETL + ML)",
    schedule_interval="0 3 * * 1",  # Chạy hàng tuần vào thứ 2 lúc 3:00 AM
    start_date=days_ago(1),
    tags=["master", "realestate", "etl", "ml"],
)

# Step 1: Trigger ETL Processing
trigger_etl_dag = TriggerDagRunOperator(
    task_id="trigger_etl_processing",
    trigger_dag_id="etl_processing_pipeline",
    wait_for_completion=True,  # Đợi ETL hoàn thành trước khi chuyển sang ML
    poke_interval=60,  # Check mỗi 60 giây
    dag=dag,
)

# Step 2: Trigger ML Training (chỉ chạy sau khi ETL hoàn thành)
trigger_ml_dag = TriggerDagRunOperator(
    task_id="trigger_ml_training",
    trigger_dag_id="ml_training_pipeline",
    wait_for_completion=True,
    poke_interval=60,
    dag=dag,
)

# Step 3: Generate Combined Reports
generate_combined_reports = DockerOperator(
    task_id="generate_combined_reports",
    image="spark-processor:latest",
    command="python /app/jobs/reporting/generate_weekly_ml_report.py --property-types house other",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment={
        "SPARK_MASTER_URL": "spark://spark-master:7077",
        "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
        "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
        "PYTHONPATH": "/app",
    },
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

# Define task dependencies
trigger_etl_dag >> trigger_ml_dag >> generate_combined_reports
