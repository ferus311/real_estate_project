from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="crawl_data_to_hdfs_test",
    schedule_interval=None,  # Không có lịch trình, chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG crawl dữ liệu và ghi vào HDFS bằng Docker",
) as dag:

    run_crawler = DockerOperator(
        task_id="run_real_estate_crawler",
        image="realestate-crawler",  # Tên image đã build
        container_name="airflow-crawler-task",
        auto_remove=True,
        command="python run.py",  # Lệnh chạy script
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_hdfs_network",
    )
