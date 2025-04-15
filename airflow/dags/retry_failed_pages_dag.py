from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Cấu hình DAG
with DAG(
    dag_id="retry_failed_pages",
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG crawl lại các trang bị lỗi từ checkpoint và ghi vào HDFS",
) as dag:

    # Task: Crawl lại các trang bị lỗi
    retry_failed_pages = DockerOperator(
        task_id="retry_failed_pages_task",
        image="realestate-crawler",  # Tên image Docker đã build
        container_name="airflow-crawler-retry-task",
        auto_remove=True,
        command="python selenium_crawler/crawl_upto_hdfs.py --retry_failed",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_hdfs_network",  # Đảm bảo mạng Docker đúng
    )
