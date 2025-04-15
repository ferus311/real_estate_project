from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Cấu hình DAG
with DAG(
    dag_id="crawl_data_to_hdfs",
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG crawl dữ liệu và ghi vào HDFS bằng Docker",
) as dag:

    # Task: Crawl từ trang 0 đến 500
    crawl_all_pages = DockerOperator(
        task_id="crawl_all_pages",
        image="realestate-crawler",  # Tên image Docker đã build
        container_name="airflow-crawler-task",
        auto_remove=True,
        command="python selenium_crawler/crawl_upto_hdfs.py --start_page 0 --end_page 500",
        docker_url="unix://var/run/docker.sock",
        network_mode="hdfs_network",  # Đảm bảo mạng Docker đúng
    )

    # Task: Crawl lại các trang bị lỗi
    retry_failed_pages = DockerOperator(
        task_id="retry_failed_pages",
        image="realestate-crawler",
        container_name="airflow-crawler-task-retry",
        auto_remove=True,
        command="python selenium_crawler/crawl_upto_hdfs.py --retry_failed",
        docker_url="unix://var/run/docker.sock",
        network_mode="hdfs_network",
    )

    # Định nghĩa thứ tự chạy
    crawl_all_pages >> retry_failed_pages
