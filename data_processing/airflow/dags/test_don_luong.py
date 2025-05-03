from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Cấu hình DAG
with DAG(
    dag_id="crawl_pages_single",
    schedule_interval=None,  # Chỉ chạy khi kích hoạt thủ công
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG crawl dữ liệu từ một khoảng trang và ghi vào HDFS",
) as dag:

    # Task: Crawl từ trang 0 đến 500
    crawl_pages = DockerOperator(
        task_id="crawl_pages_single",
        image="realestate-crawler",  # Tên image Docker đã build
        container_name="airflow-crawler-pages-single-task",
        auto_remove=True,
        command="python selenium_crawler/test_don_luong.py --start_page 1 --end_page 10",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_hdfs_network",  # Đảm bảo mạng Docker đúng
        mount_tmp_dir=False,
    )
