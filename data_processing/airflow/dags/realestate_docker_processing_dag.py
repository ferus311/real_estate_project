"""
DAG cho việc xử lý dữ liệu bất động sản sử dụng Docker container
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Các tham số có thể cấu hình
SPARK_DOCKER_IMAGE = "spark-processor:latest"
PROPERTY_TYPES = [
    "house",
    "other",
]  # Có thể mở rộng với 'apartment', 'land', 'commercial'
WAIT_FOR_CRAWLER = True  # Đặt thành False nếu không muốn đợi crawler hoàn thành

with DAG(
    "realestate_data_processing_docker",
    default_args=default_args,
    description="Real Estate Data Processing Pipeline using Docker",
    schedule_interval="0 3 * * *",  # Chạy lúc 3 giờ sáng hàng ngày
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["realestate", "data_processing", "docker"],
) as dag:

    start = DummyOperator(task_id="start_pipeline")
    end = DummyOperator(task_id="end_pipeline")

    # Sensor chờ crawler hoàn thành (nếu cần)
    if WAIT_FOR_CRAWLER:
        wait_for_crawler = ExternalTaskSensor(
            task_id="wait_for_crawler",
            external_dag_id="realestate_crawler_dag",  # ID của DAG crawler
            external_task_id="end_crawler",  # Task ID cuối cùng trong DAG crawler
            timeout=3600,  # Thời gian tối đa chờ đợi (1 giờ)
            mode="reschedule",  # Giải phóng worker slot khi đang chờ
        )
        start >> wait_for_crawler

    # Xử lý cho từng loại bất động sản
    for property_type in PROPERTY_TYPES:
        # 1. Extraction - Trích xuất dữ liệu từ HDFS
        extract_task = DockerOperator(
            task_id=f"extract_{property_type}",
            image=SPARK_DOCKER_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=[
                "jobs/extraction/extract_batdongsan.py",
                "--date",
                "{{ ds }}",
                "--property-type",
                property_type,
            ],
            network_mode="spark_network",
            docker_url="unix://var/run/docker.sock",
            mount_tmp_dir=False,
            environment={
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_DRIVER_MEMORY": "2g",
                "SPARK_EXECUTOR_MEMORY": "2g",
            },
            mounts=[
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                )
            ],
        )

        # 2. Transformation - Chuyển đổi và làm sạch dữ liệu
        transform_task = DockerOperator(
            task_id=f"transform_{property_type}",
            image=SPARK_DOCKER_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=[
                "jobs/transformation/transform_batdongsan.py",
                "--date",
                "{{ ds }}",
                "--property-type",
                property_type,
            ],
            network_mode="spark_network",
            docker_url="unix://var/run/docker.sock",
            mount_tmp_dir=False,
            environment={
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_DRIVER_MEMORY": "2g",
                "SPARK_EXECUTOR_MEMORY": "2g",
            },
            mounts=[
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                )
            ],
        )

        # 3. Unify - Hợp nhất dữ liệu
        unify_task = DockerOperator(
            task_id=f"unify_{property_type}",
            image=SPARK_DOCKER_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=[
                "jobs/transformation/unify_dataset.py",
                "--date",
                "{{ ds }}",
                "--property-type",
                property_type,
            ],
            network_mode="spark_network",
            docker_url="unix://var/run/docker.sock",
            mount_tmp_dir=False,
            environment={
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_DRIVER_MEMORY": "2g",
                "SPARK_EXECUTOR_MEMORY": "2g",
            },
            mounts=[
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                )
            ],
        )

        # 4. Data Quality - Kiểm tra chất lượng dữ liệu
        validate_task = DockerOperator(
            task_id=f"validate_{property_type}",
            image=SPARK_DOCKER_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=[
                "jobs/data_quality/data_validation.py",
                "--date",
                "{{ ds }}",
                "--property-type",
                property_type,
            ],
            network_mode="spark_network",
            docker_url="unix://var/run/docker.sock",
            mount_tmp_dir=False,
            environment={
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_DRIVER_MEMORY": "2g",
                "SPARK_EXECUTOR_MEMORY": "2g",
            },
            mounts=[
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                )
            ],
        )

        # 5. Enrich - Làm giàu dữ liệu với thông tin địa lý
        enrich_task = DockerOperator(
            task_id=f"enrich_{property_type}",
            image=SPARK_DOCKER_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=[
                "jobs/enrichment/geo_enrichment.py",
                "--date",
                "{{ ds }}",
                "--property-type",
                property_type,
            ],
            network_mode="spark_network",
            docker_url="unix://var/run/docker.sock",
            mount_tmp_dir=False,
            environment={
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_DRIVER_MEMORY": "2g",
                "SPARK_EXECUTOR_MEMORY": "2g",
            },
            mounts=[
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                )
            ],
        )

        # 6. Load - Lưu trữ vào Delta Lake
        load_task = DockerOperator(
            task_id=f"load_{property_type}",
            image=SPARK_DOCKER_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=[
                "jobs/load/load_to_delta.py",
                "--date",
                "{{ ds }}",
                "--property-type",
                property_type,
                "--table-name",
                "property_data",
            ],
            network_mode="spark_network",
            docker_url="unix://var/run/docker.sock",
            mount_tmp_dir=False,
            environment={
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_DRIVER_MEMORY": "2g",
                "SPARK_EXECUTOR_MEMORY": "2g",
            },
            mounts=[
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                )
            ],
        )

        # Thiết lập phụ thuộc tác vụ
        if WAIT_FOR_CRAWLER:
            wait_for_crawler >> extract_task
        else:
            start >> extract_task

        (
            extract_task
            >> transform_task
            >> unify_task
            >> validate_task
            >> enrich_task
            >> load_task
            >> end
        )

    # Xử lý tích hợp tất cả dữ liệu (tùy chọn)
    process_all_data = DockerOperator(
        task_id="process_all_data",
        image=SPARK_DOCKER_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=[
            "pipelines/daily_processing.py",
            "--date",
            "{{ ds }}",
            "--property-type",
            "all",
        ],
        network_mode="spark_network",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "SPARK_DRIVER_MEMORY": "4g",  # More memory for the combined processing
            "SPARK_EXECUTOR_MEMORY": "4g",
        },
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
    )

    for property_type in PROPERTY_TYPES:
        dag.get_task(f"load_{property_type}") >> process_all_data

    process_all_data >> end
