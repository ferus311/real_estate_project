from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
from airflow import DAG
from airflow.models import TaskInstance
from datetime import timedelta, datetime
import re


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "depends_on_past": False,
}
dag = DAG(
    "test_realestate_data_processing",
    default_args=default_args,
    description="DAG xử lý dữ liệu bất động sản qua Spark",
    schedule_interval=None,  # DAG này được trigger bởi pipeline_dag
    start_date=datetime(2025, 5, 1),
    tags=["data_processing", "realestate"],
)

# lấy ngày giờ xử lý


processing_date = "{{ execution_date.strftime('%Y-%m-%d') }}"

# Thêm task hiển thị tham số đã nhận
show_parameters = BashOperator(
    task_id="show_parameters",
    bash_command=f"""
    echo "================= THÔNG TIN THAM SỐ NHẬN ĐƯỢC ================="
    echo "Ngày xử lý: {processing_date}"
    echo "================================================================"
    """,
    dag=dag,
)

show_parameters
