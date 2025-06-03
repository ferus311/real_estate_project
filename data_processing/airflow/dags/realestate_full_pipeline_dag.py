from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago



from pendulum import timezone
from datetime import timedelta, datetime


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

dag = DAG(
    "realestate_full_pipeline_daily",
    default_args=default_args,
    description="Pipeline đầy đủ: Crawler -> Storage -> Xử lý dữ liệu",
    schedule_interval="0 2 * * *",  # Chạy vào 2 giờ sáng hàng ngày
    catchup=False,  # Không chạy lại các ngày trước
    start_date=datetime(2025, 6, 1, tzinfo=timezone("Asia/Ho_Chi_Minh")),  # Ngày bắt đầu DAG
    tags=["pipeline", "crawler", "storage", "processing"],
)

# Check Kafka and HDFS connectivity
check_connectivity = BashOperator(
    task_id="check_kafka_hdfs",
    bash_command="""
    echo "Checking Kafka connection..."
    docker exec kafka1 bash -c 'nc -z kafka1 19092 -w 5' || \
    (echo "Kafka is not available" && exit 1)

    echo "Checking HDFS connection..."
    docker exec namenode bash -c 'hdfs dfs -ls / >/dev/null 2>&1' || \
    (echo "HDFS is not available" && exit 1)

    echo "Connections OK"
    """,
    dag=dag,
)

show_etl_processing_date = BashOperator(
    task_id="show_etl_processing_date",
    bash_command="""
    echo "================= Thông tin pipeline ================="
    echo "Ngày xử lý dữ liệu (execution_date): {{ ds }}"
    echo "Ngày kết thúc khoảng thời gian: {{ data_interval_end | ds }}"
    echo "Thời gian thực tế chạy DAG: $(date +%Y-%m-%d\ %H:%M:%S)"
    echo "======================================================"
    """,
    dag=dag,
)

# Kích hoạt DAG crawler API Chotot
trigger_api_crawler = TriggerDagRunOperator(
    task_id="trigger_api_crawler_dag",
    trigger_dag_id="chotot_api_crawler",  # DAG API crawler của Chotot
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG crawler Playwright Batdongsan
trigger_playwright_crawler = TriggerDagRunOperator(
    task_id="trigger_playwright_crawler_dag",
    trigger_dag_id="batdongsan_playwright_crawler",  # DAG Playwright crawler của Batdongsan
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG storage để lưu trữ dữ liệu vào HDFS
trigger_storage = TriggerDagRunOperator(
    task_id="trigger_storage_dag",
    trigger_dag_id="storage_service_hdfs_json_raw",  # ID của DAG storage
    wait_for_completion=True,  # Đợi storage hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Chờ một khoảng thời gian để dữ liệu được lưu trữ hoàn tất
wait_for_storage = BashOperator(
    task_id="wait_for_storage_completion",
    bash_command="echo 'Đợi 3 phút để đảm bảo dữ liệu đã được lưu trữ hoàn tất' && sleep 180",
    dag=dag,
)

# Kích hoạt DAG xử lý dữ liệu
trigger_data_processing = TriggerDagRunOperator(
    task_id="trigger_data_processing_dag",
    trigger_dag_id="realestate_data_processing",  # ID của DAG xử lý dữ liệu
    wait_for_completion=True,  # Đợi xử lý hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Định nghĩa luồng công việc
# 1. Kiểm tra dịch vụ
# 2. Chạy crawlers song song
# 3. Storage
# 4. Chờ để đảm bảo dữ liệu được lưu trữ
# 5. Kích hoạt DAG xử lý dữ liệu
# 6. Xác minh toàn bộ pipeline
(
    show_etl_processing_date
    >> check_connectivity
    >> [trigger_api_crawler, trigger_playwright_crawler]
    >> trigger_storage
    >> wait_for_storage
    >> trigger_data_processing
)
