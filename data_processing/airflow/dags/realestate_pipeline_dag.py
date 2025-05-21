from datetime import timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "depends_on_past": False,
}

dag = DAG(
    "realestate_full_pipeline",
    default_args=default_args,
    description="Full pipeline: Run crawler and then storage service",
    schedule_interval=None,  # Chạy thủ công (có thể đặt lịch nếu cần)
    start_date=days_ago(1),
    tags=["pipeline", "crawler", "storage"],
)

# Kiểm tra trạng thái Kafka và HDFS
check_services = BashOperator(
    task_id="check_services",
    bash_command="""
    echo "Kiểm tra kết nối Kafka..."
    docker exec kafka1 bash -c 'nc -z kafka1 19092 -w 5' || \
    (echo "Kafka không khả dụng" && exit 1)

    echo "Kiểm tra kết nối HDFS..."
    docker exec namenode bash -c 'hdfs dfs -ls / >/dev/null 2>&1' || \
    (echo "HDFS không khả dụng" && exit 1)

    echo "Tất cả dịch vụ đều hoạt động bình thường"
    """,
    dag=dag,
)

# Kích hoạt DAG crawler API Chotot
trigger_api_crawler = TriggerDagRunOperator(
    task_id="trigger_api_crawler_dag",
    trigger_dag_id="chotot_api_crawler",  # DAG API crawler của Chotot
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=30,  # Kiểm tra mỗi 30 giây
    execution_date="{{ execution_date }}",
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG crawler Playwright Batdongsan
trigger_playwright_crawler = TriggerDagRunOperator(
    task_id="trigger_playwright_crawler_dag",
    trigger_dag_id="batdongsan_playwright_crawler",  # DAG Playwright crawler của Batdongsan
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=30,  # Kiểm tra mỗi 30 giây
    execution_date="{{ execution_date }}",
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG storage
trigger_storage = TriggerDagRunOperator(
    task_id="trigger_storage_dag",
    trigger_dag_id="storage_service_hdfs_raw_json",  # ID của DAG storage
    wait_for_completion=True,  # Đợi storage hoàn thành
    poke_interval=30,  # Kiểm tra mỗi 30 giây
    execution_date="{{ execution_date }}",
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kiểm tra xem dữ liệu đã được lưu đúng cách chưa
verify_results = BashOperator(
    task_id="verify_results",
    bash_command="""
    echo "Xác minh kết quả pipeline..."

    # Kiểm tra số lượng file JSON và parquet trong HDFS
    JSON_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate -name "*.json" | wc -l')
    PARQUET_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate -name "*.parquet" | wc -l')
    TOTAL_FILES=$((JSON_FILES + PARQUET_FILES))

    echo "Đã tìm thấy $JSON_FILES file JSON trong HDFS"
    echo "Đã tìm thấy $PARQUET_FILES file parquet trong HDFS"
    echo "Tổng số: $TOTAL_FILES files"

    # Log các thư mục để kiểm tra
    echo "Cấu trúc thư mục trong HDFS:"
    docker exec namenode bash -c 'hdfs dfs -ls -R /data/realestate | head -20'

    echo "Pipeline hoàn tất!"
    """,
    dag=dag,
)

# Định nghĩa luồng công việc
# Chạy crawlers song song, sau đó storage, và cuối cùng là xác minh
(
    check_services
    >> [trigger_api_crawler, trigger_playwright_crawler]
    >> trigger_storage
    >> verify_results
)
