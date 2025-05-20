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

# Kích hoạt DAG crawler
trigger_crawler = TriggerDagRunOperator(
    task_id="trigger_crawler_dag",
    trigger_dag_id="chotot_api_crawler",  # ID của DAG crawler - phải khớp với tên DAG thực tế
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=30,  # Kiểm tra mỗi 30 giây
    execution_date="{{ execution_date }}",
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG storage
trigger_storage = TriggerDagRunOperator(
    task_id="trigger_storage_dag",
    trigger_dag_id="storage_service_hdfs_parquet",  # ID của DAG storage
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

    # Kiểm tra số lượng file parquet trong HDFS
    NUM_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate -name "*.parquet" | wc -l')
    echo "Đã tìm thấy $NUM_FILES file parquet trong HDFS"

    # Log các thư mục để kiểm tra
    echo "Cấu trúc thư mục trong HDFS:"
    docker exec namenode bash -c 'hdfs dfs -ls -R /data/realestate | head -20'

    echo "Pipeline hoàn tất!"
    """,
    dag=dag,
)

# Định nghĩa luồng công việc
check_services >> trigger_crawler >> trigger_storage >> verify_results
