from datetime import timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "catchup": False,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "realestate_full_pipeline_daily",
    default_args=default_args,
    description="Pipeline đầy đủ: Crawler -> Storage -> Xử lý dữ liệu",
    # schedule_interval="0 2 * * *",  # Chạy vào 2 giờ sáng hàng ngày
    schedule_interval=None,  # Chạy thủ công (có thể đặt lịch nếu cần)
    start_date=days_ago(1),
    tags=["pipeline", "crawler", "storage", "processing"],
)

# Tham số ngày xử lý - lấy ngày hôm trước để xử lý dữ liệu từ crawler
processing_date = (
    "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
)

# Kiểm tra trạng thái Kafka, HDFS, Spark
check_services = BashOperator(
    task_id="check_services",
    bash_command="""
    echo "Kiểm tra kết nối Kafka..."
    docker exec kafka1 bash -c 'nc -z kafka1 19092 -w 5' || \
    (echo "Kafka không khả dụng" && exit 1)

    echo "Kiểm tra kết nối HDFS..."
    docker exec namenode bash -c 'hdfs dfs -ls / >/dev/null 2>&1' || \
    (echo "HDFS không khả dụng" && exit 1)

    echo "Kiểm tra kết nối Spark..."
    docker exec spark-master bash -c 'nc -z spark-master 7077 -w 5' || \
    (echo "Spark không khả dụng" && exit 1)

    echo "Tất cả dịch vụ đều hoạt động bình thường"
    """,
    dag=dag,
)

# Kích hoạt DAG crawler API Chotot
trigger_api_crawler = TriggerDagRunOperator(
    task_id="trigger_api_crawler_dag",
    trigger_dag_id="chotot_api_crawler",  # DAG API crawler của Chotot
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    execution_date="{{ execution_date }}",
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG crawler Playwright Batdongsan
trigger_playwright_crawler = TriggerDagRunOperator(
    task_id="trigger_playwright_crawler_dag",
    trigger_dag_id="batdongsan_playwright_crawler",  # DAG Playwright crawler của Batdongsan
    wait_for_completion=True,  # Đợi crawler hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    execution_date="{{ execution_date }}",
    reset_dag_run=True,  # Reset nếu DAG đã tồn tại
    dag=dag,
)

# Kích hoạt DAG storage để lưu trữ dữ liệu vào HDFS
trigger_storage = TriggerDagRunOperator(
    task_id="trigger_storage_dag",
    trigger_dag_id="storage_service_hdfs_json_raw",  # ID của DAG storage
    wait_for_completion=True,  # Đợi storage hoàn thành
    poke_interval=60,  # Kiểm tra mỗi 60 giây
    execution_date="{{ execution_date }}",
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
    conf={"processing_date": processing_date},  # Truyền ngày xử lý
    dag=dag,
)

# Kiểm tra xem toàn bộ pipeline đã hoàn thành thành công chưa
# verify_pipeline = BashOperator(
#     task_id="verify_pipeline",
#     bash_command=f"""
#     echo "================= BÁO CÁO TOÀN BỘ PIPELINE - NGÀY {processing_date} ================="

#     # Kiểm tra số lượng file ở các layer dữ liệu trong HDFS
#     RAW_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate/raw -name "*_{processing_date.replace('-', '')}.json" | wc -l')
#     GOLD_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate/processed/gold -name "*.parquet" -path "*{processing_date}*" | wc -l')

#     echo "Số lượng file ở Raw Layer: $RAW_FILES"
#     echo "Số lượng file ở Gold Layer: $GOLD_FILES"

#     if [ "$GOLD_FILES" -eq "0" ]; then
#         echo "CẢNH BÁO: Pipeline không hoàn thành thành công!"
#         echo "Hãy kiểm tra logs để xác định vấn đề."
#         exit 1
#     else
#         echo "Pipeline hoàn tất thành công! Dữ liệu đã sẵn sàng để sử dụng."
#     fi
#     """,
#     dag=dag,
# )

# Định nghĩa luồng công việc
# 1. Kiểm tra dịch vụ
# 2. Chạy crawlers song song
# 3. Storage
# 4. Chờ để đảm bảo dữ liệu được lưu trữ
# 5. Kích hoạt DAG xử lý dữ liệu
# 6. Xác minh toàn bộ pipeline
(
    check_services
    >> [trigger_api_crawler, trigger_playwright_crawler]
    >> trigger_storage
    >> wait_for_storage
    >> trigger_data_processing
)
