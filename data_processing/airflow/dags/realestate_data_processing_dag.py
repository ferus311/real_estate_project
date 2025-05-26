from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
from airflow import DAG
from airflow.models import TaskInstance
from datetime import timedelta
import re


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "depends_on_past": False,
}
dag = DAG(
    "realestate_data_processing",
    default_args=default_args,
    description="DAG xử lý dữ liệu bất động sản qua Spark",
    schedule_interval=None,  # DAG này được trigger bởi pipeline_dag
    start_date=days_ago(1),
    tags=["data_processing", "realestate"],
)

# Tham số ngày xử lý từ kích hoạt (có thể được truyền từ pipeline chính)
processing_date = "{{ dag_run.conf.get('processing_date') or execution_date.strftime('%Y-%m-%d') }}"
property_types = "{{ dag_run.conf.get('property_types', 'house') }}"

# processing_date = "2025-05-22"
# Chạy xử lý đầy đủ (Raw → Bronze → Silver → Gold)
run_processing = DockerOperator(
    task_id="run_full_processing",
    image="spark-processor:latest",
    command=f"python /app/pipelines/daily_processing.py --date {processing_date} --property-types {property_types}",
    network_mode="hdfs_network",
    api_version="auto",
    auto_remove=True,
    mount_tmp_dir=False,
    environment={
        "SPARK_MASTER_URL": "spark://spark-master:7077",
        "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
        "HDFS_NAMENODE_ADDRESS": "hdfs://namenode:9000",
    },
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

# Thêm task hiển thị tham số đã nhận
show_parameters = BashOperator(
    task_id="show_parameters",
    bash_command=f"""
    echo "================= THÔNG TIN THAM SỐ NHẬN ĐƯỢC ================="
    echo "Ngày xử lý: {processing_date}"
    echo "Loại bất động sản: {property_types}"
    echo "================================================================"
    """,
    dag=dag,
)


# Kiểm tra kết quả xử lý dữ liệu bằng cách đọc logs từ task trước
def verify_processing_results(ti, **context):
    # processing_date = context["dag_run"].conf.get("processing_date", None)
    if processing_date is None:
        from datetime import datetime, timedelta

        # Mặc định là ngày hôm qua nếu không có tham số
        processing_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Lấy logs từ task run_processing
    task_logs = ti.xcom_pull(task_ids="run_full_processing")
    if not task_logs:
        # Nếu không lấy được qua xcom, thử đọc logs trực tiếp
        task_instance = TaskInstance(
            ti.task.dag.get_task("run_full_processing"), ti.execution_date
        )
        logs = "\n".join(task_instance.log.splitlines())
    else:
        logs = task_logs

    print("================= BÁO CÁO XỬ LÝ NGÀY", processing_date, "=================")

    # Tìm thông tin về số lượng file được xử lý từ logs
    bronze_files = 0
    silver_files = 0
    gold_files = 0

    # Phân tích logs để tìm các dòng liên quan đến kết quả xử lý
    if "Successfully processed" in logs:
        print("✅ Tìm thấy thông báo xử lý thành công trong logs")

        # Tìm thông tin về Bronze files
        bronze_matches = re.findall(r"Bronze: (\d+) records", logs)
        if bronze_matches:
            bronze_files = sum([int(count) for count in bronze_matches])

        # Tìm thông tin về Silver files
        silver_matches = re.findall(r"Silver: (\d+) records", logs)
        if silver_matches:
            silver_files = sum([int(count) for count in silver_matches])

        # Tìm thông tin về Gold files
        gold_matches = re.findall(r"Unified Gold: (\d+) records", logs)
        if gold_matches:
            gold_files = sum([int(count) for count in gold_matches])

    # Nếu không tìm được trong logs, thực hiện kiểm tra HDFS trực tiếp
    if gold_files == 0:
        # Format ngày để tìm kiếm file
        date_format = processing_date.replace("-", "")

        print("Không tìm thấy thông tin đầy đủ trong logs, kiểm tra HDFS trực tiếp...")
        # Sử dụng câu lệnh bash để kiểm tra trực tiếp
        import subprocess

        # Kiểm tra các layer
        bronze_cmd = f'docker exec namenode bash -c \'hdfs dfs -find /data/realestate/processed/bronze -name "*.parquet" -path "*{processing_date}*" | wc -l\''
        silver_cmd = f'docker exec namenode bash -c \'hdfs dfs -find /data/realestate/processed/silver -name "*.parquet" -path "*{processing_date}*" | wc -l\''
        gold_cmd = f'docker exec namenode bash -c \'hdfs dfs -find /data/realestate/processed/gold -name "*.parquet" -path "*{processing_date}*" | wc -l\''

        try:
            bronze_files = int(
                subprocess.check_output(bronze_cmd, shell=True).decode().strip()
            )
            silver_files = int(
                subprocess.check_output(silver_cmd, shell=True).decode().strip()
            )
            gold_files = int(
                subprocess.check_output(gold_cmd, shell=True).decode().strip()
            )
        except Exception as e:
            print(f"Lỗi khi kiểm tra HDFS: {str(e)}")

    print("Đã tìm thấy", bronze_files, "bản ghi trong Bronze Layer")
    print("Đã tìm thấy", silver_files, "bản ghi trong Silver Layer")
    print("Đã tìm thấy", gold_files, "bản ghi trong Gold Layer")

    # Nếu không có file nào trong Gold layer, quá trình xử lý đã thất bại
    if gold_files == 0:
        print(
            f"❌ CẢNH BÁO: Không tìm thấy dữ liệu Gold nào cho ngày {processing_date}!"
        )
        print("Hãy kiểm tra logs chi tiết để xác định vấn đề.")
        raise Exception("Không tìm thấy dữ liệu Gold, xử lý thất bại.")
    else:
        print("✅ Xử lý dữ liệu hoàn tất thành công với", gold_files, "bản ghi!")
        return True


# verify_results = PythonOperator(
#     task_id="verify_results",
#     python_callable=verify_processing_results,
#     provide_context=True,
#     dag=dag,
# )

# Định nghĩa luồng công việc đơn giản
show_parameters >> run_processing
