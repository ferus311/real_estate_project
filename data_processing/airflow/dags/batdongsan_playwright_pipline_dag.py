from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "batdongsan_playwright_pipline",
    default_args=default_args,
    description="DAG to run Batdongsan Playwright crawler and store data in HDFS",
    schedule_interval=None,  # Can be set to '@daily' or other schedule if needed
    start_date=days_ago(1),
    tags=["crawler", "batdongsan", "playwright"],
)
# Check Kafka and HDFS connectivity
check_kafka = BashOperator(
    task_id="check_kafka",
    bash_command="""
    echo "Checking Kafka connection..."
    docker exec kafka1 bash -c 'nc -z kafka1 19092 -w 5' || \
    (echo "Kafka is not available" && exit 1)
    echo "Kafka is available"
    """,
    dag=dag,
)
check_hdfs = BashOperator(
    task_id="check_hdfs",
    bash_command="""
    echo "Checking HDFS connection..."
    docker exec namenode bash -c 'hdfs dfs -ls / >/dev/null 2>&1' || \
    (echo "HDFS is not available" && exit 1)
    echo "HDFS is available"
    """,
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

verify_results = BashOperator(
    task_id="verify_results",
    bash_command="""
    echo "Verifying results..."
    # Add your verification logic here
    # For example, check if the output files exist in HDFS
    docker exec namenode bash -c 'hdfs dfs -ls /data/realestate' || \
    (echo "Verification failed" && exit 1)
    echo "Verification successful"
    """,
    dag=dag,
)

(
    check_kafka
    >> check_hdfs
    >> trigger_playwright_crawler
    >> trigger_storage
    >> verify_results
)
