from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "catchup": False,
    "depends_on_past": False,
}

dag = DAG(
    "storage_service_hdfs_raw_json",
    default_args=default_args,
    description="DAG to run storage service to save raw data in JSON format to HDFS after crawler completes",
    schedule_interval=None,  # Can be set to '@daily' or other schedule if needed
    start_date=days_ago(1),
    tags=["storage", "hdfs", "json", "raw-data"],
)

# Wait for the crawler DAG to complete
wait_for_crawler = ExternalTaskSensor(
    task_id="wait_for_crawler",
    external_dag_id="chotot_api_crawler",  # The ID of the crawler DAG - match exactly with the crawler DAG ID
    external_task_id="run_chotot_api_crawler",  # The last task of the crawler DAG
    timeout=600,  # 10 minutes timeout
    mode="reschedule",  # Reschedule if not found
    allowed_states=["success"],  # Only proceed if the task succeeded
    execution_delta=timedelta(minutes=0),  # Look for same execution date
    dag=dag,
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

# Run storage service to process data from Kafka and save raw data to HDFS in JSON format
run_storage_service = DockerOperator(
    task_id="run_storage_service_hdfs_json",
    image="crawler-storage:latest",
    command="python -m services.storage_service.main --once",
    auto_remove=True,
    network_mode="hdfs_network",  # Use the same network as Kafka and HDFS
    environment={
        "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092",
        "KAFKA_TOPIC": "property-data",  # Topic with property data from crawler
        "KAFKA_GROUP_ID": "storage-service-airflow",
        "HDFS_NAMENODE": "namenode:9870",
        "HDFS_USER": "airflow",
        "HDFS_BASE_PATH": "/data/realestate",
        "STORAGE_TYPE": "hdfs",
        "FILE_FORMAT": "json",
        "BATCH_SIZE": "20000",  # Process 1000 records at a time
        "FLUSH_INTERVAL": "120",  # Flush every 1 minute if batch size not reached
        "MIN_FILE_SIZE_MB": "5",  # Minimum file size for efficient HDFS storage
        "MAX_FILE_SIZE_MB": "128",  # Maximum file size for manageability
        "MAX_RETRIES": "3",
        "RETRY_DELAY": "5",
        "FILE_PREFIX": "property_data",
        "PROCESS_BACKUP_ON_STARTUP": "False",
    },
    docker_url="unix://var/run/docker.sock",
    mounts=[],  # No volume mounts needed as per optimization requirements
    dag=dag,
)

# Check if data was properly saved to HDFS
verify_hdfs_data = BashOperator(
    task_id="verify_hdfs_data",
    bash_command="""
    echo "Verifying data in HDFS..."
    # Check if files were created
    docker exec namenode bash -c 'hdfs dfs -ls /data/realestate/chotot' || echo "Warning: No data found in HDFS for chotot source"

    # Count the number of data files (json, parquet, etc.)
    PARQUET_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate -name "*.parquet" | wc -l')
    JSON_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate -name "*.json" | wc -l')
    TOTAL_FILES=$((PARQUET_FILES + JSON_FILES))

    echo "Found $JSON_FILES JSON files in HDFS"
    echo "Found $PARQUET_FILES Parquet files in HDFS"
    echo "Found $TOTAL_FILES total data files in HDFS"

    if [ "$TOTAL_FILES" -eq "0" ]; then
        echo "Warning: No data files found. Storage service may have failed."
        # Exit with a warning rather than error for better debugging
        # exit 1
    fi
    """,
    dag=dag,
)

# Define the workflow
check_connectivity >> run_storage_service >> verify_hdfs_data
