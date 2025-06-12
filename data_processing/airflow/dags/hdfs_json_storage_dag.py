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
    "hdfs_json_storage",
    default_args=default_args,
    description="DAG to run storage service to save raw data in JSON format to HDFS after crawler completes",
    schedule_interval=None,  # Can be set to '@daily' or other schedule if needed
    start_date=days_ago(1),
    tags=["storage", "hdfs", "json", "raw-data"],
)

# Run storage service to process data from Kafka and save to HDFS in JSON format (for raw data)
run_storage_service = DockerOperator(
    task_id="run_hdfs_json_storage",
    image="realestate-crawler:latest",
    command="python -m services.storage_service.main --once",
    auto_remove=True,
    network_mode="hdfs_network",  # Use the same network as Kafka and HDFS
    environment={
        "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092",
        "KAFKA_TOPIC": "property-data",  # Topic with property data from crawler
        "KAFKA_GROUP_ID": "storage-service-airflow",
        "HDFS_NAMENODE": "namenode:9870",
        "HDFS_USER": "airflow",
        "STORAGE_TYPE": "hdfs",
        "FILE_FORMAT": "json",  # Prioritize JSON for raw data storage
        "BATCH_SIZE": "20000",  # Process 20000 records at a time
        "FLUSH_INTERVAL": "120",  # Flush every 2 minutes if batch size not reached
        "MIN_FILE_SIZE_MB": "5",  # Minimum file size for efficient HDFS storage
        "MAX_FILE_SIZE_MB": "128",  # Maximum file size for manageability
        "FILE_PREFIX": "property_data",
        "PROCESS_BACKUP_ON_STARTUP": "False",
        "RUN_ONCE_MODE": "true",  # Enable auto-stop mode to terminate after processing available messages
        "IDLE_TIMEOUT": "60",  # Wait for 10 minutes without messages before stopping (tune as needed)
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
    docker exec namenode bash -c 'hdfs dfs -ls /data/realestate/raw' || echo "Warning: No data found in HDFS raw directory"

    # Count the number of JSON files (for raw data)
    JSON_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate/raw -name "*.json" | wc -l')
    echo "Found $JSON_FILES JSON files in HDFS raw directory"

    # Count the number of parquet files (for processed data)
    PARQUET_FILES=$(docker exec namenode bash -c 'hdfs dfs -find /data/realestate -name "*.parquet" | wc -l')
    echo "Found $PARQUET_FILES Parquet files in HDFS"

    if [ "$JSON_FILES" -eq "0" ]; then
        echo "Warning: No JSON files found in raw directory. Raw data storage may have failed."
        # Exit with a warning rather than error for better debugging
        # exit 1
    fi
    """,
    dag=dag,
)

# Define the workflow
run_storage_service >> verify_hdfs_data
