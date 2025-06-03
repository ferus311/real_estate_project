from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "chotot_api_crawler",
    default_args=default_args,
    description="DAG crawler API Chotot",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["crawler", "chotot", "api"],
)

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

run_crawler = DockerOperator(
    task_id="run_chotot_api_crawler",
    image="realestate-crawler:latest",
    command="python -m services.api_crawler.main --once",
    auto_remove=True,
    network_mode="hdfs_network",
    environment={
        "SOURCE": "chotot",
        "START_PAGE": "1",
        "END_PAGE": "200",
        "OUTPUT_TOPIC": "property-data",
        "MAX_CONCURRENT": "5",
        "STOP_ON_EMPTY": "true",
        "MAX_EMPTY_PAGES": "5",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092",
    },
    mount_tmp_dir=False,
    mounts=[Mount(source="/crawler/checkpoint", target="/app/checkpoint", type="bind")],
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

check_logs = BashOperator(
    task_id="check_logs",
    bash_command="""
    echo "Checking crawler logs..."
    docker logs $(docker ps -alq --filter name=run_chotot_api_crawler) | tail -50

    if docker logs $(docker ps -alq --filter name=run_chotot_api_crawler) 2>&1 | grep -i "error\|exception\|fail"; then
        echo "Test failed: Found errors in logs"
        exit 1
    else
        echo "Test passed: No errors found"
    fi
    """,
    dag=dag,
)

check_kafka >> run_crawler >> check_logs
