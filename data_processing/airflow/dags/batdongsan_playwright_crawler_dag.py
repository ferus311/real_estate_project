from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "batdongsan_playwright_crawler",
    default_args=default_args,
    description="DAG crawler Playwright Badongsan",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["crawler", "batdongsan", "playwright"],
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

run_list_crawler = DockerOperator(
    task_id="run_batdongsan_playwright_list_crawler",
    image="crawler-list:latest",
    command="python -m services.list_crawler.main --once",
    auto_remove=True,
    network_mode="hdfs_network",
    environment={
        "SOURCE": "batdongsan",
        "CRAWLER_TYPE": "playwright",
        "START_PAGE": "1",
        "END_PAGE": "100",
        "OUTPUT_TOPIC": "property-urls",
        "MAX_CONCURRENT": "5",
        "STOP_ON_EMPTY": "true",
        "MAX_EMPTY_PAGES": "5",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092",
        "HDFS_NAMENODE": "namenode:9870",
    },
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)
check_list_logs = BashOperator(
    task_id="check_list_logs",
    bash_command="""
    echo "Checking crawler logs..."
    docker logs $(docker ps -alq --filter name=run_batdongsan_playwright_list_crawler) | tail -50
    """,
    dag=dag,
)

run_detail_crawler = DockerOperator(
    task_id="run_batdongsan_playwright_detail_crawler",
    image="crawler-detail:latest",
    command="python -m services.detail_crawler.main --once",
    auto_remove=True,
    network_mode="hdfs_network",
    environment={
        "SOURCE": "batdongsan",
        "OUTPUT_TOPIC": "property-data",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092",
        "HDFS_NAMENODE": "namenode:9870",
        "MAX_CONCURRENT": "7",
        "MAX_RETRIES": "3",
        "CRAWLER_TYPE": "playwright",
    },
    docker_url="unix://var/run/docker.sock",
    dag=dag,
)

check_detail_logs = BashOperator(
    task_id="check_detail_logs",
    bash_command="""
    echo "Checking crawler logs..."
    docker logs $(docker ps -alq --filter name=run_batdongsan_playwright_detail_crawler) | tail -50
    """,
    dag=dag,
)


check_kafka >> run_list_crawler >> check_list_logs >> run_detail_crawler >> check_detail_logs
