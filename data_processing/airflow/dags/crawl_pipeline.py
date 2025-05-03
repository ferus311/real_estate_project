from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG chạy hàng ngày lúc 2:00 AM
with DAG(
    'real_estate_crawler_pipeline',
    default_args=default_args,
    description='Crawl và xử lý dữ liệu bất động sản theo lô lớn',
    schedule_interval='0 2 * * *',  # Chạy lúc 2:00 AM hàng ngày
    start_date=datetime(2025, 5, 1),
    catchup=False,
) as dag:

    # Task 1: Crawl nhiều trang danh sách
    crawl_list = BashOperator(
        task_id='crawl_list_layer',
        bash_command='python /opt/real_estate_crawler/list_layer/crawl_list.py '
                    '--start_page 1 --end_page 100 --concurrent 10 '  # Crawl nhiều trang hơn
                    '--output /opt/real_estate_crawler/data/urls_{{ ds }}.json',
    )

    # Task 2: Crawl chi tiết với số lượng lớn và xử lý song song
    crawl_detail = BashOperator(
        task_id='crawl_detail_layer',
        bash_command='python /opt/real_estate_crawler/detail_layer/crawl_detail.py '
                    '--input /opt/real_estate_crawler/data/urls_{{ ds }}.json '
                    '--output /data/test/raw_data/posts_{{ ds }}.parquet '  # Lưu trực tiếp vào HDFS
                    '--concurrent 20 --batch_size 500',  # Tăng độ song song và batch size
    )

    # Task 3: Chạy spark job để xử lý và biến đổi dữ liệu
    process_data = BashOperator(
        task_id='process_data_spark',
        bash_command='spark-submit '
                    '--master spark://spark-master:7077 '
                    '--deploy-mode client '
                    '/opt/real_estate_crawler/spark_jobs/transform_data.py '
                    '--input /data/test/raw_data/posts_{{ ds }}.parquet '
                    '--output /data/test/processed/cleaned_posts_{{ ds }}.parquet',
    )

    # Định nghĩa thứ tự chạy các tasks
    crawl_list >> crawl_detail >> process_data
