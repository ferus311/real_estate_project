"""
DAG xử lý dữ liệu bất động sản
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os

# Cấu hình mặc định cho DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Thư mục gốc chứa mã nguồn
SPARK_DIR = "/home/fer/data/real_estate_project/data_processing/spark"

# Tạo DAG
dag = DAG(
    "realestate_data_processing",
    default_args=default_args,
    description="Real Estate Data Processing Pipeline",
    schedule_interval="0 1 * * *",  # Chạy hàng ngày lúc 1 giờ sáng
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["realestate", "data_processing"],
)

# Operator bắt đầu
start = DummyOperator(
    task_id="start_pipeline",
    dag=dag,
)

# Operator kết thúc
end = DummyOperator(
    task_id="end_pipeline",
    dag=dag,
)

# TaskGroup cho xử lý dữ liệu nhà ở (house)
with TaskGroup(group_id="process_house_data", dag=dag) as process_house:
    # 1. Extract Data
    extract_batdongsan_house = BashOperator(
        task_id="extract_batdongsan_house",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/extraction/extract_batdongsan.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    extract_chotot_house = BashOperator(
        task_id="extract_chotot_house",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/extraction/extract_chotot.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    # 2. Transform Data
    transform_batdongsan_house = BashOperator(
        task_id="transform_batdongsan_house",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/transform_batdongsan.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    transform_chotot_house = BashOperator(
        task_id="transform_chotot_house",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/transform_chotot.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    # 3. Unify Data
    unify_house_data = BashOperator(
        task_id="unify_house_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/unify_dataset.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    # 4. Data Quality
    validate_house_data = BashOperator(
        task_id="validate_house_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/data_quality/data_validation.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    # 5. Enrichment
    enrich_house_data = BashOperator(
        task_id="enrich_house_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/enrichment/geo_enrichment.py --date {{{{ ds }}}} --property-type house",
        dag=dag,
    )

    # 6. Load to Delta
    load_house_to_delta = BashOperator(
        task_id="load_house_to_delta",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/load/load_to_delta.py --date {{{{ ds }}}} --property-type house --table-name property_data",
        dag=dag,
    )

    # Set up task dependencies
    [extract_batdongsan_house, extract_chotot_house] >> [
        transform_batdongsan_house,
        transform_chotot_house,
    ]
    (
        [transform_batdongsan_house, transform_chotot_house]
        >> unify_house_data
        >> validate_house_data
        >> enrich_house_data
        >> load_house_to_delta
    )

# TaskGroup cho xử lý dữ liệu loại khác (other)
with TaskGroup(group_id="process_other_data", dag=dag) as process_other:
    # 1. Extract Data
    extract_batdongsan_other = BashOperator(
        task_id="extract_batdongsan_other",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/extraction/extract_batdongsan.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    extract_chotot_other = BashOperator(
        task_id="extract_chotot_other",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/extraction/extract_chotot.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    # 2. Transform Data
    transform_batdongsan_other = BashOperator(
        task_id="transform_batdongsan_other",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/transform_batdongsan.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    transform_chotot_other = BashOperator(
        task_id="transform_chotot_other",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/transform_chotot.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    # 3. Unify Data
    unify_other_data = BashOperator(
        task_id="unify_other_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/unify_dataset.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    # 4. Data Quality
    validate_other_data = BashOperator(
        task_id="validate_other_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/data_quality/data_validation.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    # 5. Enrichment
    enrich_other_data = BashOperator(
        task_id="enrich_other_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/enrichment/geo_enrichment.py --date {{{{ ds }}}} --property-type other",
        dag=dag,
    )

    # 6. Load to Delta
    load_other_to_delta = BashOperator(
        task_id="load_other_to_delta",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/load/load_to_delta.py --date {{{{ ds }}}} --property-type other --table-name property_data",
        dag=dag,
    )

    # Set up task dependencies
    [extract_batdongsan_other, extract_chotot_other] >> [
        transform_batdongsan_other,
        transform_chotot_other,
    ]
    (
        [transform_batdongsan_other, transform_chotot_other]
        >> unify_other_data
        >> validate_other_data
        >> enrich_other_data
        >> load_other_to_delta
    )

# TaskGroup cho xử lý dữ liệu tổng hợp (all)
with TaskGroup(group_id="process_all_data", dag=dag) as process_all:
    # Hợp nhất và đánh giá tất cả dữ liệu
    unify_all_data = BashOperator(
        task_id="unify_all_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/transformation/unify_dataset.py --date {{{{ ds }}}} --property-type all",
        dag=dag,
    )

    validate_all_data = BashOperator(
        task_id="validate_all_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/data_quality/data_validation.py --date {{{{ ds }}}} --property-type all",
        dag=dag,
    )

    enrich_all_data = BashOperator(
        task_id="enrich_all_data",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/enrichment/geo_enrichment.py --date {{{{ ds }}}} --property-type all",
        dag=dag,
    )

    load_all_to_delta = BashOperator(
        task_id="load_all_to_delta",
        bash_command=f"spark-submit {SPARK_DIR}/jobs/load/load_to_delta.py --date {{{{ ds }}}} --property-type all --table-name property_all_data",
        dag=dag,
    )

    # Set up task dependencies
    unify_all_data >> validate_all_data >> enrich_all_data >> load_all_to_delta

# Thiết lập thứ tự thực hiện các task group
start >> [process_house, process_other] >> process_all >> end
