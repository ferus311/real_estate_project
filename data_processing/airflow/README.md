# Module Airflow

## Tổng Quan

Module Airflow chịu trách nhiệm điều phối và tự động hóa các quy trình xử lý dữ liệu, huấn luyện mô hình và triển khai hệ thống.

## Cấu Trúc Module

```
airflow/
├── dags/                  # Các DAG
│   ├── crawler/          # DAG thu thập dữ liệu
│   ├── processing/       # DAG xử lý dữ liệu
│   └── ml/              # DAG machine learning
├── operators/            # Custom operators
├── sensors/             # Custom sensors
└── utils/               # Tiện ích
```

## Các DAG Chính

1. **realestate_crawler_dag**

    ```mermaid
    graph TD
        A[Start] --> B[Crawl List]
        B --> C[Crawl Details]
        C --> D[Store Data]
        D --> E[End]
    ```

2. **realestate_processing_dag**

    ```mermaid
    graph TD
        A[Start] --> B[Data Cleaning]
        B --> C[Feature Engineering]
        C --> D[Data Validation]
        D --> E[End]
    ```

3. **realestate_ml_dag**
    ```mermaid
    graph TD
        A[Start] --> B[Train Model]
        B --> C[Evaluate Model]
        C --> D[Register Model]
        D --> E[Deploy Model]
        E --> F[End]
    ```

## Lịch Trình

1. **Crawler DAG**

    - Chạy mỗi 6 giờ
    - Retry 3 lần
    - Timeout 2 giờ

2. **Processing DAG**

    - Chạy mỗi 12 giờ
    - Phụ thuộc vào Crawler DAG
    - Timeout 4 giờ

3. **ML DAG**
    - Chạy mỗi 24 giờ
    - Phụ thuộc vào Processing DAG
    - Timeout 6 giờ

## Cấu Hình

```yaml
airflow:
    schedule_interval: '0 */6 * * *'
    catchup: false
    max_active_runs: 1
    concurrency: 4
    retries: 3
    retry_delay: 300
```

## Monitoring

1. **Metrics**

    - DAG success rate
    - Task duration
    - Resource usage
    - Error rate

2. **Alerts**
    - Email notifications
    - Slack integration
    - Error reporting
    - Performance alerts

## Tích Hợp

-   HDFS
-   Spark
-   Kafka
-   MLflow
-   Prometheus

## Bảo Mật

-   Authentication
-   Authorization
-   SSL/TLS
-   Secrets management

## Khôi Phục

-   Checkpointing
-   Backfill
-   Error handling
-   Data recovery
