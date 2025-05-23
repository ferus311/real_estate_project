# Xử lý dữ liệu bất động sản

Thư mục này chứa mã nguồn cho việc xử lý dữ liệu bất động sản từ các nguồn khác nhau (Batdongsan, Chotot) bằng Apache Spark.

## Cấu trúc thư mục

```
data_processing/
├── spark/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── schema/                  # Định nghĩa các schema cho dữ liệu
│   │   │   ├── batdongsan_schema.py
│   │   │   ├── chotot_schema.py
│   │   │   └── common_schema.py
│   │   ├── utils/                   # Các tiện ích chung
│   │   │   ├── __init__.py
│   │   │   ├── date_utils.py
│   │   │   ├── hdfs_utils.py
│   │   │   └── logging_utils.py
│   │   └── config/                  # Cấu hình cho pipeline
│   │       ├── __init__.py
│   │       └── spark_config.py
│   ├── jobs/                        # Các job xử lý Spark riêng biệt
│   │   ├── __init__.py
│   │   ├── extraction/              # Trích xuất dữ liệu từ raw
│   │   │   ├── extract_batdongsan.py
│   │   │   └── extract_chotot.py
│   │   ├── transformation/          # Chuyển đổi dữ liệu
│   │   │   ├── transform_batdongsan.py
│   │   │   ├── transform_chotot.py
│   │   │   └── unify_dataset.py     # Hợp nhất dữ liệu từ nhiều nguồn
│   │   ├── enrichment/              # Làm giàu dữ liệu
│   │   │   ├── geo_enrichment.py
│   │   │   └── text_feature_extraction.py
│   │   ├── data_quality/            # Kiểm tra chất lượng dữ liệu
│   │   │   ├── data_validation.py
│   │   │   └── data_profiling.py
│   │   └── load/                    # Tải dữ liệu vào data lake/warehouse
│   │       ├── load_to_delta.py
│   │       └── load_to_feature_store.py
│   ├── analys/                      # Phân tích dữ liệu
│   │   ├── market_trend.py
│   │   ├── price_analysis.py
│   │   └── location_analysis.py
│   ├── pipelines/                   # Định nghĩa các pipeline hoàn chỉnh
│   │   ├── daily_processing.py
│   │   ├── weekly_analytics.py
│   │   └── feature_engineering.py
│   └── tests/                       # Unit tests và integration tests
│       ├── test_extraction.py
│       ├── test_transformation.py
│       └── test_pipelines.py
└── resources/                       # Tài nguyên và cấu hình
    ├── log4j.properties
    └── app.conf
```

2. Tổ chức dữ liệu trên HDFS
   Ngoài cấu trúc thư mục code như trên, bạn nên tổ chức dữ liệu trên HDFS theo mô hình medallion architecture (kiến trúc huy chương) bao gồm các layer sau:

```
├── raw/ # Dữ liệu thô từ các nguồn (định dạng JSON ưu tiên)
│ ├── batdongsan/
│ │ ├── house/ # Nhà ở
│ │ │ └── yyyy/mm/dd # Phân vùng theo ngày để tối ưu lưu trữ
│ │ └── other/ # Các loại khác
│ │ └── yyyy/mm/dd
│ └── chotot/
│ ├── house/ # Nhà ở
│ │ └── yyyy/mm/dd
│ └── other/ # Các loại khác
│ └── yyyy/mm/dd
│
├── processed/ # Dữ liệu đã xử lý (định dạng Parquet)
│ ├── cleaned/ # Dữ liệu đã làm sạch
│ │ ├── house/ # Nhà ở
│ │ │ └── yyyy/mm/dd
│ │ └── all/ # Tất cả các loại
│ │ └── yyyy/mm/dd
│ ├── integrated/ # Dữ liệu đã tích hợp từ nhiều nguồn
│ │ ├── house/ # Nhà ở
│ │ │ └── yyyy/mm/dd
│ │ └── all/ # Tất cả các loại
│ │ └── yyyy/mm/dd
│ └── analytics/ # Dữ liệu tổng hợp cho phân tích
│ ├── price_trends/dd
│ │ ├── house/yyyy/mm/dd
│ │ ├── apartment/yyyy/mm/dd
│ │ ├── land/yyyy/mm/dd
│ │ └── all/yyyy/mm/dd
│ ├── region_stats/
│ │ ├── house/yyyy/mm/dd
│ │ ├── apartment/yyyy/mm/dd
│ │ ├── land/yyyy/mm/dd
│ │ └── all/yyyy/mm/dd
│ └── property_metrics/
│ ├── house/yyyy/mm/dd
│ ├── apartment/yyyy/mm/dd
│ ├── land/yyyy/mm/dd
│ └── all/yyyy/mm/dd
│

```

## Luồng xử lý dữ liệu

Luồng xử lý dữ liệu bất động sản theo các bước:

1. **Extraction**: Trích xuất dữ liệu thô từ HDFS và chuyển sang định dạng Parquet
2. **Transformation**: Làm sạch và chuyển đổi dữ liệu (chuyển kiểu dữ liệu, chuẩn hóa, v.v.)
3. **Integration**: Hợp nhất dữ liệu từ nhiều nguồn (Batdongsan, Chotot)
4. **Data Quality**: Kiểm tra chất lượng dữ liệu, lọc dữ liệu không hợp lệ
5. **Enrichment**: Làm giàu dữ liệu với thông tin bổ sung (geo, phân loại)
6. **Load**: Tải dữ liệu vào kho dữ liệu cuối cùng (Delta Lake)

## Schema dữ liệu

### Schema Batdongsan:

```
 |-- area: string (nullable = true)
 |-- bathroom: string (nullable = true)
 |-- bedroom: string (nullable = true)
 |-- crawl_timestamp: string (nullable = true)
 |-- data_type: string (nullable = true)
 |-- description: string (nullable = true)
 |-- facade_width: string (nullable = true)
 |-- floor_count: string (nullable = true)
 |-- house_direction: string (nullable = true)
 |-- interior: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- legal_status: string (nullable = true)
 |-- location: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- posted_date: string (nullable = true)
 |-- price: string (nullable = true)
 |-- price_per_m2: string (nullable = true)
 |-- road_width: string (nullable = true)
 |-- seller_info: string (nullable = true)
 |-- source: string (nullable = true)
 |-- title: string (nullable = true)
 |-- url: string (nullable = true)
```

### Schema Chotot:

```
 |-- area: string (nullable = true)
 |-- bathroom: string (nullable = true)
 |-- bedroom: string (nullable = true)
 |-- crawl_timestamp: string (nullable = true)
 |-- data_type: string (nullable = true)
 |-- description: string (nullable = true)
 |-- floor_count: string (nullable = true)
 |-- house_direction: string (nullable = true)
 |-- house_type: string (nullable = true)
 |-- interior: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- legal_status: string (nullable = true)
 |-- length: string (nullable = true)
 |-- living_size: string (nullable = true)
 |-- location: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- posted_date: string (nullable = true)
 |-- price: string (nullable = true)
 |-- price_per_m2: string (nullable = true)
 |-- seller_info: string (nullable = true)
 |-- source: string (nullable = true)
 |-- title: string (nullable = true)
 |-- url: string (nullable = true)
 |-- width: string (nullable = true)
```

## Chạy các job

### Chạy job riêng lẻ

Để chạy một job riêng lẻ:

```bash
spark-submit jobs/extraction/extract_batdongsan.py --date 2025-05-23 --property-type house
```

### Chạy pipeline đầy đủ

Để chạy toàn bộ pipeline:

```bash
spark-submit pipelines/daily_processing.py --date 2025-05-23 --property-types house other
```

## Lập lịch với Airflow

Pipeline đã được cấu hình để chạy tự động hàng ngày thông qua Apache Airflow. DAG `realestate_data_processing` được định nghĩa trong thư mục `data_processing/airflow/dags/`.

## Thiết lập môi trường

Cần cài đặt:

-   Apache Spark 3.3.0+
-   Delta Lake 2.1.0
-   Python 3.9+

## Phụ thuộc

-   PySpark
-   Delta Lake
-   Hadoop HDFS Client
