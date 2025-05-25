# Hệ Thống Thu Thập và Xử Lý Dữ Liệu Bất Động Sản

## Tổng Quan Dự Án

Đây là một hệ thống hoàn chỉnh thu thập, xử lý và phân tích dữ liệu bất động sản từ các nguồn khác nhau (Batdongsan.com.vn và Chotot.vn). Hệ thống được xây dựng theo kiến trúc microservices với các công nghệ hiện đại để đảm bảo khả năng mở rộng, độ tin cậy và hiệu suất cao.

## Kiến Trúc Hệ Thống

### Sơ Đồ Kiến Trúc Tổng Quan

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DATA SOURCES  │    │   CRAWLERS      │    │   MESSAGING     │
│                 │    │                 │    │                 │
│ • Batdongsan.vn │───▶│ • List Crawler  │───▶│    Apache       │
│ • Chotot.vn     │    │ • Detail Crawler│    │    Kafka        │
│                 │    │ • API Crawler   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   WEB FRONTEND  │    │  DATA STORAGE   │    │ STORAGE SERVICE │
│                 │    │                 │    │                 │
│ • React + Vite  │◀───│ • HDFS (Raw)    │◀───│ • Kafka → HDFS  │
│ • Ant Design    │    │ • Delta Lake    │    │ • Format Conv.  │
│ • TypeScript    │    │ • Parquet       │    │ • Partitioning  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   REST API      │    │ DATA PROCESSING │    │   ORCHESTRATION │
│                 │    │                 │    │                 │
│ • FastAPI       │    │ • Apache Spark  │    │ • Apache Airflow│
│ • Data Serving  │    │ • ETL Pipelines │    │ • DAG Workflows │
│ • ML Predictions│    │ • ML Training   │    │ • Scheduling    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Luồng Dữ Liệu Chi Tiết

### 1. Thu Thập Dữ Liệu (Data Collection)

#### 1.1 Crawler Services

```
┌─────────────────────────────────────────────────────────────────┐
│                        CRAWLER LAYER                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  List Crawler   │  │ Detail Crawler  │  │  API Crawler    │  │
│  │                 │  │                 │  │                 │  │
│  │ • Playwright    │  │ • Playwright    │  │ • REST API      │  │
│  │ • Page Lists    │  │ • Property      │  │ • JSON Response │  │
│  │ • URL Extract   │  │   Details       │  │ • Pagination    │  │
│  │ • Pagination    │  │ • Rich Content  │  │ • Rate Limiting │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│          │                     │                     │          │
│          ▼                     ▼                     ▼          │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │             Kafka Topics (property-data)                   │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Workflow Chi Tiết:**

1. **List Crawler**: Thu thập danh sách URL từ trang chủ

    - Batdongsan: Sử dụng Selenium cho JavaScript rendering
    - Chotot: Sử dụng API endpoints trực tiếp

2. **Detail Crawler**: Thu thập thông tin chi tiết từ từng URL

    - Sử dụng Playwright cho performance tốt hơn
    - Xử lý lazy loading và dynamic content

3. **Kafka Integration**: Gửi dữ liệu realtime
    - Topic: `property-data` và `property-urls`
    - Partitioning theo source và property_type
    - Retry mechanism và error handling

### 2. Lưu Trữ Dữ Liệu (Data Storage)

#### 2.1 Storage Service

```
┌─────────────────────────────────────────────────────────────────┐
│                        STORAGE SERVICE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Kafka Consumer ──▶ Buffer Management ──▶ HDFS Writer          │
│       │                      │                    │             │
│       │              ┌─────────────────┐          │             │
│       │              │ Batch Size:     │          │             │
│       │              │ 20,000 records  │          │             │
│       │              │ Flush Interval: │          │             │
│       │              │ 5 minutes       │          │             │
│       │              └─────────────────┘          │             │
│       │                      │                    │             │
│       ▼                      ▼                    ▼             │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │              HDFS File Structure                            │  │
│  │  /data/realestate/raw/{source}/{type}/{yyyy/mm/dd}/        │  │
│  │  - property_data_YYYYMMDD_HHMMSS.json                      │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Cấu Trúc Dữ Liệu HDFS:**

```
hdfs://namenode:9000/data/realestate/
├── raw/                          # Dữ liệu thô (JSON)
│   ├── batdongsan/
│   │   ├── house/2025/05/25/
│   │   └── other/2025/05/25/
│   └── chotot/
│       ├── house/2025/05/25/
│       └── other/2025/05/25/
├── processed/                    # Dữ liệu đã xử lý (Parquet)
│   ├── cleaned/
│   ├── integrated/
│   └── analytics/
└── ml/                          # Dữ liệu ML
    ├── features/
    ├── training/
    └── models/
```

### 3. Xử Lý Dữ Liệu (Data Processing)

#### 3.1 Spark ETL Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPARK PROCESSING PIPELINE                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────┐  │
│  │ EXTRACTION  │  │TRANSFORMATION│  │ ENRICHMENT  │  │  LOAD  │  │
│  │             │  │             │  │             │  │        │  │
│  │ • Read JSON │─▶│ • Clean Data│─▶│ • Geo Info  │─▶│ Delta  │  │
│  │ • Schema    │  │ • Normalize │  │ • Features  │  │ Lake   │  │
│  │   Validation│  │ • Dedupe    │  │ • Validation│  │        │  │
│  │ • Filter    │  │ • Transform │  │ • ML Prep   │  │        │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Các Bước Xử Lý:**

1. **Extraction** (`jobs/extraction/`):

    - `extract_batdongsan.py`: Trích xuất dữ liệu Batdongsan
    - `extract_chotot.py`: Trích xuất dữ liệu Chotot
    - Schema validation và basic filtering

2. **Transformation** (`jobs/transformation/`):

    - `transform_batdongsan.py`: Chuẩn hóa dữ liệu Batdongsan
    - `transform_chotot.py`: Chuẩn hóa dữ liệu Chotot
    - `unify_dataset.py`: Hợp nhất dữ liệu từ nhiều nguồn

3. **Data Quality** (`jobs/data_quality/`):

    - `data_validation.py`: Kiểm tra chất lượng dữ liệu
    - `data_profiling.py`: Tạo profile dữ liệu

4. **Enrichment** (`jobs/enrichment/`):

    - `geo_enrichment.py`: Làm giàu thông tin địa lý
    - `text_feature_extraction.py`: Trích xuất features từ text

5. **Load** (`jobs/load/`):
    - `load_to_delta.py`: Lưu vào Delta Lake
    - `load_to_feature_store.py`: Lưu features cho ML

### 4. Machine Learning Pipeline

#### 4.1 ML Training và Prediction

```
┌─────────────────────────────────────────────────────────────────┐
│                    MACHINE LEARNING PIPELINE                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────┐  │
│  │   DATA      │  │  FEATURE    │  │   MODEL     │  │PREDICT │  │
│  │ PREPARATION │  │ ENGINEERING │  │  TRAINING   │  │DEPLOY  │  │
│  │             │  │             │  │             │  │        │  │
│  │ • Clean     │─▶│ • Extract   │─▶│ • Random    │─▶│ REST   │  │
│  │ • Split     │  │ • Scale     │  │   Forest    │  │ API    │  │
│  │ • Validate  │  │ • Encode    │  │ • GBT       │  │        │  │
│  │             │  │ • Select    │  │ • Linear    │  │        │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**ML Components:**

1. **PricePredictionModel** (`data_processing/ml/price_prediction_model.py`):

    - Random Forest, Gradient Boosted Trees, Linear Regression
    - Feature engineering tự động
    - Cross-validation và hyperparameter tuning

2. **Features**:

    - Categorical: `city`, `district`, `property_type`
    - Numerical: `area_cleaned`, `bedrooms_cleaned`, `bathrooms_cleaned`
    - Target: `price_cleaned`

3. **Model Evaluation**:
    - RMSE (Root Mean Square Error)
    - MAE (Mean Absolute Error)
    - R² (Coefficient of Determination)

### 5. Orchestration với Apache Airflow

#### 5.1 DAG Workflows

```
┌─────────────────────────────────────────────────────────────────┐
│                        AIRFLOW DAGS                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              FULL PIPELINE DAG                           │  │
│  │                                                          │  │
│  │  Check Services ──▶ Crawlers ──▶ Storage ──▶ Verify     │  │
│  │       │                │           │          │         │  │
│  │       ▼                ▼           ▼          ▼         │  │
│  │   [Kafka,HDFS]    [API,List,    [HDFS      [File       │  │
│  │                   Detail]        Writer]    Count]      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │            DATA PROCESSING DAG                           │  │
│  │                                                          │  │
│  │  Extract ──▶ Transform ──▶ Unify ──▶ Validate ──▶ Load  │  │
│  │     │           │           │          │          │     │  │
│  │     ▼           ▼           ▼          ▼          ▼     │  │
│  │  [Raw→Bronze] [Clean]   [Merge]   [Quality]   [Delta]  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Các DAG Chính:**

1. **realestate_pipeline_dag**: Luồng chính crawler → storage
2. **realestate_data_processing_dag**: ETL pipeline với Spark
3. **chotot_api_crawler_dag**: Crawler riêng cho Chotot API
4. **batdongsan_playwright_crawler_dag**: Crawler riêng cho Batdongsan
5. **storage_service_dag**: Service lưu trữ từ Kafka → HDFS

## Hướng Dẫn Triển Khai

### 1. Yêu Cầu Hệ Thống

```bash
# Phần cứng tối thiểu
CPU: 8 cores
RAM: 16GB
Storage: 100GB SSD
Network: 100Mbps

# Phần mềm
Docker: 20.10+
Docker Compose: 2.0+
Git: 2.30+
```

### 2. Cài Đặt và Khởi Động

```bash
# 1. Clone repository
git clone https://github.com/your-username/real_estate_project.git
cd real_estate_project

# 2. Khởi tạo volumes và networks
./scripts/init_volumes.sh

# 3. Khởi động toàn bộ hệ thống
./scripts/start_all.sh

# 4. Kiểm tra trạng thái services
docker ps
```

### 3. Truy Cập Các Giao Diện

```bash
# Web Interfaces
Airflow UI:     http://localhost:8080    (admin/admin)
HDFS NameNode:  http://localhost:9870
Spark Master:   http://localhost:8181
Jupyter:        http://localhost:8888
React Frontend: http://localhost:3000

# Service Endpoints
Kafka:          localhost:9092
HDFS:           hdfs://localhost:9000
Postgres:       localhost:5432
```

## Sử Dụng Hệ Thống

### 1. Chạy Crawler Thủ Công

```bash
# Chạy crawler Chotot API
docker run --rm --network hdfs_network \
  -e SOURCE=chotot \
  -e START_PAGE=1 \
  -e END_PAGE=10 \
  -e OUTPUT_TOPIC=property-data \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:19092 \
  realestate-crawler:latest python -m services.api_crawler.main

# Chạy crawler Batdongsan List
docker run --rm --network hdfs_network \
  -e SOURCE=batdongsan \
  -e OUTPUT_TOPIC=property-urls \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:19092 \
  realestate-crawler:latest python -m services.list_crawler.main

# Chạy Storage Service
docker run --rm --network hdfs_network \
  -e KAFKA_TOPIC=property-data \
  -e STORAGE_TYPE=hdfs \
  -e HDFS_NAMENODE=namenode:9870 \
  realestate-crawler:latest python -m services.storage_service.main
```

### 2. Chạy Spark Jobs

```bash
# Trích xuất dữ liệu Chotot
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/extraction/extract_chotot.py \
  --date 2025-05-25 \
  --property-type house

# Chuyển đổi dữ liệu
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/transformation/transform_chotot.py \
  --date 2025-05-25 \
  --property-type house

# Hợp nhất dữ liệu
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/transformation/unify_dataset.py \
  --date 2025-05-25 \
  --property-type all
```

### 3. Huấn Luyện Mô Hình ML

```bash
# Huấn luyện mô hình dự đoán giá
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/ml/train_model.py \
  --train-data /data/realestate/processed/integrated/house/2025/05/25 \
  --model-type random_forest \
  --output /data/realestate/ml/models/house/price_model_v1
```

### 4. Trigger Airflow DAGs

```bash
# Kích hoạt pipeline crawler
curl -X POST "http://localhost:8080/api/v1/dags/realestate_pipeline_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{}'

# Kích hoạt data processing
curl -X POST "http://localhost:8080/api/v1/dags/realestate_data_processing_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{}'
```

## Giám Sát và Troubleshooting

### 1. Kiểm Tra Logs

```bash
# Logs của các services chính
docker logs airflow-web
docker logs spark-master
docker logs kafka1
docker logs namenode

# Logs của crawler jobs
docker logs $(docker ps -q --filter name=crawler)

# Logs Spark jobs
docker exec spark-master cat /opt/bitnami/spark/logs/spark-master.out
```

### 2. Kiểm Tra Dữ Liệu

```bash
# Kiểm tra dữ liệu trong HDFS
docker exec namenode hdfs dfs -ls -R /data/realestate

# Đếm số lượng files
docker exec namenode hdfs dfs -find /data/realestate -name "*.json" | wc -l
docker exec namenode hdfs dfs -find /data/realestate -name "*.parquet" | wc -l

# Kiểm tra Kafka topics
docker exec kafka1 kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic property-data \
  --from-beginning --max-messages 10
```

### 3. Performance Monitoring

```bash
# Monitor resource usage
docker stats

# Check Spark job status
curl http://localhost:8181/api/v1/applications

# Monitor HDFS health
curl http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem
```

## Schema Dữ Liệu

### 1. Raw Data Schemas

#### 1.1 Batdongsan Raw Schema (22 fields)

Dữ liệu thô được thu thập từ website Batdongsan.com.vn

```python
# All fields are StringType() từ web scraping
StructType([
    StructField("area", StringType(), True),              # "120m²", "150 m2"
    StructField("bathroom", StringType(), True),          # "2", "3 phòng"
    StructField("bedroom", StringType(), True),           # "3", "4 phòng ngủ"
    StructField("crawl_timestamp", StringType(), True),   # Unix timestamp string
    StructField("data_type", StringType(), True),         # "house", "apartment"
    StructField("description", StringType(), True),       # HTML description
    StructField("facade_width", StringType(), True),      # "5m", "6.5m"
    StructField("floor_count", StringType(), True),       # "2", "3 tầng"
    StructField("house_direction", StringType(), True),   # "Đông", "Tây Nam"
    StructField("interior", StringType(), True),          # "Đầy đủ", "Cơ bản"
    StructField("latitude", StringType(), True),          # "10.762622"
    StructField("legal_status", StringType(), True),      # "Sổ đỏ", "Sổ hồng"
    StructField("location", StringType(), True),          # "Quận 1, TP.HCM"
    StructField("longitude", StringType(), True),         # "106.660172"
    StructField("posted_date", StringType(), True),       # "2025-01-15"
    StructField("price", StringType(), True),             # "5.2 tỷ", "850 triệu"
    StructField("price_per_m2", StringType(), True),      # "43.3 triệu/m²"
    StructField("road_width", StringType(), True),        # "4m", "8m"
    StructField("seller_info", StringType(), True),       # JSON contact info
    StructField("source", StringType(), True),            # "batdongsan"
    StructField("title", StringType(), True),             # Property title
    StructField("url", StringType(), True),               # Original URL
])
```

#### 1.2 Chotot Raw Schema (24 fields)

Dữ liệu thô từ API Chotot.com

```python
# All fields are StringType() từ API response
StructType([
    StructField("area", StringType(), True),              # "120", "150.5"
    StructField("bathroom", StringType(), True),          # "2", "3"
    StructField("bedroom", StringType(), True),           # "3", "4"
    StructField("crawl_timestamp", StringType(), True),   # Unix timestamp
    StructField("data_type", StringType(), True),         # Property category
    StructField("description", StringType(), True),       # Text description
    StructField("floor_count", StringType(), True),       # "2", "3"
    StructField("house_direction", StringType(), True),   # Direction code
    StructField("house_type", StringType(), True),        # Type code
    StructField("interior", StringType(), True),          # Interior code
    StructField("latitude", StringType(), True),          # "10.762622"
    StructField("legal_status", StringType(), True),      # Legal code
    StructField("length", StringType(), True),            # "15", "20.5"
    StructField("living_size", StringType(), True),       # Usable area
    StructField("location", StringType(), True),          # Location string
    StructField("longitude", StringType(), True),         # "106.660172"
    StructField("posted_date", StringType(), True),       # Unix timestamp
    StructField("price", StringType(), True),             # "5200000000"
    StructField("price_per_m2", StringType(), True),      # "43300000"
    StructField("seller_info", StringType(), True),       # Seller data
    StructField("source", StringType(), True),            # "chotot"
    StructField("title", StringType(), True),             # Property title
    StructField("url", StringType(), True),               # API endpoint
    StructField("width", StringType(), True),             # "8", "10.5"
])
```

### 2. Processed Data Schemas

#### 2.1 Batdongsan Processed Schema

Sau khi xử lý và chuyển đổi kiểu dữ liệu

```python
StructType([
    StructField("area", DoubleType(), True),              # Converted to m²
    StructField("bathroom", DoubleType(), True),          # Numeric count
    StructField("bedroom", DoubleType(), True),           # Numeric count
    StructField("crawl_timestamp", TimestampType(), True), # Proper timestamp
    StructField("data_type", StringType(), True),         # Cleaned category
    StructField("description", StringType(), True),       # Cleaned text
    StructField("facade_width", DoubleType(), True),      # Meters
    StructField("floor_count", DoubleType(), True),       # Floor count
    StructField("house_direction", StringType(), True),   # Standardized
    StructField("interior", StringType(), True),          # Standardized
    StructField("latitude", DoubleType(), True),          # Decimal degrees
    StructField("legal_status", StringType(), True),      # Standardized
    StructField("location", StringType(), True),          # Cleaned
    StructField("longitude", DoubleType(), True),         # Decimal degrees
    StructField("posted_date", TimestampType(), True),    # Proper timestamp
    StructField("price", DoubleType(), True),             # VND amount
    StructField("price_per_m2", DoubleType(), True),      # VND per m²
    StructField("road_width", DoubleType(), True),        # Meters
    StructField("seller_info", StringType(), True),       # JSON string
    StructField("source", StringType(), True),            # "batdongsan"
    StructField("title", StringType(), True),             # Cleaned title
    StructField("url", StringType(), True),               # Original URL
    StructField("processing_date", TimestampType(), True), # Processing time
    StructField("processing_id", StringType(), True),     # Batch ID
])
```

#### 2.2 Chotot Processed Schema

Sau khi xử lý từ dữ liệu API

```python
StructType([
    StructField("area", DoubleType(), True),              # m² numeric
    StructField("bathroom", DoubleType(), True),          # Count
    StructField("bedroom", DoubleType(), True),           # Count
    StructField("crawl_timestamp", TimestampType(), True), # Converted timestamp
    StructField("data_type", StringType(), True),         # Category
    StructField("description", StringType(), True),       # Cleaned text
    StructField("floor_count", DoubleType(), True),       # Floor count
    StructField("house_direction", StringType(), True),   # Direction
    StructField("house_type", StringType(), True),        # Property type
    StructField("interior", StringType(), True),          # Interior status
    StructField("latitude", DoubleType(), True),          # Coordinates
    StructField("legal_status", StringType(), True),      # Legal docs
    StructField("length", DoubleType(), True),            # Lot length (m)
    StructField("living_size", DoubleType(), True),       # Usable area
    StructField("location", StringType(), True),          # Location
    StructField("longitude", DoubleType(), True),         # Coordinates
    StructField("posted_date", TimestampType(), True),    # Post date
    StructField("price", DoubleType(), True),             # VND amount
    StructField("price_per_m2", DoubleType(), True),      # VND per m²
    # seller_info removed during processing
    StructField("source", StringType(), True),            # "chotot"
    StructField("title", StringType(), True),             # Property title
    StructField("url", StringType(), True),               # API URL
    StructField("width", DoubleType(), True),             # Lot width (m)
    StructField("processing_date", TimestampType(), True), # Processing time
    StructField("processing_id", StringType(), True),     # Batch ID
])
```

### 3. Unified Schema

#### 3.1 Common Property Schema

Schema thống nhất cho cả hai nguồn dữ liệu

```python
StructType([
    # Identifiers
    StructField("id", StringType(), False),               # Unique ID
    StructField("url", StringType(), True),               # Source URL
    StructField("data_source", StringType(), True),       # "batdongsan" | "chotot"

    # Basic Information
    StructField("title", StringType(), True),             # Property title
    StructField("description", StringType(), True),       # Description
    StructField("location", StringType(), True),          # Full location

    # Location Details (extracted)
    StructField("province", StringType(), True),          # Province/City
    StructField("district", StringType(), True),          # District
    StructField("ward", StringType(), True),              # Ward/Commune

    # Coordinates
    StructField("latitude", DoubleType(), True),          # GPS latitude
    StructField("longitude", DoubleType(), True),         # GPS longitude

    # Core Metrics
    StructField("price", DoubleType(), True),             # VND price
    StructField("area", DoubleType(), True),              # Total area (m²)
    StructField("price_per_m2", DoubleType(), True),      # VND per m²

    # Property Details
    StructField("bedroom", DoubleType(), True),           # Bedroom count
    StructField("bathroom", DoubleType(), True),          # Bathroom count
    StructField("floor_count", DoubleType(), True),       # Floor count

    # Property Features
    StructField("house_direction", StringType(), True),   # Direction
    StructField("legal_status", StringType(), True),      # Legal papers
    StructField("interior", StringType(), True),          # Interior condition
    StructField("house_type", StringType(), True),        # House type
    StructField("property_type", StringType(), True),     # Property category

    # Timestamps
    StructField("posted_date", TimestampType(), True),    # Original post date
    StructField("crawl_timestamp", TimestampType(), True), # Crawl time
    StructField("processing_date", TimestampType(), True), # Processing time
])
```

### 4. Data Processing Transformations (Chi Tiết Xử Lý Dữ Liệu)

The processing layer transforms raw data from Bronze to Silver layer through sophisticated data engineering pipelines implemented in PySpark.

#### 4.1 Batdongsan Data Transformation Pipeline

The `transform_batdongsan.py` implements a comprehensive 10-step transformation process:

**Step 1: Data Type Conversions**

-   Convert string fields to appropriate numeric types using regex cleaning
-   Fields processed: `area`, `bathroom`, `bedroom`, `floor_count`, `facade_width`, `road_width`, `latitude`, `longitude`
-   Pattern: `regexp_replace(col("field"), "[^0-9\\.]", "").cast("double")`

**Step 2: Price Processing with Vietnamese Currency Handling**

```python
# Intelligent price parsing for Vietnamese formats
price_processing:
  negotiable_detection: "thỏa thuận" | "thoathuan" → is_negotiable = True
  billion_conversion: "tỷ" | "ty" → multiply by 1,000,000,000
  million_conversion: "triệu" | "trieu" → multiply by 1,000,000
  price_per_m2_calculation: price / area (when missing)
```

**Step 3: Categorical Data Standardization**

-   **House Direction Mapping**: Vietnamese directions → English constants
    -   `"đông", "dong"` → `"EAST"`
    -   `"tây", "tay"` → `"WEST"`
    -   Compound directions: `"đôngnam"` → `"SOUTHEAST"`
-   **Interior Status Classification**: Text analysis → standardized categories
    -   Luxury keywords: `"cao cấp", "luxury", "5 sao"` → `"LUXURY"`
    -   Furnished: `"đầy đủ", "full", "nội thất"` → `"FULLY_FURNISHED"`
    -   Basic: `"cơ bản", "bình thường"` → `"BASIC"`
    -   Unfurnished: `"thô", "trống", "không"` → `"UNFURNISHED"`
-   **Legal Status Normalization**: Vietnamese legal terms → standard codes
    -   `"sổ đỏ", "sổ hồng"` → `"RED_BOOK"`
    -   `"thổ cư", "cnqsdđ"` → `"LAND_USE_CERTIFICATE"`

#### 4.2 Advanced Outlier Detection System

The pipeline implements a multi-layered outlier detection approach:

**Business Logic Outliers (Geographic and Domain Constraints)**

```python
business_rules:
  price_range: [50,000,000, 100,000,000,000]  # 50M - 100B VND
  area_range: [10, 2000]  # 10-2000 sqm
  price_per_m2_range: [500,000, 1,000,000,000]  # 500K-1B VND/sqm
  latitude_bounds: [8.0, 23.5]  # Vietnam geographic bounds
  longitude_bounds: [102.0, 110.0]  # Vietnam geographic bounds
  bedroom_range: [1, 15]
  bathroom_range: [1, 10]
```

**Statistical Outliers (IQR Method)**

-   Calculates Q1, Q3, and IQR for price and area
-   Outlier bounds: `[Q1 - 1.5*IQR, Q3 + 1.5*IQR]`
-   Only applied when sample size > 100 records

**Relationship-based Outliers**

-   Detects unrealistic price-to-area ratios
-   Flags properties with price/area < 1M VND/sqm or > 500M VND/sqm
-   Multi-dimensional outlier removal: `(price_outlier AND area_outlier) OR relationship_outlier`

#### 4.3 Chotot Data Transformation Pipeline

The `transform_chotot.py` implements similar but adapted processing:

**Price Processing Adaptations**

-   Handles different currency formats and negotiation indicators
-   Specific regex patterns for Chotot's pricing format
-   Exchange rate conversions where applicable

**Location Standardization**

-   Different geographic extraction patterns
-   City detection: Ho Chi Minh vs Hanoi vs Other
-   District and ward normalization specific to Chotot format

#### 4.4 Smart Imputation Engine

**Multi-Strategy Bedroom/Bathroom Imputation**

1. **Text Extraction Strategy**

```python
# Extract from Vietnamese property descriptions
bedroom_patterns: r"(\d+)\s*(phòng\s*ngủ|pn|bedroom)"
bathroom_patterns: r"(\d+)\s*(phòng\s*tắm|wc|toilet|bathroom)"
validation_range: [1, 10] bedrooms, [1, 5] bathrooms
```

2. **Area-Based Estimation**

```python
area_to_bedroom_mapping:
  "≤30 sqm": 1 bedroom
  "31-50 sqm": 2 bedrooms
  "51-80 sqm": 3 bedrooms
  "81-120 sqm": 4 bedrooms
  "121-200 sqm": 5 bedrooms
  ">200 sqm": 6 bedrooms

area_to_bathroom_mapping:
  "≤40 sqm": 1 bathroom
  "41-80 sqm": 2 bathrooms
  "81-150 sqm": 3 bathrooms
  ">150 sqm": 4 bathrooms
```

3. **Group-Based Median Imputation**

-   Groups by: `city_extracted`, `price_range`, `area_range`
-   Price ranges: under_1b, 1b_3b, 3b_5b, 5b_10b, over_10b
-   Area ranges: small, medium, large, very_large
-   Fallback to overall dataset median

4. **Cross-Validation Rules**

```python
validation_rules:
  bathroom_bedroom_ratio: bathroom ≤ bedroom + 1
  area_bedroom_constraint:
    - "area < 30 sqm AND bedroom > 2" → bedroom = 1
    - "area < 50 sqm AND bedroom > 3" → bedroom = 2
  range_enforcement: bedroom [1,10], bathroom [1,6]
```

#### 4.5 Data Quality Scoring System

**Quality Score Calculation (0-100 scale)**

```python
quality_components:
  valid_area: 20 points
  valid_price: 20 points
  valid_bedroom: 15 points
  valid_bathroom: 15 points
  valid_location: 15 points
  has_coordinates: 15 points

minimum_threshold: 60 points (for Silver layer inclusion)
high_quality: ≥80 points
premium_quality: ≥90 points
```

**Final Quality Filters**

-   Must have valid area AND (valid price OR price_per_m2)
-   Must have valid location (length > 5 characters)
-   Must achieve minimum quality score of 60/100
-   Results in ~15-25% data filtering depending on source quality

### 5. Chi Tiết Xử Lý Dữ Liệu (Data Transformation Details)

#### 4.1 Xử Lý Giá (Price Processing)

**4.1.1 Chuyển Đổi Text Sang Số**

```python
# Batdongsan: Text processing với regex
price_text = "5.2 tỷ"
when(lower(col("price_text")).contains("tỷ"),
     regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double") * 1000000000)
.when(lower(col("price_text")).contains("triệu"),
     regexp_replace(col("price_text"), "[^0-9\\.]", "").cast("double") * 1000000)

# Kết quả:
"5.2 tỷ" → 5,200,000,000 (VND)
"850 triệu" → 850,000,000 (VND)
"43.3 triệu/m²" → 43,300,000 (VND/m²)
```

**4.1.2 Chotot: Chuyển Đổi Đơn Vị**

```python
# Chotot API đã có số, chỉ cần nhân với đơn vị
price_per_m2 = regexp_replace(col("price_per_m2_text"), "[^0-9\\.]", "").cast("double") * 1000000
```

**4.1.3 Loại Bỏ Giá Trị Ngoại Lai**

```python
# Tính percentile cho từng cột
percentiles = df.select(
    percentile_approx("price", [0.01, 0.99], 10000).alias("price_percentiles")
).collect()[0]

# Lọc outliers
filtered_df = df.filter(
    (col("price") >= percentiles["price_percentiles"][0]) &
    (col("price") <= percentiles["price_percentiles"][1])
)

# Ngưỡng thực tế:
# Price: 1% = 100M VND, 99% = 50B VND
# Area: 1% = 20m², 99% = 500m²
```

#### 4.2 Xử Lý Diện Tích (Area Processing)

**4.2.1 Làm Sạch Text**

```python
# Loại bỏ ký tự không cần thiết
area_clean = regexp_replace(col("area"), "[^0-9\\.]", "").cast("double")

# Các trường hợp xử lý:
"120m²" → 120.0
"150 m2" → 150.0
"120.5 mét vuông" → 120.5
"không xác định" → NULL
"" → NULL
```

**4.2.2 Quy Tắc Validation**

```python
# Business rules cho diện tích
area_validation = (
    (col("area") > 0) &                    # Phải dương
    (col("area") < 10000) &                # Không quá 1 hecta
    (col("area") >= 20)                    # Tối thiểu 20m² (hợp lý cho nhà ở)
)

# Loại bỏ giá trị không hợp lý
valid_df = df.filter(area_validation)
```

#### 4.3 Xử Lý Giá Trị Thiếu (Missing Value Handling)

**4.3.1 Tính Giá Trị Trung Bình**

```python
# Tính toán để imputation
avg_values = {}
for col_name in ["bathroom", "bedroom", "floor_count"]:
    avg_val = df.filter(col(col_name).isNotNull()).agg(avg(col(col_name))).collect()[0][0]
    avg_values[col_name] = avg_val

# Kết quả thực tế:
# bathroom_avg = 2.1
# bedroom_avg = 3.2
# floor_count_avg = 2.8
```

**4.3.2 Strategies Điền Giá Trị Thiếu**

```python
# Strategy 1: Mean imputation cho numeric fields
for col_name in ["bathroom", "bedroom", "floor_count"]:
    df = df.withColumn(col_name,
        when(col(col_name).isNull(), avg_values[col_name]).otherwise(col(col_name)))

# Strategy 2: Calculated fields
df = df.withColumn("price_per_m2",
    when(col("price_per_m2").isNull() & col("area").isNotNull(),
         round(col("price") / col("area"), 2)).otherwise(col("price_per_m2")))

# Strategy 3: Drop columns với quá nhiều missing values
if "seller_info" in df.columns:
    df = df.drop("seller_info")  # > 80% missing
```

#### 4.4 Xử Lý Dữ Liệu Trùng Lặp (Duplicate Handling)

**4.4.1 Phát Hiện Trùng Lặp**

```python
# Đếm trước khi xử lý
count_before = df.count()

# Loại bỏ trùng lặp dựa trên URL (unique identifier)
df_deduplicated = df.dropDuplicates(["url"])

count_after = df_deduplicated.count()
duplicates_removed = count_before - count_after

# Kết quả thực tế: ~5-8% duplicate rate
```

**4.4.2 Advanced Duplicate Detection**

```python
# Phát hiện trùng lặp phức tạp hơn
window = Window.partitionBy("title", "location", "price").orderBy("crawl_timestamp")
df_with_rank = df.withColumn("rank", row_number().over(window))
df_latest = df_with_rank.filter(col("rank") == 1).drop("rank")
```

#### 4.5 Xử Lý Timestamp

**4.5.1 Unix Timestamp Conversion**

```python
# Chotot: Unix timestamp to proper timestamp
df = df.withColumn("crawl_timestamp",
    from_unixtime(col("crawl_timestamp")).cast("timestamp"))
df = df.withColumn("posted_date",
    from_unixtime(col("posted_date")).cast("timestamp"))
```

**4.5.2 String Date Parsing**

```python
# Batdongsan: String date parsing
df = df.withColumn("posted_date",
    to_timestamp(col("posted_date"), "yyyy-MM-dd"))
```

#### 4.6 Làm Sạch Text Fields

**4.6.1 Location Standardization**

```python
# Chuẩn hóa location
df = df.withColumn("location",
    trim(regexp_replace(col("location"), "\\s+", " ")))  # Remove extra spaces

# Extract địa chỉ thành các thành phần
df = df.withColumn("province",
    when(col("location").contains("TP.HCM"), "TP.HCM")
    .when(col("location").contains("Hà Nội"), "Hà Nội")
    .otherwise("Other"))
```

**4.6.2 Title và Description Cleaning**

```python
# Làm sạch title
df = df.withColumn("title",
    trim(regexp_replace(col("title"), "[\\n\\r\\t]", " ")))

# Làm sạch description (loại bỏ HTML tags cho Batdongsan)
df = df.withColumn("description",
    regexp_replace(col("description"), "<[^>]*>", ""))
```

#### 4.7 Categorical Data Processing

**4.7.1 Direction Standardization**

```python
# Chuẩn hóa hướng nhà
direction_mapping = {
    "1": "Đông", "2": "Tây", "3": "Nam", "4": "Bắc",
    "5": "Đông Nam", "6": "Đông Bắc", "7": "Tây Nam", "8": "Tây Bắc"
}

# Apply mapping
df = df.withColumn("house_direction_clean",
    coalesce(*[when(col("house_direction") == k, v) for k, v in direction_mapping.items()]))
```

**4.7.2 Legal Status Mapping**

```python
# Mapping legal document codes
legal_mapping = {
    "1": "Sổ đỏ", "2": "Sổ hồng", "3": "Giấy tờ khác",
    "4": "Đang chờ sổ", "5": "Không có sổ"
}
```

#### 4.8 Data Quality Validation

**4.8.1 Business Logic Validation**

```python
# Kiểm tra tính hợp lý của dữ liệu
quality_checks = (
    (col("area") > 0) &                           # Diện tích dương
    (col("price") > 0) &                          # Giá dương
    (col("price_per_m2") > 0) &                   # Giá/m² dương
    (col("bedroom") >= 0) &                       # Phòng ngủ không âm
    (col("bathroom") >= 0) &                      # Phòng tắm không âm
    (col("floor_count") >= 0) &                   # Số tầng không âm
    (col("latitude").between(8.0, 24.0)) &        # Latitude Việt Nam
    (col("longitude").between(102.0, 110.0)) &    # Longitude Việt Nam
    (col("price_per_m2") <= col("price") * 2)     # Relationship check
)

# Apply validation
validated_df = df.filter(quality_checks)
```

**4.8.2 Consistency Checks**

```python
# Kiểm tra tính nhất quán
consistency_check = abs(col("price_per_m2") - (col("price") / col("area"))) / col("price_per_m2") <= 0.05

# Highlight inconsistent records
inconsistent_df = df.filter(~consistency_check)
```

#### 4.9 Performance Optimization

**4.9.1 Data Partitioning**

```python
# Partition data for better performance
df_partitioned = df.repartition(col("source"), col("data_type"))

# Write with partitioning
df_partitioned.write.mode("overwrite").partitionBy("source", "processing_date").parquet(output_path)
```

**4.9.2 Caching Strategy**

```python
# Cache intermediate results
df_cleaned = df.filter(quality_checks).cache()
df_cleaned.count()  # Trigger caching

# Process different transformations
df_enriched = df_cleaned.withColumn("price_range",
    when(col("price") < 1000000000, "Dưới 1 tỷ")
    .when(col("price") < 3000000000, "1-3 tỷ")
    .otherwise("Trên 3 tỷ"))
```

#### 4.10 Thống Kê Quá Trình Xử Lý

**4.10.1 Processing Metrics**

```python
# Track processing statistics
processing_stats = {
    "total_input_records": bronze_df.count(),
    "after_deduplication": df_deduplicated.count(),
    "after_missing_value_handling": df_imputed.count(),
    "after_outlier_removal": df_filtered.count(),
    "final_valid_records": final_df.count(),
    "data_quality_score": final_df.count() / bronze_df.count()
}

# Typical results:
# total_input_records: 100,000
# after_deduplication: 92,000 (8% duplicates)
# after_outlier_removal: 87,000 (5% outliers)
# final_valid_records: 85,000 (85% retention rate)
```

**4.10.2 Column-wise Quality Metrics**

```python
# Missing value statistics
missing_stats = final_df.select([
    (count(when(col(c).isNull(), c)) / count(lit(1))).alias(f"{c}_missing_rate")
    for c in final_df.columns
]).collect()[0]

# Typical missing rates after processing:
# price: 0% (required field)
# area: 0% (required field)
# bedroom: 2% (after imputation)
# bathroom: 3% (after imputation)
# legal_status: 15% (acceptable for optional field)
```

### 5. Kafka Message Schema

#### 5.1 Real-time Data Stream

```json
{
    "messageId": "uuid-string",
    "timestamp": "2025-01-15T10:30:00Z",
    "source": "batdongsan|chotot",
    "eventType": "property_found|property_updated",
    "data": {
        "url": "string",
        "title": "string",
        "price": "string",
        "area": "string",
        "location": "string",
        "coordinates": {
            "lat": "string",
            "lng": "string"
        }
    },
    "metadata": {
        "crawlerId": "string",
        "retryCount": 0,
        "processingStatus": "pending|processing|completed|failed"
    }
}
```

### 6. Error and Log Schemas

#### 6.1 Crawler Error Schema

```python
# Error logging for crawlers
StructType([
    StructField("timestamp", TimestampType(), True),         # Error occurrence time
    StructField("service_name", StringType(), True),        # "list_crawler|detail_crawler|api_crawler"
    StructField("source", StringType(), True),              # "batdongsan|chotot"
    StructField("crawler_type", StringType(), True),        # "playwright|api"
    StructField("error_type", StringType(), True),          # "connection|parsing|validation|timeout"
    StructField("error_code", StringType(), True),          # HTTP status or custom error code
    StructField("error_message", StringType(), True),       # Detailed error message
    StructField("url", StringType(), True),                 # URL being processed when error occurred
    StructField("retry_count", DoubleType(), True),         # Number of retries attempted
    StructField("stack_trace", StringType(), True),         # Full stack trace for debugging
    StructField("request_id", StringType(), True),          # Unique request identifier
    StructField("session_id", StringType(), True),          # Crawler session ID
])
```

#### 6.2 Processing Error Schema

```python
# Spark job processing errors
StructType([
    StructField("job_id", StringType(), True),              # Spark job identifier
    StructField("timestamp", TimestampType(), True),        # Error timestamp
    StructField("job_type", StringType(), True),            # "transformation|validation|enrichment"
    StructField("stage_name", StringType(), True),          # Processing stage name
    StructField("error_category", StringType(), True),      # "data_quality|schema|computation"
    StructField("error_severity", StringType(), True),      # "critical|warning|info"
    StructField("affected_records", DoubleType(), True),    # Number of records affected
    StructField("total_records", DoubleType(), True),       # Total records in batch
    StructField("error_details", StringType(), True),       # JSON error details
    StructField("input_path", StringType(), True),          # Input data path
    StructField("output_path", StringType(), True),         # Expected output path
    StructField("processing_date", StringType(), True),     # Date being processed
])
```

#### 6.3 System Monitoring Schema

```python
# System performance and health metrics
StructType([
    StructField("metric_timestamp", TimestampType(), True), # Metric collection time
    StructField("service_name", StringType(), True),        # Service being monitored
    StructField("host_name", StringType(), True),           # Container/host name
    StructField("metric_type", StringType(), True),         # "performance|health|usage"
    StructField("cpu_usage", DoubleType(), True),           # CPU percentage
    StructField("memory_usage", DoubleType(), True),        # Memory usage in MB
    StructField("disk_usage", DoubleType(), True),          # Disk usage percentage
    StructField("network_io", DoubleType(), True),          # Network I/O in MB/s
    StructField("active_connections", DoubleType(), True),  # Number of active connections
    StructField("queue_size", DoubleType(), True),          # Kafka queue size
    StructField("processing_rate", DoubleType(), True),     # Records processed per second
    StructField("error_rate", DoubleType(), True),          # Error percentage
    StructField("response_time", DoubleType(), True),       # Average response time (ms)
])
```

### 7. Configuration Schemas

#### 7.1 Environment Variables Schema

```bash
# Core Infrastructure
KAFKA_BOOTSTRAP_SERVERS="kafka1:19092"
HDFS_NAMENODE="namenode:9870"
HDFS_USER="airflow"
POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"
POSTGRES_DB="airflow"

# Crawler Configuration
CRAWLER_TYPE="playwright|api"                      # Crawler implementation type
SOURCE="batdongsan|chotot"                         # Data source
MAX_CONCURRENT="10"                                # Concurrent requests limit
MAX_RETRIES="3"                                    # Maximum retry attempts
IDLE_TIMEOUT="60"                                  # Idle timeout in seconds
STOP_ON_EMPTY="true"                               # Stop when no data found
MAX_EMPTY_PAGES="5"                                # Max consecutive empty pages
FORCE_CRAWL_INTERVAL_HOURS="6"                     # Force crawl interval

# Data Processing
BATCH_SIZE="20000"                                 # Records per batch
FLUSH_INTERVAL="120"                               # Flush interval in seconds
MIN_FILE_SIZE_MB="5"                               # Minimum file size for HDFS
MAX_FILE_SIZE_MB="128"                             # Maximum file size for HDFS
FILE_FORMAT="json|parquet"                        # Output file format
STORAGE_TYPE="hdfs|local"                          # Storage destination

# Spark Configuration
SPARK_MASTER_URL="spark://spark-master:7077"
SPARK_DRIVER_MEMORY="2g"
SPARK_EXECUTOR_MEMORY="2g"
SPARK_EXECUTOR_CORES="2"
SPARK_TOTAL_EXECUTOR_CORES="4"

# Kafka Topics
OUTPUT_TOPIC="property-data"                       # Main data topic
ERROR_TOPIC="error-logs"                          # Error logging topic
MONITORING_TOPIC="system-metrics"                 # System monitoring topic
```

#### 7.2 Airflow DAG Configuration Schema

```python
# DAG configuration structure
{
    "dag_id": "string",                             # Unique DAG identifier
    "description": "string",                        # DAG description
    "schedule_interval": "string",                  # Cron expression or preset
    "start_date": "datetime",                       # DAG start date
    "catchup": "boolean",                           # Whether to catch up missed runs
    "max_active_runs": "integer",                   # Maximum concurrent runs
    "retries": "integer",                           # Default retry count
    "retry_delay": "timedelta",                     # Delay between retries
    "owner": "string",                              # DAG owner
    "tags": ["string"],                             # DAG tags for grouping
    "depends_on_past": "boolean",                   # Task dependency on past runs
    "email_on_failure": "boolean",                  # Email notifications
    "email_on_retry": "boolean",                    # Email on retry
    "email": ["string"],                            # Notification emails
    "sla": "timedelta",                             # Service level agreement
    "on_failure_callback": "function",              # Failure callback function
    "on_success_callback": "function",              # Success callback function
}
```

#### 7.3 Docker Service Configuration

```yaml
# Docker compose service schema
services:
    service_name:
        image: 'string' # Docker image name:tag
        container_name: 'string' # Container name
        restart: 'policy' # Restart policy
        ports:
            - 'host_port:container_port' # Port mappings
        environment:
            ENV_VAR: 'value' # Environment variables
        volumes:
            - 'host_path:container_path:mode' # Volume mounts
        networks:
            - 'network_name' # Network connections
        depends_on:
            - 'service_name' # Service dependencies
        healthcheck:
            test: ['CMD', 'command'] # Health check command
            interval: '30s' # Check interval
            timeout: '10s' # Check timeout
            retries: 3 # Max retries
            start_period: '60s' # Start period
        deploy:
            resources:
                limits:
                    memory: '2G' # Memory limit
                    cpus: '1.0' # CPU limit
                reservations:
                    memory: '1G' # Memory reservation
                    cpus: '0.5' # CPU reservation
```

### 8. API Response Schemas

#### 8.1 Chotot API Response Schema

```json
{
    "ad": {
        "list_id": "number", // Property ID
        "subject": "string", // Property title
        "price": "number", // Price in VND
        "area_name": "string", // Location name
        "body": "string", // Description
        "date": "number", // Posted date (Unix timestamp)
        "list_time": "number", // Listed time (Unix timestamp)
        "latitude": "number", // GPS latitude
        "longitude": "number", // GPS longitude
        "account_name": "string", // Seller name
        "account_oid": "number", // Seller ID
        "ward_name": "string", // Ward name
        "region_name": "string", // Region name
        "area_name": "string", // Area name
        "category_name": "string" // Category name
    },
    "attributes": {
        "price_per_m": "number", // Price per m² in VND
        "size": "number", // Total area in m²
        "rooms": "number", // Number of bedrooms
        "toilets": "number", // Number of bathrooms
        "floors": "number", // Number of floors
        "width": "number", // Width in meters
        "length": "number", // Length in meters
        "living_size": "number", // Living area in m²
        "direction": "number", // Direction code
        "property_legal_document": "number", // Legal document code
        "furnishing_sell": "number", // Interior condition code
        "property_type": "number", // Property type code
        "house_type": "number" // House type code
    }
}
```

#### 8.2 Batdongsan Web Scraping Schema

```json
{
    "url": "string", // Original property URL
    "title": "string", // Property title
    "price": "string", // Price text (e.g., "5.2 tỷ")
    "price_per_m2": "string", // Price per m² text
    "area": "string", // Area text (e.g., "120m²")
    "bedroom": "string", // Bedroom count text
    "bathroom": "string", // Bathroom count text
    "floor_count": "string", // Floor count text
    "facade_width": "string", // Facade width text
    "road_width": "string", // Road width text
    "house_direction": "string", // Direction text
    "legal_status": "string", // Legal status text
    "interior": "string", // Interior condition text
    "house_type": "string", // House type text
    "location": "string", // Full location string
    "description": "string", // Property description
    "posted_date": "string", // Posted date string
    "crawl_timestamp": "string", // Crawl timestamp
    "latitude": "string", // GPS latitude string
    "longitude": "string", // GPS longitude string
    "seller_info": "string", // Seller contact info JSON
    "data_type": "string", // "house" | "apartment"
    "source": "batdongsan" // Data source identifier
}
```

### 9. Data Lineage Schema

#### 9.1 Data Processing Lineage

```python
# Tracking data flow through the pipeline
StructType([
    StructField("lineage_id", StringType(), True),          # Unique lineage ID
    StructField("source_dataset", StringType(), True),      # Source dataset path
    StructField("target_dataset", StringType(), True),      # Target dataset path
    StructField("transformation_type", StringType(), True), # "extraction|transformation|loading"
    StructField("job_name", StringType(), True),            # Spark job name
    StructField("job_id", StringType(), True),              # Spark job ID
    StructField("start_time", TimestampType(), True),       # Job start time
    StructField("end_time", TimestampType(), True),         # Job completion time
    StructField("duration_seconds", DoubleType(), True),    # Processing duration
    StructField("input_records", DoubleType(), True),       # Input record count
    StructField("output_records", DoubleType(), True),      # Output record count
    StructField("filtered_records", DoubleType(), True),    # Filtered out records
    StructField("error_records", DoubleType(), True),       # Records with errors
    StructField("success_rate", DoubleType(), True),        # Success percentage
    StructField("data_quality_score", DoubleType(), True),  # Overall quality score
    StructField("schema_version", StringType(), True),      # Schema version used
    StructField("transformation_rules", StringType(), True), # Applied transformation rules
])
```
