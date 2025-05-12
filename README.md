# Hệ thống Crawler Bất Động Sản

Hệ thống thu thập, xử lý và phân tích dữ liệu bất động sản từ nhiều nguồn khác nhau, sử dụng kiến trúc microservices và các công nghệ hiện đại như Docker, Airflow, Kafka, HDFS và Spark.

## Kiến trúc hệ thống

Hệ thống bao gồm các thành phần chính sau:

### 1. Crawlers

Thu thập dữ liệu từ các nguồn khác nhau:

-   **BaseCrawler**: Interface cho crawler danh sách
-   **BaseDetailCrawler**: Interface cho crawler chi tiết
-   **BaseApiCrawler**: Interface cho crawler API
-   **BatdongsanCrawler**: Crawler danh sách Batdongsan
-   **BatdongsanDetailCrawler**: Crawler chi tiết Batdongsan
-   **ChototApiCrawler**: Crawler API cho Chotot

### 2. Services

Quản lý quy trình thu thập và xử lý dữ liệu:

-   **ListCrawlerService**: Thu thập danh sách URL
-   **DetailCrawlerService**: Thu thập chi tiết từ URL
-   **StorageService**: Lưu trữ dữ liệu
-   **RetryService**: Xử lý các URL thất bại
-   **ReportService**: Tạo báo cáo thu thập dữ liệu

### 3. Storage

Hệ thống lưu trữ dữ liệu:

-   **BaseStorage**: Interface cho hệ thống lưu trữ
-   **LocalStorage**: Lưu trữ trên filesystem
-   **HDFSStorage**: Lưu trữ trên HDFS

### 4. Data Processing

Xử lý và phân tích dữ liệu:

-   **Data Cleaning**: Làm sạch và chuẩn hóa dữ liệu
-   **Data Integration**: Tích hợp dữ liệu từ nhiều nguồn
-   **Analytics**: Phân tích dữ liệu và tạo báo cáo
-   **Machine Learning**: Huấn luyện mô hình dự đoán giá

### 5. Airflow DAGs

Điều phối và tự động hóa quy trình:

-   **realestate_crawler_dag**: Thu thập dữ liệu bất động sản
-   **realestate_data_processing_dag**: Xử lý dữ liệu bằng Spark
-   **realestate_ml_dag**: Huấn luyện mô hình ML
-   **realestate_monitoring_dag**: Giám sát chất lượng dữ liệu
-   **realestate_api_dag**: Cập nhật dữ liệu cho API
-   **realestate_full_pipeline_dag**: Điều phối toàn bộ quy trình

## Cài đặt và chạy

### Yêu cầu hệ thống

-   Docker và Docker Compose
-   Ít nhất 16GB RAM
-   50GB dung lượng ổ cứng

### Cài đặt

1. Clone repository:

```bash
git clone https://github.com/your-username/realestate-crawler.git
cd realestate-crawler
```

2. Khởi tạo các thư mục cần thiết:

```bash
./scripts/init_volumes.sh
```

3. Khởi động hệ thống:

```bash
./scripts/start_all.sh
```

### Sử dụng

#### Chạy crawler thủ công

```bash
# Chạy crawler Batdongsan
./scripts/run_crawler_jobs.sh batdongsan both

# Chạy crawler Chotot
./scripts/run_crawler_jobs.sh chotot both

# Chạy tất cả các crawler
./scripts/run_crawler_jobs.sh all both
```

#### Chạy Spark jobs thủ công

```bash
# Chạy job làm sạch dữ liệu
./scripts/run_spark_jobs.sh data_cleaning 2023-06-01

# Chạy job huấn luyện mô hình
./scripts/run_spark_jobs.sh ml 2023-06-01

# Chạy toàn bộ quy trình xử lý
./scripts/run_spark_jobs.sh all 2023-06-01
```

#### Truy cập các giao diện

-   **Airflow UI**: http://localhost:8080 (user/pass: admin/admin)
-   **HDFS UI**: http://localhost:9870
-   **Spark UI**: http://localhost:8181
-   **Jupyter Notebook**: http://localhost:8888

### Dừng hệ thống

```bash
./scripts/stop_all.sh
```

## Cấu trúc thư mục

```
real_estate_project/
├── crawler/
│   ├── common/
│   │   ├── base/
│   │   ├── factory/
│   │   ├── storage/
|   |   ├── queue/kafka_client.py
|   |   └── models/
│   ├── services/
│   │   ├── list_crawler/
│   │   ├── detail_crawler/
│   │   └── storage_service/
│   └── sources/
│       ├── batdongsan/
│       └── chotot/
├── data_processing/
│   ├── airflow/
│   │   └── dags/
│   ├── notebooks/
│   ├── spark/
│   │   └── jobs/
│   └── ml/
├── docker/
│   ├── yml/
│   │   ├── airflow.yml
│   │   ├── crawler.yml
│   │   ├── hdfs.yml
│   │   ├── kafka.yml
│   │   └── spark.yml
│   └── hadoop.env
└── scripts/
    ├── init_volumes.sh
    ├── start_all.sh
    ├── stop_all.sh
    ├── run_crawler_jobs.sh
    └── run_spark_jobs.sh
```

## Mô hình dữ liệu

Dữ liệu bất động sản được lưu trữ với các trường chính:

-   **url**: URL của bài đăng
-   **title**: Tiêu đề bài đăng
-   **price**: Giá bất động sản
-   **area**: Diện tích (m²)
-   **location**: Vị trí (tỉnh/thành phố, quận/huyện)
-   **description**: Mô tả chi tiết
-   **features**: Các tính năng của bất động sản
-   **contact**: Thông tin liên hệ
-   **post_time**: Thời gian đăng bài
-   **crawl_time**: Thời gian thu thập

## Quy trình xử lý dữ liệu

1. **Thu thập dữ liệu**:

    - Crawl danh sách URL từ các trang nguồn
    - Crawl chi tiết từ các URL đã thu thập
    - Lưu trữ dữ liệu thô vào HDFS

2. **Xử lý dữ liệu**:

    - Làm sạch và chuẩn hóa dữ liệu
    - Tích hợp dữ liệu từ nhiều nguồn
    - Phân tích và tạo các chỉ số thống kê

3. **Huấn luyện mô hình**:

    - Chuẩn bị dữ liệu huấn luyện
    - Huấn luyện mô hình dự đoán giá
    - Đánh giá và triển khai mô hình

4. **Giám sát và báo cáo**:
    - Giám sát chất lượng dữ liệu
    - Tạo báo cáo thu thập dữ liệu
    - Gửi thông báo qua Slack

## Đóng góp

Vui lòng đọc [CONTRIBUTING.md](CONTRIBUTING.md) để biết chi tiết về quy trình đóng góp.

## Giấy phép

Dự án này được cấp phép theo giấy phép MIT - xem tệp [LICENSE](LICENSE) để biết chi tiết.
