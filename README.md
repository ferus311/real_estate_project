- giá trị hostname trong datanode của opt/hadoop/etc/hdfs-site.xml chưa set hostname = localhost

- lưu trữ setup data cẩn thận tránh lỗi namenode ở safe mode ( xóa data đi setup kỹ vào)

```
┌───────────────────────────┐                    ┌─────────────────────────────────────┐
│                           │                    │           HDFS STORAGE               │
│                           │                    │   ┌─────────┐   ┌─────────────┐     │
│      AIRFLOW              │                    │   │         │   │  DataNode1  │     │
│   ORCHESTRATION           │                    │   │NameNode │───┼─────────────┤     │
│   ┌──────────────┐        │                    │   │         │   │  DataNode2  │     │
│   │              │        │     Controls       │   └─────────┘   ├─────────────┤     │
│   │ Scheduler    ├────────┼─────────────┐      │                 │  DataNode3  │     │
│   │              │        │             │      │                 └─────────────┘     │
│   └──────┬───────┘        │             │      └────────────┬────────────────────────┘
│          │                │             │                   │ Stored
│   ┌──────▼───────┐        │             │                   │ Raw Data
│   │              │        │             ▼                   │
│   │ WebServer    │        │      ┌─────────────────────────▼──┐
│   │              │        │      │                            │
│   └──────────────┘        │      │                            │
│                           │      │                            │
└───────────────────────────┘      │     CRAWLER SYSTEM         │
                                   │                            │
┌───────────────────────────┐      │  ┌────────────────────┐    │
│                           │      │  │                    │    │
│      MESSAGING SYSTEM     │      │  │  List Layer        │    │
│                           │      │  │  (crawl_list.py)   │    │
│   ┌──────────┐            │      │  │                    │    │
│   │          │            │      │  └──────┬─────────────┘    │
│   │ZooKeeper │            │      │         │                  │
│   │          │            │      │         │ URLs             │
│   └──────┬───┘            │      │         ▼                  │
│          │                │      │  ┌────────────────────┐    │
│   ┌──────▼───┐   Messages │      │  │                    │    │
│   │          ◄────────────┼──────┼──┤  Detail Layer      │    │
│   │ Kafka    │            │      │  │  (crawl_detail.py) │    │
│   │          │            │      │  │                    │    │
│   └──────────┘            │      │  └────────────────────┘    │
│                           │      │                            │
└───────────────────────────┘      └────────────────────────────┘
          │                                     ▲
          │                                     │
          │                                     │ Raw Data
          │ Processed                           │
          │ Messages                            │
          ▼                                     │
┌─────────────────────────┐            ┌────────┴────────────┐
│                         │            │                     │
│    SPARK PROCESSING     │            │                     │
│  ┌──────────────┐       │    Data    │   WEB APPLICATION   │
│  │              │       │  Processing│                     │
│  │ Spark Master │       │  Results   │   ┌─────────────┐   │
│  │              │       ├────────────►   │             │   │
│  └──┬────────┬──┘       │            │   │ Frontend    │   │
│     │        │          │            │   │             │   │
│  ┌──▼──┐  ┌──▼──┐       │            │   └─────────────┘   │
│  │     │  │     │       │            │                     │
│  │ W1  │  │ W2  │       │            └─────────────────────┘
│  │     │  │     │       │
│  └─────┘  └─────┘       │
│                         │
└─────────────────────────┘
```


```
real_estate_project/
├── docker/                          # Cấu hình Docker
│   ├── docker-compose.yml           # File tổng hợp các service
│   ├── .env                         # Biến môi trường dùng chung
│   └── volumes/                     # Dữ liệu persistent của các container
├── crawler/                         # Module thu thập dữ liệu
│   ├── common/                      # Mã nguồn dùng chung cho các crawler
│   │   ├── models/                  # Định nghĩa schema dữ liệu
│   │   ├── storage/                 # Xử lý lưu trữ (HDFS, local,...)
│   │   ├── queue/                   # Xử lý message queue (Kafka)
│   │   └── utils/                   # Các tiện ích (logging, monitoring)
│   ├── sources/                     # Các nguồn thu thập dữ liệu
│   │   ├── batdongsan/              # Module crawler cho batdongsan.com
│   │   │   ├── playwright/          # Crawler sử dụng Playwright
│   │   │   ├── selenium/            # Crawler sử dụng Selenium
│   │   │   └── scrapy/              # Crawler sử dụng Scrapy
│   │   ├── chotot/                  # Module crawler cho chotot.com
│   │   └── api/                     # Thu thập dữ liệu từ API
│   ├── services/                    # Các service chạy độc lập
│   │   ├── list_crawler/            # Service thu thập danh sách URL
│   │   ├── detail_crawler/          # Service thu thập chi tiết
│   │   ├── storage_service/         # Service lưu trữ
│   │   └── retry_service/           # Service xử lý lỗi và thử lại
│   ├── Dockerfile                   # Build image cho crawler
│   ├── requirements.txt             # Thư viện Python cho crawler
│   └── entrypoint.sh                # Điểm vào cho container
├── data_processing/                 # Xử lý dữ liệu
│   ├── airflow/                     # Orchestration
│   │   ├── dags/                    # DAG files
│   │   └── plugins/                 # Custom plugins
│   ├── spark/                       # Xử lý dữ liệu với Spark
│   │   ├── jobs/                    # Spark jobs
│   │   ├── utils/                   # Tiện ích cho Spark
│   │   └── Dockerfile               # Build image cho Spark
│   ├── ml/                          # Machine Learning
│   │   ├── models/                  # Các mô hình ML
│   │   ├── training/                # Scripts huấn luyện
│   │   ├── prediction/              # Scripts dự đoán
│   │   ├── evaluation/              # Đánh giá mô hình
│   │   └── Dockerfile               # Build image cho ML
│   └── notebooks/                   # Jupyter notebooks
├── api_service/                     # API phục vụ frontend
│   ├── app/                         # Mã nguồn API (FastAPI hoặc Flask)
│   ├── Dockerfile                   # Build image cho API
│   └── requirements.txt             # Thư viện Python cho API
├── webapp/                          # Frontend web app
│   ├── client/                      # Mã nguồn frontend (React)
│   ├── Dockerfile                   # Build image cho webapp
│   └── nginx.conf                   # Cấu hình Nginx
├── scripts/                         # Scripts hỗ trợ
│   ├── setup.sh                     # Khởi tạo môi trường
│   ├── deploy.sh                    # Triển khai hệ thống
│   └── utils/                       # Scripts tiện ích khác
└── README.md                        # Tài liệu hướng dẫn
```
---

# Real Estate Data Pipeline System

## 4. System Workflow

### 4.1 Data Collection Flow

#### **List Crawler Service**

* Thu thập URL từ nhiều trang danh sách bất động sản.
* Đẩy các URL thu thập được vào Kafka topic: `property-urls`.
* Chạy theo lịch trình định kỳ (ví dụ: mỗi giờ một lần).

#### **Detail Crawler Service**

* Nhận URL từ Kafka topic `property-urls`.
* Crawl thông tin chi tiết từ từng URL.
* Đẩy dữ liệu chi tiết vào Kafka topic: `property-data`.

#### **Storage Service**

* Nhận dữ liệu chi tiết từ Kafka topic `property-data`.
* Tích lũy dữ liệu thành batch.
* Lưu batch dữ liệu vào HDFS theo thời gian định kỳ hoặc theo kích thước batch.

#### **Retry Service**

* Nhận các URL crawl lỗi từ Kafka topic `failed-urls`.
* Theo dõi số lần thử lại.
* Gửi lại URL vào `property-urls` nếu chưa vượt ngưỡng retry.

---

### 4.2 Data Processing Flow

#### **Airflow DAG**

* Lên lịch chạy các pipeline định kỳ.
* Điều phối các job: crawling, xử lý dữ liệu, huấn luyện mô hình.

#### **Spark Jobs**

* Đọc dữ liệu thô từ HDFS.
* Thực hiện làm sạch và tiền xử lý dữ liệu.
* Trích xuất đặc trưng.
* Lưu trữ dữ liệu đã xử lý trở lại HDFS.

#### **ML Jobs**

* Huấn luyện mô hình từ dữ liệu đã xử lý.
* Đánh giá hiệu suất mô hình bằng các chỉ số như RMSE, MAPE.
* Lưu mô hình đã huấn luyện để triển khai cho API.

#### **API Service**

* Cung cấp API REST để truy vấn dữ liệu.
* Thực hiện dự đoán dựa trên mô hình ML.
* Phục vụ dữ liệu cho frontend web app.

#### **Frontend Web App**

* Hiển thị dữ liệu bất động sản.
* Cung cấp giao diện tìm kiếm, lọc dữ liệu.
* Hiển thị kết quả dự đoán và phân tích trực quan.

---

## 5. Architecture Benefits

### ✅ **Modular & Extensible**

* Dễ dàng thêm nguồn dữ liệu mới.
* Linh hoạt thay thế công cụ crawl (Scrapy, Selenium, Playwright).
* Có thể scale horizontally bằng cách thêm crawler/worker.

### ✅ **High Fault Tolerance**

* Cơ chế retry tự động cho URL lỗi.
* Các service được cô lập, giảm ảnh hưởng dây chuyền.
* Kafka đảm bảo dữ liệu không mất nếu một service bị down.

### ✅ **Horizontal Scalability**

* Có thể chạy nhiều crawler cùng lúc để tăng tốc thu thập.
* Spark có thể mở rộng bằng cách thêm nhiều worker xử lý song song.

### ✅ **Easy to Monitor and Manage**

* Hệ thống logging tập trung.
* Có thể tích hợp Prometheus, Grafana để giám sát hiệu năng.
* Dễ phát hiện lỗi và phân tích nguyên nhân.

### ✅ **Simple Deployment with Docker**

* Mỗi thành phần đóng gói riêng biệt trong container.
* Đảm bảo môi trường nhất quán giữa dev và production.
* Dễ scale lên hoặc xuống tùy theo tài nguyên.

---
