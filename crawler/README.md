# Tài liệu mô tả module `crawler` - Dự án Real Estate Project

Mô-đun crawler trong dự án này có nhiệm vụ thu thập dữ liệu bất động sản từ các nguồn trực tuyến, xử lý và lưu trữ chúng để phân tích sau này. Module được thiết kế theo kiến trúc module hóa, linh hoạt và có thể mở rộng.

## Cấu trúc thư mục

```
crawler/
├── __init__.py              # File khởi tạo package
├── Dockerfile               # Cấu hình Docker để build container cho crawler
├── requirements.txt         # Danh sách các thư viện Python cần thiết
├── common/                  # Chứa các module dùng chung cho toàn bộ hệ thống
│   ├── __init__.py
│   ├── base/                # Chứa các lớp cơ sở (abstract classes)
│   ├── factory/             # Chứa các factory để tạo instances
│   ├── models/              # Chứa các model dữ liệu
│   ├── queue/               # Quản lý giao tiếp qua message queue (Kafka)
│   ├── storage/             # Quản lý lưu trữ dữ liệu
│   └── utils/               # Công cụ tiện ích dùng chung
└── services/                # Các dịch vụ cụ thể của hệ thống crawler
    ├── api_crawler/         # Thu thập dữ liệu từ API
    ├── detail_crawler/      # Thu thập chi tiết từ các trang chi tiết
    ├── list_crawler/        # Thu thập danh sách các URL từ trang danh sách
    └── storage_service/     # Dịch vụ lưu trữ dữ liệu vào hệ thống
└── sources/                 # Mã nguồn cụ thể cho từng nguồn dữ liệu
    ├── batdongsan/          # Mã nguồn cho website batdongsan.com.vn
    └── chotot/              # Mã nguồn cho website chotot.com
```

## Các thành phần chính

### 1. Dockerfile

File này chứa cấu hình để tạo container Docker cho crawler, bao gồm:

-   Sử dụng Python 3.11 làm base image
-   Cài đặt các dependency hệ thống cần thiết
-   Cài đặt các thư viện Python từ requirements.txt
-   Cài đặt Playwright và các trình duyệt cần thiết
-   Thiết lập môi trường để chạy crawler

### 2. requirements.txt

Chứa danh sách các thư viện Python cần thiết cho module crawler:

-   pandas, numpy: Thư viện xử lý dữ liệu
-   pyarrow, hdfs, pyhdfs: Thư viện làm việc với HDFS
-   kafka-python, confluent-kafka: Thư viện làm việc với Kafka
-   playwright, beautifulsoup4, requests, aiohttp: Thư viện để crawl web
-   dotenv: Thư viện để quản lý biến môi trường

### 3. common/base/

Chứa các lớp cơ sở (abstract classes) định nghĩa giao diện chung cho toàn bộ hệ thống:

#### base_service.py

-   Lớp cơ sở `BaseService`: Cấu trúc chung cho tất cả các service
-   Quản lý vòng đời của service: khởi tạo, dừng, báo cáo thống kê
-   Xử lý tín hiệu và theo dõi hiệu suất

#### base_list_crawler.py

-   Định nghĩa giao diện cho crawler lấy danh sách URL từ trang danh sách
-   Hỗ trợ cấu hình crawl: max concurrent, force crawl
-   Định nghĩa các phương thức trừu tượng cho quá trình crawl

#### base_detail_crawler.py

-   Định nghĩa giao diện cho crawler lấy chi tiết từng bài đăng
-   Quản lý crawl theo batch và xử lý kết quả
-   Định nghĩa các phương thức trừu tượng cho việc crawl chi tiết

#### base_api_crawler.py

-   Định nghĩa giao diện cho crawler lấy dữ liệu từ API
-   Xử lý các vấn đề chung khi gọi API: retry, delay, header
-   Định nghĩa các phương thức trừu tượng cho việc gọi API

#### base_storage.py

-   Định nghĩa giao diện cho việc lưu trữ dữ liệu
-   Hỗ trợ nhiều định dạng dữ liệu: parquet, csv, json
-   Định nghĩa các phương thức trừu tượng cho việc lưu và đọc dữ liệu

### 4. common/factory/

#### crawler_factory.py

-   Factory pattern để tạo các instance crawler phù hợp
-   Quản lý đăng ký các class crawler cụ thể cho từng nguồn dữ liệu
-   Hỗ trợ tạo list_crawler, detail_crawler, api_crawler

### 5. common/models/

#### house_detail.py

-   Định nghĩa cấu trúc dữ liệu cho thông tin bất động sản
-   Sử dụng dataclass để tạo model với các trường dữ liệu chuẩn
-   Bao gồm các thuộc tính cơ bản: title, description, price, area, etc.

### 6. common/queue/

Chứa các module để giao tiếp với hàng đợi tin nhắn (Kafka), cho phép các service giao tiếp với nhau.

### 7. common/storage/

#### hdfs_writer.py

-   Cung cấp lớp `HDFSWriter` để ghi dữ liệu vào HDFS
-   Hỗ trợ các định dạng file khác nhau: parquet, json, csv
-   Quản lý kết nối tới HDFS và xử lý lỗi

### 8. services/list_crawler/

#### main.py

-   Dịch vụ crawl danh sách URL từ trang danh sách
-   Quản lý crawl theo trang (từ start_page đến end_page)
-   Đẩy URL vào Kafka để detail_crawler xử lý sau
-   Hỗ trợ checkpoint để có thể tiếp tục crawl sau khi dừng

### 9. services/detail_crawler/

#### main.py

-   Dịch vụ crawl chi tiết từ các URL
-   Đọc URL từ Kafka, crawl chi tiết và đẩy dữ liệu vào Kafka
-   Xử lý crawl bất đồng bộ với asyncio
-   Hỗ trợ retry và xử lý lỗi

### 10. services/api_crawler/

#### main.py

-   Dịch vụ crawl dữ liệu từ API
-   Hỗ trợ phân trang và giới hạn tốc độ crawl
-   Đẩy dữ liệu đã crawl vào Kafka

### 11. services/storage_service/

#### main.py

-   Dịch vụ lưu trữ dữ liệu từ Kafka vào hệ thống lưu trữ
-   Hỗ trợ nhiều loại lưu trữ: local, hdfs
-   Xử lý buffer và batch để tối ưu hiệu suất

### 12. sources/

Chứa mã nguồn cụ thể cho từng nguồn dữ liệu:

#### batdongsan/

-   Mã nguồn cho việc crawl dữ liệu từ website batdongsan.com.vn
-   Triển khai cụ thể cho list_crawler và detail_crawler

#### chotot/

-   Mã nguồn cho việc crawl dữ liệu từ website chotot.com
-   Triển khai cụ thể cho detail_crawler và api_crawler

## Luồng dữ liệu

1. **List Crawler** thu thập danh sách URL từ trang danh sách và đẩy vào Kafka (topic: property-urls)
2. **Detail Crawler** đọc URL từ Kafka, crawl chi tiết và đẩy dữ liệu vào Kafka (topic: property-data)
3. **API Crawler** thu thập dữ liệu trực tiếp từ API và đẩy vào Kafka (topic: property-data)
4. **Storage Service** đọc dữ liệu từ Kafka và lưu trữ vào hệ thống (local hoặc HDFS)

## Cách sử dụng

Tất cả các service đều có thể được cấu hình thông qua biến môi trường (.env) hoặc truyền trực tiếp khi khởi tạo service. Các service có thể được chạy độc lập hoặc cùng nhau thông qua Docker.

### Biến môi trường chính

-   **SOURCE**: Nguồn dữ liệu (batdongsan, chotot)
-   **CRAWLER_TYPE**: Loại crawler (playwright, default)
-   **MAX_CONCURRENT**: Số lượng crawl đồng thời tối đa
-   **BATCH_SIZE**: Kích thước batch khi xử lý dữ liệu
-   **START_PAGE, END_PAGE**: Phạm vi trang cần crawl
-   **FORCE_CRAWL**: Có crawl lại dữ liệu đã crawl hay không
-   **HDFS_NAMENODE, HDFS_USER**: Cấu hình HDFS
-   **KAFKA_TOPIC**: Topic Kafka để đọc/ghi dữ liệu

## Mở rộng

Hệ thống được thiết kế để dễ dàng mở rộng:

1. Thêm nguồn dữ liệu mới: Tạo package mới trong thư mục `sources/` và triển khai các lớp crawler cụ thể
2. Thêm loại crawler mới: Mở rộng các factory trong `common/factory/` và đăng ký crawler mới
3. Thêm phương thức lưu trữ mới: Triển khai `BaseStorage` và cập nhật `StorageFactory`
