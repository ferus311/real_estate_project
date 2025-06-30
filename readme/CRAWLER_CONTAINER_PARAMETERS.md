# Bảng Biểu Tham Số Container Thu Thập Dữ Liệu

## 📝 Tổng quan hệ thống

Hệ thống crawler bao gồm 4 loại service chính:

1. **List Crawler** - Thu thập danh sách URL từ trang web
2. **Detail Crawler** - Thu thập chi tiết từ URL đã có
3. **API Crawler** - Thu thập dữ liệu qua API (chủ yếu cho Chotot)
4. **Storage Service** - Lưu trữ dữ liệu từ Kafka vào storage

### 🏗️ Kiến trúc hỗ trợ

**Nguồn dữ liệu hỗ trợ:**

-   `batdongsan` - batdongsan.com.vn (Playwright crawler)
-   `chotot` - chotot.com (API crawler + Detail crawler)

**Crawler types:**

-   `playwright` - Sử dụng Playwright browser automation
-   `default` - Crawler mặc định cho từng nguồn
-   `selenium` - Sử dụng Selenium (nếu được implement)

---

## 1. List Crawler Container (Thu thập danh sách URL)

### Tham số môi trường (Environment Variables)

| Tham số                      | Mô tả                                    | Kiểu dữ liệu | Giá trị mặc định | Ví dụ                    | Bắt buộc |
| ---------------------------- | ---------------------------------------- | ------------ | ---------------- | ------------------------ | -------- |
| `SOURCE`                     | Nguồn dữ liệu cần thu thập               | string       | "batdongsan"     | "batdongsan", "chotot"   | ✅       |
| `CRAWLER_TYPE`               | Loại crawler engine                      | string       | "playwright"     | "playwright", "selenium" | ❌       |
| `MAX_CONCURRENT`             | Số thread đồng thời                      | integer      | 5                | 3, 5, 10                 | ❌       |
| `START_PAGE`                 | Trang bắt đầu thu thập                   | integer      | 1                | 1, 10, 50                | ❌       |
| `END_PAGE`                   | Trang kết thúc thu thập                  | integer      | 500              | 100, 500, 1000           | ❌       |
| `OUTPUT_FILE`                | File xuất dữ liệu (nếu không dùng Kafka) | string       | null             | "/data/urls.json"        | ❌       |
| `CHECKPOINT_DIR`             | Thư mục lưu checkpoint                   | string       | "./checkpoint"   | "/data/checkpoint"       | ❌       |
| `FORCE_CRAWL`                | Bắt buộc crawl lại                       | boolean      | false            | "true", "false"          | ❌       |
| `FORCE_CRAWL_INTERVAL_HOURS` | Khoảng thời gian force crawl (giờ)       | float        | 24               | 12, 24, 48               | ❌       |

### Kafka Configuration

| Tham số                   | Mô tả          | Giá trị mặc định | Ví dụ                                          |
| ------------------------- | -------------- | ---------------- | ---------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka server   | "kafka1:19092"   | "kafka:9092", "kafka1:19092", "localhost:9092" |
| `KAFKA_TOPIC_URLS`        | Topic gửi URLs | "property-urls"  | "property-urls"                                |
| `KAFKA_CLIENT_ID`         | Client ID      | hostname         | "list-crawler-01"                              |

### Ví dụ Docker Run - List Crawler

#### Cấu hình cơ bản:

```bash
docker run -d \
  --name list-crawler-batdongsan \
  --network real_estate_network \
  -e SOURCE=batdongsan \
  -e CRAWLER_TYPE=playwright \
  -e MAX_CONCURRENT=5 \
  -e START_PAGE=1 \
  -e END_PAGE=100 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/list_crawler/main.py
```

#### Cấu hình nâng cao với checkpoint:

```bash
docker run -d \
  --name list-crawler-batdongsan-advanced \
  --network real_estate_network \
  -v /host/checkpoint:/app/checkpoint \
  -e SOURCE=batdongsan \
  -e CRAWLER_TYPE=playwright \
  -e MAX_CONCURRENT=10 \
  -e START_PAGE=1 \
  -e END_PAGE=1000 \
  -e FORCE_CRAWL=true \
  -e FORCE_CRAWL_INTERVAL_HOURS=12 \
  -e CHECKPOINT_DIR=/app/checkpoint \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/list_crawler/main.py
```

#### Cấu hình xuất file thay vì Kafka:

```bash
docker run -d \
  --name list-crawler-file-output \
  -v /host/data:/app/data \
  -e SOURCE=batdongsan \
  -e START_PAGE=1 \
  -e END_PAGE=50 \
  -e OUTPUT_FILE=/app/data/urls.json \
  crawler:latest python services/list_crawler/main.py
```

---

## 2. Detail Crawler Container (Thu thập chi tiết từ URL)

### Tham số môi trường (Environment Variables)

| Tham số            | Mô tả                                | Kiểu dữ liệu | Giá trị mặc định | Ví dụ                   | Bắt buộc |
| ------------------ | ------------------------------------ | ------------ | ---------------- | ----------------------- | -------- |
| `SOURCE`           | Nguồn dữ liệu                        | string       | "batdongsan"     | "batdongsan", "chotot"  | ✅       |
| `CRAWLER_TYPE`     | Loại crawler engine                  | string       | "default"        | "default", "playwright" | ❌       |
| `MAX_CONCURRENT`   | Số thread đồng thời                  | integer      | 5                | 3, 5, 10, 20            | ❌       |
| `BATCH_SIZE`       | Kích thước batch xử lý               | integer      | 100              | 50, 100, 200            | ❌       |
| `RETRY_LIMIT`      | Số lần retry khi lỗi                 | integer      | 3                | 2, 3, 5                 | ❌       |
| `RETRY_DELAY`      | Thời gian chờ giữa các retry (giây)  | integer      | 5                | 3, 5, 10                | ❌       |
| `OUTPUT_TOPIC`     | Kafka topic xuất dữ liệu             | string       | "property-data"  | "property-data"         | ❌       |
| `RUN_ONCE_MODE`    | Chế độ chạy một lần rồi dừng         | boolean      | false            | "true", "false"         | ❌       |
| `IDLE_TIMEOUT`     | Timeout khi không có dữ liệu (giây)  | integer      | 60               | 30, 60, 120             | ❌       |
| `COMMIT_THRESHOLD` | Số URL xử lý trước khi commit offset | integer      | 20               | 10, 20, 50              | ❌       |
| `COMMIT_INTERVAL`  | Thời gian giữa các lần commit (giây) | integer      | 60               | 30, 60, 120             | ❌       |

### Kafka Configuration

| Tham số                      | Mô tả                 | Giá trị mặc định       | Ví dụ                                          |
| ---------------------------- | --------------------- | ---------------------- | ---------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`    | Kafka server          | "kafka1:19092"         | "kafka:9092", "kafka1:19092", "localhost:9092" |
| `KAFKA_GROUP_ID`             | Consumer group ID     | "detail-crawler-group" | "detail-crawler-group"                         |
| `KAFKA_TOPIC_INPUT`          | Topic nhận URLs       | "property-urls"        | "property-urls"                                |
| `KAFKA_TOPIC_OUTPUT`         | Topic gửi dữ liệu     | "property-data"        | "property-data"                                |
| `KAFKA_CLIENT_ID`            | Client ID             | hostname               | "detail-crawler-01"                            |
| `KAFKA_AUTO_OFFSET_RESET`    | Offset reset strategy | "earliest"             | "earliest", "latest"                           |
| `KAFKA_ENABLE_AUTO_COMMIT`   | Auto commit offset    | false                  | "true", "false"                                |
| `KAFKA_MAX_POLL_INTERVAL_MS` | Max poll interval     | 600000                 | 300000, 600000                                 |

### Ví dụ Docker Run - Detail Crawler

#### Cấu hình cơ bản:

```bash
docker run -d \
  --name detail-crawler-batdongsan \
  --network real_estate_network \
  -e SOURCE=batdongsan \
  -e CRAWLER_TYPE=default \
  -e MAX_CONCURRENT=5 \
  -e BATCH_SIZE=100 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/detail_crawler/main.py
```

#### Cấu hình hiệu suất cao:

```bash
docker run -d \
  --name detail-crawler-high-performance \
  --network real_estate_network \
  --memory=2g \
  --cpus=2 \
  -e SOURCE=batdongsan \
  -e CRAWLER_TYPE=playwright \
  -e MAX_CONCURRENT=20 \
  -e BATCH_SIZE=200 \
  -e RETRY_LIMIT=2 \
  -e RETRY_DELAY=3 \
  -e COMMIT_THRESHOLD=50 \
  -e COMMIT_INTERVAL=30 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/detail_crawler/main.py
```

#### Cấu hình chế độ run-once:

```bash
docker run -d \
  --name detail-crawler-run-once \
  --network real_estate_network \
  -e SOURCE=batdongsan \
  -e RUN_ONCE_MODE=true \
  -e IDLE_TIMEOUT=120 \
  -e MAX_CONCURRENT=10 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/detail_crawler/main.py
```

---

## 3. API Crawler Container (Thu thập dữ liệu qua API - chủ yếu Chotot)

### Tham số môi trường (Environment Variables)

| Tham số           | Mô tả                                     | Kiểu dữ liệu | Giá trị mặc định | Ví dụ                       | Bắt buộc |
| ----------------- | ----------------------------------------- | ------------ | ---------------- | --------------------------- | -------- |
| `SOURCE`          | Nguồn dữ liệu API                         | string       | "chotot"         | "chotot"                    | ✅       |
| `MAX_CONCURRENT`  | Số thread đồng thời                       | integer      | 5                | 3, 5, 10                    | ❌       |
| `START_PAGE`      | Trang bắt đầu thu thập                    | integer      | 1                | 1, 10, 50                   | ❌       |
| `END_PAGE`        | Trang kết thúc thu thập                   | integer      | 5                | 5, 10, 100                  | ❌       |
| `REGION`          | Khu vực cần thu thập                      | string       | null             | "13000" (HCM), "12000" (HN) | ❌       |
| `OUTPUT_TOPIC`    | Kafka topic xuất dữ liệu                  | string       | "property-data"  | "property-data"             | ❌       |
| `INTERVAL`        | Khoảng thời gian giữa các lần chạy (giây) | integer      | 3600             | 1800, 3600, 7200            | ❌       |
| `STOP_ON_EMPTY`   | Dừng khi gặp trang trống                  | boolean      | true             | "true", "false"             | ❌       |
| `MAX_EMPTY_PAGES` | Số trang trống tối đa trước khi dừng      | integer      | 2                | 1, 2, 5                     | ❌       |

### Ví dụ Docker Run - API Crawler

#### Cấu hình cơ bản cho Chotot:

```bash
docker run -d \
  --name api-crawler-chotot \
  --network real_estate_network \
  -e SOURCE=chotot \
  -e MAX_CONCURRENT=5 \
  -e START_PAGE=1 \
  -e END_PAGE=50 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/api_crawler/main.py
```

#### Cấu hình cho khu vực cụ thể (HCM):

```bash
docker run -d \
  --name api-crawler-chotot-hcm \
  --network real_estate_network \
  -e SOURCE=chotot \
  -e REGION=13000 \
  -e MAX_CONCURRENT=10 \
  -e START_PAGE=1 \
  -e END_PAGE=100 \
  -e INTERVAL=1800 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/api_crawler/main.py
```

---

## 4. Storage Service Container (Lưu trữ dữ liệu từ Kafka)

### Tham số môi trường (Environment Variables)

| Tham số          | Mô tả                          | Kiểu dữ liệu | Giá trị mặc định        | Ví dụ                          | Bắt buộc |
| ---------------- | ------------------------------ | ------------ | ----------------------- | ------------------------------ | -------- |
| `STORAGE_TYPE`   | Loại storage                   | string       | "local"                 | "local", "hdfs", "s3"          | ❌       |
| `BATCH_SIZE`     | Kích thước batch để flush      | integer      | 20000                   | 10000, 20000, 50000            | ❌       |
| `FLUSH_INTERVAL` | Thời gian flush định kỳ (giây) | integer      | 300                     | 180, 300, 600                  | ❌       |
| `KAFKA_TOPIC`    | Kafka topic đọc dữ liệu        | string       | "property-data"         | "property-data"                | ❌       |
| `KAFKA_GROUP_ID` | Consumer group ID              | string       | "storage-service-group" | "storage-service-group"        | ❌       |
| `FILE_PREFIX`    | Prefix cho tên file            | string       | "property_data"         | "property_data", "real_estate" | ❌       |
| `FILE_FORMAT`    | Định dạng file lưu trữ         | string       | "json"/"parquet"        | "json", "parquet", "csv"       | ❌       |

### Storage Type Specific Parameters

#### Local Storage:

| Tham số           | Mô tả                 | Giá trị mặc định |
| ----------------- | --------------------- | ---------------- |
| `LOCAL_BASE_PATH` | Thư mục lưu trữ local | "/data"          |

#### HDFS Storage:

| Tham số             | Mô tả                  | Giá trị mặc định       |
| ------------------- | ---------------------- | ---------------------- |
| `HDFS_NAMENODE_URL` | URL của HDFS namenode  | "hdfs://namenode:9000" |
| `HDFS_BASE_PATH`    | Thư mục base trên HDFS | "/real_estate"         |

#### S3 Storage:

| Tham số                 | Mô tả          | Giá trị mặc định |
| ----------------------- | -------------- | ---------------- |
| `AWS_ACCESS_KEY_ID`     | AWS Access Key | -                |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Key | -                |
| `AWS_S3_BUCKET`         | S3 bucket name | -                |
| `AWS_S3_PREFIX`         | S3 prefix path | "real_estate/"   |

### Ví dụ Docker Run - Storage Service

#### Local storage:

```bash
docker run -d \
  --name storage-service-local \
  --network real_estate_network \
  -v /host/data:/data \
  -e STORAGE_TYPE=local \
  -e LOCAL_BASE_PATH=/data \
  -e BATCH_SIZE=10000 \
  -e FILE_FORMAT=parquet \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/storage_service/main.py
```

#### HDFS storage:

```bash
docker run -d \
  --name storage-service-hdfs \
  --network real_estate_network \
  -e STORAGE_TYPE=hdfs \
  -e HDFS_NAMENODE_URL=hdfs://namenode:9000 \
  -e HDFS_BASE_PATH=/real_estate \
  -e BATCH_SIZE=20000 \
  -e FILE_FORMAT=json \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  crawler:latest python services/storage_service/main.py
```

---

## 5. Docker Compose Example - Hệ thống đầy đủ

### Cấu hình đầy đủ hệ thống crawler với tất cả services

```yaml
version: '3.8'

services:
    # Kafka Infrastructure
    zookeeper:
        image: confluentinc/cp-zookeeper:latest
        environment:
            ZOOKEEPER_CLIENT_PORT: 2181
            ZOOKEEPER_TICK_TIME: 2000
        networks:
            - real_estate_network

    kafka:
        image: confluentinc/cp-kafka:latest
        depends_on:
            - zookeeper
        ports:
            - '9092:9092'
        environment:
            KAFKA_BROKER_ID: 1
            KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
            KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,PLAINTEXT_HOST://localhost:9092
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
            KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
        networks:
            - real_estate_network

    # List Crawler cho Batdongsan
    list-crawler-batdongsan:
        build: ./crawler
        container_name: list-crawler-batdongsan
        environment:
            - SOURCE=batdongsan
            - CRAWLER_TYPE=playwright
            - MAX_CONCURRENT=5
            - START_PAGE=1
            - END_PAGE=500
            - FORCE_CRAWL=false
            - FORCE_CRAWL_INTERVAL_HOURS=24
            - KAFKA_BOOTSTRAP_SERVERS=kafka:19092
        volumes:
            - ./data/checkpoint:/app/checkpoint
        command: python services/list_crawler/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network

    # Detail Crawler cho Batdongsan
    detail-crawler-batdongsan:
        build: ./crawler
        container_name: detail-crawler-batdongsan
        environment:
            - SOURCE=batdongsan
            - CRAWLER_TYPE=default
            - MAX_CONCURRENT=10
            - BATCH_SIZE=100
            - RETRY_LIMIT=3
            - RETRY_DELAY=5
            - RUN_ONCE_MODE=false
            - KAFKA_BOOTSTRAP_SERVERS=kafka:19092
        command: python services/detail_crawler/main.py
        depends_on:
            - kafka
            - list-crawler-batdongsan
        networks:
            - real_estate_network

    # API Crawler cho Chotot
    api-crawler-chotot:
        build: ./crawler
        container_name: api-crawler-chotot
        environment:
            - SOURCE=chotot
            - MAX_CONCURRENT=10
            - START_PAGE=1
            - END_PAGE=100
            - INTERVAL=3600
            - STOP_ON_EMPTY=true
            - MAX_EMPTY_PAGES=2
            - KAFKA_BOOTSTRAP_SERVERS=kafka:19092
        command: python services/api_crawler/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network

    # Detail Crawler cho Chotot
    detail-crawler-chotot:
        build: ./crawler
        container_name: detail-crawler-chotot
        environment:
            - SOURCE=chotot
            - CRAWLER_TYPE=default
            - MAX_CONCURRENT=8
            - BATCH_SIZE=150
            - KAFKA_BOOTSTRAP_SERVERS=kafka:19092
        command: python services/detail_crawler/main.py
        depends_on:
            - kafka
            - api-crawler-chotot
        networks:
            - real_estate_network

    # Storage Service - Local
    storage-service-local:
        build: ./crawler
        container_name: storage-service-local
        environment:
            - STORAGE_TYPE=local
            - LOCAL_BASE_PATH=/data
            - BATCH_SIZE=10000
            - FLUSH_INTERVAL=300
            - FILE_FORMAT=parquet
            - KAFKA_BOOTSTRAP_SERVERS=kafka:19092
            - KAFKA_TOPIC=property-data
        volumes:
            - ./data/output:/data
        command: python services/storage_service/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network

    # Storage Service - HDFS (optional)
    storage-service-hdfs:
        build: ./crawler
        container_name: storage-service-hdfs
        environment:
            - STORAGE_TYPE=hdfs
            - HDFS_NAMENODE_URL=hdfs://namenode:9000
            - HDFS_BASE_PATH=/real_estate
            - BATCH_SIZE=20000
            - FLUSH_INTERVAL=600
            - FILE_FORMAT=json
            - KAFKA_BOOTSTRAP_SERVERS=kafka:19092
        command: python services/storage_service/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network
        profiles:
            - hdfs # Chỉ chạy khi có profile hdfs

networks:
    real_estate_network:
        driver: bridge

volumes:
    kafka_data:
    zookeeper_data:
```

### Chạy hệ thống:

```bash
# Chạy hệ thống cơ bản (local storage)
docker-compose up -d

# Chạy với HDFS storage
docker-compose --profile hdfs up -d

# Scale detail crawlers
docker-compose up -d --scale detail-crawler-batdongsan=3

# Xem logs
docker-compose logs -f list-crawler-batdongsan
docker-compose logs -f detail-crawler-batdongsan
```

```yaml
version: '3.8'

services:
    # List Crawler cho Batdongsan
    list-crawler-batdongsan:
        build: ./crawler
        container_name: list-crawler-batdongsan
        environment:
            - SOURCE=batdongsan
            - CRAWLER_TYPE=playwright
            - MAX_CONCURRENT=5
            - START_PAGE=1
            - END_PAGE=500
            - FORCE_CRAWL=false
            - FORCE_CRAWL_INTERVAL_HOURS=24
            - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
        volumes:
            - ./data/checkpoint:/app/checkpoint
        command: python services/list_crawler/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network

    # Detail Crawler cho Batdongsan
    detail-crawler-batdongsan:
        build: ./crawler
        container_name: detail-crawler-batdongsan
        environment:
            - SOURCE=batdongsan
            - CRAWLER_TYPE=default
            - MAX_CONCURRENT=10
            - BATCH_SIZE=100
            - RETRY_LIMIT=3
            - RETRY_DELAY=5
            - RUN_ONCE_MODE=false
            - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
        command: python services/detail_crawler/main.py
        depends_on:
            - kafka
            - list-crawler-batdongsan
        networks:
            - real_estate_network

    # List Crawler cho Chotot
    list-crawler-chotot:
        build: ./crawler
        container_name: list-crawler-chotot
        environment:
            - SOURCE=chotot
            - CRAWLER_TYPE=playwright
            - MAX_CONCURRENT=3
            - START_PAGE=1
            - END_PAGE=200
            - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
        volumes:
            - ./data/checkpoint:/app/checkpoint
        command: python services/list_crawler/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network

    # Detail Crawler cho Chotot
    detail-crawler-chotot:
        build: ./crawler
        container_name: detail-crawler-chotot
        environment:
            - SOURCE=chotot
            - CRAWLER_TYPE=default
            - MAX_CONCURRENT=8
            - BATCH_SIZE=150
            - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
        command: python services/detail_crawler/main.py
        depends_on:
            - kafka
            - list-crawler-chotot
        networks:
            - real_estate_network

    # API Crawler cho Chotot
    api-crawler-chotot:
        build: ./crawler
        container_name: api-crawler-chotot
        environment:
            - SOURCE=chotot
            - MAX_CONCURRENT=5
            - START_PAGE=1
            - END_PAGE=50
            - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
        command: python services/api_crawler/main.py
        depends_on:
            - kafka
        networks:
            - real_estate_network

networks:
    real_estate_network:
        external: true
```

---

## 6. Advanced Configuration và Environment Variables đầy đủ

### Biến môi trường chung cho tất cả services:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:19092
KAFKA_CLIENT_ID=crawler-service
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=false
KAFKA_MAX_POLL_INTERVAL_MS=600000

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Service Health Check
HEALTH_CHECK_INTERVAL=30
HEALTH_CHECK_TIMEOUT=10
```

### Factory Registry - Các loại crawler có sẵn:

#### List Crawlers:

```python
LIST_CRAWLER_REGISTRY = {
    "batdongsan": {
        "default": BatdongsanListCrawler,
        "playwright": BatdongsanListCrawler,
    }
}
```

#### Detail Crawlers:

```python
DETAIL_CRAWLER_REGISTRY = {
    "batdongsan": {
        "default": BatdongsanDetailCrawler,
    },
    "chotot": {
        "default": ChototDetailCrawler,
    }
}
```

#### API Crawlers:

```python
API_CRAWLER_REGISTRY = {
    "chotot": {
        "default": ChototApiCrawler,
    }
}
```

---

## 7. Troubleshooting và Performance Tuning

### Memory và Resource Management:

```bash
# Tăng memory cho container
docker run --memory=4g --memory-swap=8g \
  --cpus=2.0 \
  -e MAX_CONCURRENT=20 \
  [other options...] crawler:latest

# Giới hạn JVM memory cho Kafka consumer
-e KAFKA_HEAP_OPTS="-Xmx2g -Xms1g"
```

### Kafka Performance Tuning:

```bash
# Producer settings
-e KAFKA_BATCH_SIZE=65536
-e KAFKA_LINGER_MS=10
-e KAFKA_COMPRESSION_TYPE=snappy

# Consumer settings
-e KAFKA_FETCH_MIN_BYTES=1024
-e KAFKA_FETCH_MAX_WAIT_MS=500
-e KAFKA_MAX_PARTITION_FETCH_BYTES=1048576
```

### Monitoring Commands:

```bash
# Kiểm tra Kafka topics và messages
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic property-urls --from-beginning

# Monitor container resources
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"

# Check logs với filters
docker logs -f --since=1h list-crawler-batdongsan | grep ERROR
docker logs -f detail-crawler-batdongsan | jq '.level' | sort | uniq -c
```

### Debug Mode:

```bash
# Chạy với debug mode
docker run -it --rm \
  -e LOG_LEVEL=DEBUG \
  -e MAX_CONCURRENT=1 \
  -e START_PAGE=1 \
  -e END_PAGE=2 \
  crawler:latest python services/list_crawler/main.py
```

---

## 8. Security và Best Practices

### Security Configuration:

```bash
# Chạy với non-root user
docker run --user 1000:1000 [options...] crawler:latest

# Network isolation
docker network create --driver bridge --internal crawler_internal_network

# Secrets management
docker run -e KAFKA_PASSWORD_FILE=/run/secrets/kafka_password \
  --secret kafka_password \
  [options...] crawler:latest
```

### Production Best Practices:

1. **Health Checks:**

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s \
  CMD python -c "import requests; requests.get('http://localhost:8080/health')"
```

2. **Resource Limits:**

```yaml
deploy:
    resources:
        limits:
            cpus: '2.0'
            memory: 4G
        reservations:
            cpus: '0.5'
            memory: 1G
```

3. **Restart Policies:**

```yaml
restart: unless-stopped
deploy:
    restart_policy:
        condition: on-failure
        delay: 30s
        max_attempts: 3
```

Bảng biểu này bây giờ đã bao gồm đầy đủ thông tin về tất cả các services trong hệ thống crawler, bao gồm API Crawler và Storage Service mà trước đó đã bị thiếu!
