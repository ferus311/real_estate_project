#!/bin/bash
set -e

# Create necessary directories
echo "Creating necessary directories and setting permissions..."
mkdir -p /app/logs
mkdir -p /app/crawler/checkpoint
chmod -R 777 /app/logs
chmod -R 777 /app/crawler/checkpoint

# Lấy thông tin service từ biến môi trường
SERVICE_TYPE=${SERVICE_TYPE:-list_crawler}
SOURCE=${SOURCE:-batdongsan}
CRAWLER_TYPE=${CRAWLER_TYPE:-default}

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-kafka1:19092}
KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d':' -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d':' -f2)

# Kiểm tra xem có cần kết nối HDFS không
if [ "$SERVICE_TYPE" = "storage_service" ] && [ "$STORAGE_TYPE" = "hdfs" ]; then
  WAIT_FOR_HDFS=true
else
  WAIT_FOR_HDFS=false
fi

# Wait for Kafka to be ready
echo "Waiting for Kafka at $KAFKA_HOST:$KAFKA_PORT..."
until nc -z ${KAFKA_HOST} ${KAFKA_PORT} || [ $? -eq 1 ]; do
  echo "Kafka is unavailable - sleeping"
  sleep 3
done
echo "Kafka is up"

# Wait for HDFS if needed
if [ "$WAIT_FOR_HDFS" = true ]; then
  HDFS_NAMENODE=${HDFS_NAMENODE:-namenode:9870}
  HDFS_HOST=$(echo $HDFS_NAMENODE | cut -d':' -f1)
  HDFS_PORT=$(echo $HDFS_NAMENODE | cut -d':' -f2)

  echo "Waiting for HDFS at $HDFS_HOST:$HDFS_PORT..."
  until nc -z ${HDFS_HOST} ${HDFS_PORT} || [ $? -eq 1 ]; do
    echo "HDFS is unavailable - sleeping"
    sleep 3
  done
  echo "HDFS is up"
fi

echo "Environment:"
echo "SERVICE_TYPE: $SERVICE_TYPE"
echo "SOURCE: $SOURCE"
echo "CRAWLER_TYPE: $CRAWLER_TYPE"
echo "KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS"

# Đặt đường dẫn chứa mã nguồn
CRAWLER_DIR="/app"

# Tạo thư mục checkpoint nếu chưa tồn tại
if [ ! -d "$CRAWLER_DIR/checkpoint" ]; then
    mkdir -p "$CRAWLER_DIR/checkpoint"
    echo "Created checkpoint directory: $CRAWLER_DIR/checkpoint"
fi

# Cấu hình logging
export PYTHONUNBUFFERED=1
export LOG_LEVEL=${LOG_LEVEL:-INFO}

# Di chuyển đến thư mục chứa mã nguồn
cd "$CRAWLER_DIR"
echo "Working directory: $(pwd)"

# Hiển thị các biến môi trường
echo "Environment variables:"
echo "- KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS"
echo "- CRAWLER_MODE: $CRAWLER_MODE"
echo "- CRAWLER_SOURCE: $CRAWLER_SOURCE"
echo "- STORAGE_TYPE: $STORAGE_TYPE"

# Kiểm tra biến môi trường
if [ -z "$CRAWLER_MODE" ]; then
    echo "Error: CRAWLER_MODE environment variable is not set. Valid values: crawler, parser, linktracker, storage, scheduler, compaction"
    exit 1
fi

# Cài đặt thư viện Python từ requirements.txt
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Chạy các dịch vụ dựa trên CRAWLER_MODE
case "$CRAWLER_MODE" in
    crawler)
        # Kiểm tra CRAWLER_SOURCE
        if [ -z "$CRAWLER_SOURCE" ]; then
            echo "Error: CRAWLER_SOURCE environment variable is not set when CRAWLER_MODE=crawler"
            exit 1
        fi
        echo "Starting crawler for source: $CRAWLER_SOURCE"
        python -m sources.$CRAWLER_SOURCE.crawler.main
        ;;

    parser)
        # Kiểm tra CRAWLER_SOURCE
        if [ -z "$CRAWLER_SOURCE" ]; then
            echo "Error: CRAWLER_SOURCE environment variable is not set when CRAWLER_MODE=parser"
            exit 1
        fi
        echo "Starting parser for source: $CRAWLER_SOURCE"
        python -m sources.$CRAWLER_SOURCE.parser.main
        ;;

    linktracker)
        echo "Starting link tracker service"
        python -m services.link_tracker_service.main
        ;;

    storage)
        echo "Starting storage service"
        python -m services.storage_service.main
        ;;

    scheduler)
        echo "Starting scheduler service"
        python -m services.scheduler_service.main
        ;;

    compaction)
        echo "Starting file compaction service"
        # Lấy tham số từ biến môi trường
        MIN_DAYS=${COMPACTION_MIN_DAYS:-1}
        MAX_DAYS=${COMPACTION_MAX_DAYS:-7}
        MIN_FILES=${COMPACTION_MIN_FILES:-5}
        TARGET_SIZE=${COMPACTION_TARGET_SIZE_MB:-128}

        echo "Compaction parameters: min_days=$MIN_DAYS, max_days=$MAX_DAYS, min_files=$MIN_FILES, target_size=$TARGET_SIZE MB"
        python -m services.storage_service.file_compaction --min-days $MIN_DAYS --max-days $MAX_DAYS --min-files $MIN_FILES --target-size $TARGET_SIZE
        ;;

    *)
        echo "Error: Unknown CRAWLER_MODE value: $CRAWLER_MODE. Valid values: crawler, parser, linktracker, storage, scheduler, compaction"
        exit 1
        ;;
esac

# Kết thúc
echo "Process completed."
