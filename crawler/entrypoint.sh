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
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
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

# Start the specified service
echo "Starting $SERVICE_TYPE service for source $SOURCE..."
case $SERVICE_TYPE in
  list_crawler)
    echo "Running list crawler for $SOURCE with type $CRAWLER_TYPE"
    exec python -m crawler.services.list_crawler.main
    ;;
  detail_crawler)
    echo "Running detail crawler for $SOURCE with type $CRAWLER_TYPE"
    exec python -m crawler.services.detail_crawler.main
    ;;
  api_crawler)
    echo "Running API crawler for $SOURCE"
    exec python -m crawler.services.api_crawler.main
    ;;
  storage_service)
    echo "Running storage service with type $STORAGE_TYPE"
    exec python -m crawler.services.storage.main
    ;;
  retry_service)
    echo "Running retry service for $SOURCE"
    exec python -m crawler.services.retry.main
    ;;
  test)
    echo "Running test mode with command: $@"
    exec "$@"
    ;;
  *)
    echo "Unknown service: $SERVICE_TYPE"
    echo "Available services: list_crawler, detail_crawler, api_crawler, storage_service, retry_service, test"
    exit 1
    ;;
esac
