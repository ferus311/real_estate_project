#!/bin/bash
echo "===== DỪNG HỆ THỐNG THU THẬP DỮ LIỆU BẤT ĐỘNG SẢN ====="

# Thư mục gốc của dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Dừng từng dịch vụ theo thứ tự ngược lại
echo "Dừng các crawler service..."
docker compose -f ${PROJECT_DIR}/docker/yml/crawler.yml down

echo "Dừng Spark..."
docker compose -f ${PROJECT_DIR}/docker/yml/spark.yml down

echo "Dừng Airflow..."
docker compose -f ${PROJECT_DIR}/docker/yml/airflow.yml down

echo "Dừng Kafka..."
docker compose -f ${PROJECT_DIR}/docker/yml/kafka.yml down

echo "Dừng HDFS..."
docker compose -f ${PROJECT_DIR}/docker/yml/hdfs.yml down

echo "===== ĐÃ DỪNG TOÀN BỘ HỆ THỐNG ====="
