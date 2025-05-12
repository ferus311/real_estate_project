#!/bin/bash

# Script dừng hệ thống crawler bất động sản
# Dừng các dịch vụ theo thứ tự ngược lại với khởi động
cd ./docker
# Màu sắc cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Thư mục gốc của dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo -e "${BLUE}[INFO]${NC} Bắt đầu dừng các dịch vụ..."

# Dừng crawler shell
echo -e "${BLUE}[INFO]${NC} Dừng crawler shell..."
docker compose -f ${PROJECT_DIR}/docker/yml/crawler.yml down

# Dừng Airflow
echo -e "${BLUE}[INFO]${NC} Dừng Airflow..."
docker compose -f ${PROJECT_DIR}/docker/yml/airflow.yml down

# Dừng Kafka
echo -e "${BLUE}[INFO]${NC} Dừng Kafka và Zookeeper..."
docker compose -f ${PROJECT_DIR}/docker/yml/kafka.yml down

# Dừng HDFS
echo -e "${BLUE}[INFO]${NC} Dừng HDFS cluster..."
docker compose -f ${PROJECT_DIR}/docker/yml/hdfs.yml down

echo -e "${GREEN}[SUCCESS]${NC} Tất cả các dịch vụ đã được dừng thành công!"

# Hỏi người dùng có muốn xóa các networks không
echo -e "${YELLOW}[QUESTION]${NC} Bạn có muốn xóa các networks (hdfs_network, kafka_network) không? (y/n)"
read -r answer
if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
    echo -e "${BLUE}[INFO]${NC} Đang xóa networks..."
    docker network rm hdfs_network kafka_network
    echo -e "${GREEN}[SUCCESS]${NC} Đã xóa networks!"
fi

# Hỏi người dùng có muốn xóa dữ liệu trong volumes không
echo -e "${YELLOW}[QUESTION]${NC} Bạn có muốn xóa dữ liệu trong thư mục volumes không? (y/n)"
echo -e "${RED}[WARNING]${NC} Điều này sẽ xóa tất cả dữ liệu HDFS, Kafka, Zookeeper!"
read -r answer
if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
    echo -e "${BLUE}[INFO]${NC} Đang xóa dữ liệu trong thư mục volumes..."
    rm -rf ${PROJECT_DIR}/docker/volumes/*
    echo -e "${GREEN}[SUCCESS]${NC} Đã xóa dữ liệu trong thư mục volumes!"
fi
