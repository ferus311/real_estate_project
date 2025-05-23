#!/bin/bash

# Script khởi động hệ thống crawler bất động sản
# Khởi động các dịch vụ theo thứ tự phù hợp
set -e
# cd ./docker
# Màu sắc cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Thư mục gốc của dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Đảm bảo biến môi trường AIRFLOW_UID được thiết lập
export AIRFLOW_UID=${AIRFLOW_UID:-$(id -u)}
echo -e "${BLUE}[INFO]${NC} Sử dụng AIRFLOW_UID=${AIRFLOW_UID}"

# Hàm kiểm tra lỗi
check_error() {
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} $1"
        exit 1
    fi
}

# Hàm kiểm tra dịch vụ đã sẵn sàng chưa
wait_for_service() {
    local service=$1
    local port=$2
    local host=${3:-localhost}
    local timeout=${4:-60}

    echo -e "${YELLOW}[WAIT]${NC} Đang chờ $service khởi động tại $host:$port..."

    local start_time=$(date +%s)
    local end_time=$((start_time + timeout))

    while [ $(date +%s) -lt $end_time ]; do
        if nc -z $host $port > /dev/null 2>&1; then
            echo -e "${GREEN}[OK]${NC} $service đã sẵn sàng!"
            return 0
        fi
        sleep 2
    done

    echo -e "${RED}[TIMEOUT]${NC} $service không khởi động sau $timeout giây."
    return 1
}

dirs=(
  "docker/volumes/hdfs/namenode"
  "docker/volumes/hdfs/datanode1"
#   "docker/volumes/hdfs/datanode2"
#   "docker/volumes/hdfs/datanode3"
  "data_processing/airflow/dags"
  "data_processing/airflow/logs"
  "data_processing/airflow/plugins"
  "data_processing/notebooks"
  "data_processing/spark/jobs"
)


# Kiểm tra xem có thiếu thư mục nào không
need_init=false
for dir in "${dirs[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "⚠️  Thiếu thư mục: $dir"
    need_init=true
    break
  fi
done



# Kiểm tra xem các networks đã tồn tại chưa
echo -e "${BLUE}[INFO]${NC} Kiểm tra và tạo Docker networks..."
if ! docker network ls | grep -q hdfs_network; then
    echo -e "${BLUE}[INFO]${NC} Tạo network hdfs_network..."
    docker network create hdfs_network
    check_error "Không thể tạo network hdfs_network"
fi


# Khởi động HDFS cluster
echo -e "${BLUE}[INFO]${NC} Khởi động HDFS cluster..."
docker compose -f ${PROJECT_DIR}/docker/yml/hdfs.yml up -d namenode
check_error "Không thể khởi động namenode"

# Đợi namenode khởi động
wait_for_service "namenode" "9870" "localhost" 120

# Khởi động các datanode
echo -e "${BLUE}[INFO]${NC} Khởi động các datanode..."
docker compose -f ${PROJECT_DIR}/docker/yml/hdfs.yml up -d datanode1
check_error "Không thể khởi động datanode"

# Khởi động Spark
echo -e "${BLUE}[INFO]${NC} Khởi động Spark..."
docker compose -f ${PROJECT_DIR}/docker/yml/spark.yml up -d
check_error "Không thể khởi động Spark"
# Đợi Spark khởi động
wait_for_service "Spark" "8181" "localhost" 120


echo "===== HOÀN THÀNH KHỞI ĐỘNG ====="
echo "Tất cả các dịch vụ đã được khởi động trong $MINUTES phút $SECONDS giây!"
echo
echo "TRUY CẬP CÁC GIAO DIỆN:"
echo "- HDFS UI: http://localhost:9870"
# echo "- Airflow UI: http://localhost:8080 (user/pass: admin/admin)"
echo "- Spark UI: http://localhost:8181"
echo "- Jupyter: http://localhost:8888"
echo
echo "KIỂM TRA TRẠNG THÁI:"
echo "- Xem danh sách container: docker ps"
echo "- Xem logs HDFS: docker logs namenode"
echo "- Xem logs Airflow: docker logs airflow-web"
echo
echo "DỪNG HỆ THỐNG:"
echo "- Sử dụng: ./scripts/stop_all.sh"
