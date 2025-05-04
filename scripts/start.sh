#!/bin/bash

# Đặt biến để dễ debug
set -e  # Thoát script nếu có lỗi
START_TIME=$(date +%s)

echo "===== BẮT ĐẦU KHỞI ĐỘNG HỆ THỐNG THU THẬP DỮ LIỆU BẤT ĐỘNG SẢN ====="
echo "Thời gian bắt đầu: $(date)"

# Tạo các thư mục cần thiết (phải khớp với cấu trúc trong docker-compose.yml)
echo "Tạo các thư mục cần thiết..."
mkdir -p docker/volumes/hdfs/namenode
mkdir -p docker/volumes/hdfs/datanode1
mkdir -p docker/volumes/hdfs/datanode2
mkdir -p docker/volumes/hdfs/datanode3
mkdir -p docker/volumes/kafka
mkdir -p data_processing/airflow/dags
mkdir -p data_processing/airflow/logs
mkdir -p data_processing/airflow/plugins
mkdir -p data_processing/notebooks

# Đặt biến môi trường cho Airflow
export AIRFLOW_UID=$(id -u)
export AIRFLOW_VERSION=2.8.1
echo "AIRFLOW_UID đã được đặt thành: $AIRFLOW_UID"

# Kiểm tra file hadoop.env
HADOOP_ENV_FILE="docker/hadoop.env"

if [ ! -f "$HADOOP_ENV_FILE" ]; then
    echo "CẢNH BÁO: Không tìm thấy file hadoop.env tại $HADOOP_ENV_FILE."
    exit 1
fi

# Di chuyển đến thư mục docker
cd docker

# Khởi chạy hệ thống bằng docker-compose
echo "===== KHỞI ĐỘNG HỆ THỐNG ====="

# Khởi động Postgres trước
echo "Khởi động Postgres..."
docker compose up -d postgres
sleep 10

# Chạy airflow-init và đợi nó hoàn thành
echo "Khởi động Airflow Init để thiết lập database..."
docker compose run --rm airflow-init
echo "Airflow Init đã chạy xong."

# Đợi 5 giây để đảm bảo database đã sẵn sàng
sleep 5

# Khởi động các service khác, trừ airflow-webserver và airflow-scheduler
echo "Khởi động các dịch vụ cơ sở hạ tầng..."
docker compose up -d zookeeper kafka namenode datanode1 datanode2 datanode3

# Đợi HDFS khởi động hoàn tất
echo "Đợi HDFS khởi động hoàn tất..."
sleep 20

# Khởi động Airflow webserver và scheduler
echo "Khởi động Airflow webserver và scheduler..."
docker compose up -d airflow-webserver airflow-scheduler
sleep 10

# Khởi động Spark
echo "Khởi động Spark..."
docker compose up -d spark-master spark-worker-1 spark-worker-2
sleep 10

# Khởi động các service còn lại
echo "Khởi động các service còn lại..."
docker compose up -d list-crawler-service detail-crawler-service storage-service retry-service jupyter api webapp

# Kiểm tra các dịch vụ chính đã hoạt động chưa
echo "===== KIỂM TRA TRẠNG THÁI DỊCH VỤ ====="

# Kiểm tra HDFS
if docker ps | grep namenode > /dev/null; then
    echo "✅ HDFS namenode đã khởi động thành công."
else
    echo "❌ HDFS namenode khởi động thất bại. Kiểm tra logs: docker logs realestate_namenode"
fi

# Kiểm tra Kafka
if docker ps | grep kafka > /dev/null; then
    echo "✅ Kafka đã khởi động thành công."
else
    echo "❌ Kafka khởi động thất bại. Kiểm tra logs: docker logs realestate_kafka"
fi

# Kiểm tra Airflow
if docker ps | grep airflow-web > /dev/null; then
    echo "✅ Airflow webserver đã khởi động thành công."
else
    echo "❌ Airflow webserver khởi động thất bại. Kiểm tra logs: docker logs realestate_airflow_web"
fi

# Kiểm tra Spark
if docker ps | grep spark-master > /dev/null; then
    echo "✅ Spark master đã khởi động thành công."
else
    echo "❌ Spark master khởi động thất bại. Kiểm tra logs: docker logs realestate_spark_master"
fi

# Tính thời gian khởi động
END_TIME=$(date +%s)
RUNTIME=$((END_TIME-START_TIME))
MINUTES=$((RUNTIME / 60))
SECONDS=$((RUNTIME % 60))

echo "===== HOÀN THÀNH KHỞI ĐỘNG ====="
echo "Tất cả các dịch vụ đã được khởi động trong $MINUTES phút $SECONDS giây!"
echo
echo "TRUY CẬP CÁC GIAO DIỆN:"
echo "- HDFS UI: http://localhost:9870"
echo "- Airflow UI: http://localhost:8080 (user/pass: admin/admin)"
echo "- Spark UI: http://localhost:8181"
echo "- Jupyter: http://localhost:8888"
echo "- Web App: http://localhost:3000"
echo
echo "KIỂM TRA TRẠNG THÁI:"
echo "- Xem danh sách container: docker ps"
echo "- Xem logs HDFS: docker logs realestate_namenode"
echo "- Xem logs Kafka: docker logs realestate_kafka"
echo "- Xem logs Airflow: docker logs realestate_airflow_web"
echo
echo "DỪNG HỆ THỐNG:"
echo "- Sử dụng: ./scripts/stop_all.sh"
