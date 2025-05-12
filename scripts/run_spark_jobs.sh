#!/bin/bash

# Script để chạy các Spark job thủ công
# Sử dụng: ./scripts/run_spark_jobs.sh [data_cleaning|data_integration|analytics|ml|all] [YYYY-MM-DD]

# Màu sắc cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Thư mục gốc của dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Kiểm tra tham số
if [ $# -lt 1 ]; then
    echo -e "${RED}Thiếu tham số!${NC}"
    echo -e "Sử dụng: $0 [data_cleaning|data_integration|analytics|ml|all] [YYYY-MM-DD]"
    exit 1
fi

JOB_TYPE=$1
PROCESS_DATE=${2:-$(date +%Y-%m-%d)}

# Kiểm tra xem Docker đã chạy chưa
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}Docker không chạy. Vui lòng khởi động Docker trước.${NC}"
    exit 1
fi

# Kiểm tra xem Spark container đã chạy chưa
if ! docker ps | grep -q "spark-master"; then
    echo -e "${YELLOW}Spark container chưa chạy. Đang khởi động hệ thống...${NC}"
    ./scripts/start_all.sh
    sleep 10
fi

# Biến môi trường
SPARK_MASTER="spark://spark-master:7077"
HDFS_INPUT_DIR="/data/realestate/processed"
HDFS_OUTPUT_DIR="/data/realestate/analytics"
HDFS_MODEL_DIR="/data/realestate/models"

# Hàm chạy Spark job
run_spark_job() {
    local job_file=$1
    local job_args=$2
    local job_name=$(basename $job_file .py)

    echo -e "${BLUE}[INFO]${NC} Đang chạy Spark job: $job_name..."

    docker exec -it spark-master spark-submit \
        --master $SPARK_MASTER \
        --name $job_name \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.driver.bindAddress=0.0.0.0 \
        $job_file $job_args

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}[SUCCESS]${NC} Spark job $job_name hoàn thành!"
    else
        echo -e "${RED}[ERROR]${NC} Spark job $job_name thất bại!"
        exit 1
    fi
}

# Hàm chạy job làm sạch dữ liệu
run_data_cleaning() {
    echo -e "${BLUE}[INFO]${NC} Đang chạy job làm sạch dữ liệu..."

    # Làm sạch dữ liệu Batdongsan
    run_spark_job \
        "/opt/bitnami/spark/jobs/data_cleaning/clean_batdongsan.py" \
        "--input ${HDFS_INPUT_DIR}/batdongsan_${PROCESS_DATE} --output ${HDFS_OUTPUT_DIR}/cleaned/batdongsan_${PROCESS_DATE}"

    # Làm sạch dữ liệu Chotot
    run_spark_job \
        "/opt/bitnami/spark/jobs/data_cleaning/clean_chotot.py" \
        "--input ${HDFS_INPUT_DIR}/chotot_${PROCESS_DATE} --output ${HDFS_OUTPUT_DIR}/cleaned/chotot_${PROCESS_DATE}"
}

# Hàm chạy job tích hợp dữ liệu
run_data_integration() {
    echo -e "${BLUE}[INFO]${NC} Đang chạy job tích hợp dữ liệu..."

    run_spark_job \
        "/opt/bitnami/spark/jobs/data_integration/integrate_sources.py" \
        "--batdongsan_input ${HDFS_OUTPUT_DIR}/cleaned/batdongsan_${PROCESS_DATE} --chotot_input ${HDFS_OUTPUT_DIR}/cleaned/chotot_${PROCESS_DATE} --output ${HDFS_OUTPUT_DIR}/integrated/realestate_${PROCESS_DATE}"
}

# Hàm chạy job phân tích dữ liệu
run_analytics() {
    echo -e "${BLUE}[INFO]${NC} Đang chạy job phân tích dữ liệu..."

    run_spark_job \
        "/opt/bitnami/spark/jobs/analytics/price_analytics.py" \
        "--input ${HDFS_OUTPUT_DIR}/integrated/realestate_${PROCESS_DATE} --output ${HDFS_OUTPUT_DIR}/analytics/stats_${PROCESS_DATE}"

    # Xuất dữ liệu cho API
    run_spark_job \
        "/opt/bitnami/spark/jobs/export/export_for_api.py" \
        "--input ${HDFS_OUTPUT_DIR}/integrated/realestate_${PROCESS_DATE} --analytics ${HDFS_OUTPUT_DIR}/analytics/stats_${PROCESS_DATE} --output ${HDFS_OUTPUT_DIR}/api/data_${PROCESS_DATE}"
}

# Hàm chạy job huấn luyện mô hình ML
run_ml_job() {
    echo -e "${BLUE}[INFO]${NC} Đang chạy job huấn luyện mô hình ML..."

    # Chuẩn bị dữ liệu huấn luyện
    run_spark_job \
        "/opt/bitnami/spark/jobs/ml/prepare_training_data.py" \
        "--input ${HDFS_OUTPUT_DIR}/integrated --output ${HDFS_MODEL_DIR}/training_data_${PROCESS_DATE} --months 6"

    # Huấn luyện mô hình cơ bản
    run_spark_job \
        "/opt/bitnami/spark/jobs/ml/train_linear_model.py" \
        "--input ${HDFS_MODEL_DIR}/training_data_${PROCESS_DATE} --output ${HDFS_MODEL_DIR}/basic_model_${PROCESS_DATE} --features area,num_bedrooms,num_bathrooms,location_score"

    # Huấn luyện mô hình nâng cao
    run_spark_job \
        "/opt/bitnami/spark/jobs/ml/train_gbm_model.py" \
        "--input ${HDFS_MODEL_DIR}/training_data_${PROCESS_DATE} --output ${HDFS_MODEL_DIR}/advanced_model_${PROCESS_DATE} --features area,num_bedrooms,num_bathrooms,location_score,nearby_facilities,year_built,floor"

    # Đánh giá mô hình
    run_spark_job \
        "/opt/bitnami/spark/jobs/ml/evaluate_models.py" \
        "--basic_model ${HDFS_MODEL_DIR}/basic_model_${PROCESS_DATE} --advanced_model ${HDFS_MODEL_DIR}/advanced_model_${PROCESS_DATE} --test_data ${HDFS_MODEL_DIR}/training_data_${PROCESS_DATE}/test --output ${HDFS_MODEL_DIR}/evaluation/metrics_${PROCESS_DATE}.json"

    # Xuất mô hình để triển khai
    run_spark_job \
        "/opt/bitnami/spark/jobs/ml/export_model.py" \
        "--model ${HDFS_MODEL_DIR}/advanced_model_${PROCESS_DATE} --output ${HDFS_MODEL_DIR}/deployment/model_${PROCESS_DATE}"
}

# Chạy các job theo tham số
case $JOB_TYPE in
    data_cleaning)
        run_data_cleaning
        ;;
    data_integration)
        run_data_integration
        ;;
    analytics)
        run_analytics
        ;;
    ml)
        run_ml_job
        ;;
    all)
        run_data_cleaning
        run_data_integration
        run_analytics
        run_ml_job
        ;;
    *)
        echo -e "${RED}Loại job không hợp lệ. Sử dụng: data_cleaning, data_integration, analytics, ml hoặc all${NC}"
        exit 1
        ;;
esac

echo -e "${GREEN}Hoàn thành công việc!${NC}"
exit 0
