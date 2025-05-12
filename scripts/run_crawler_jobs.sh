#!/bin/bash

# Script để chạy các crawler job thủ công
# Sử dụng: ./scripts/run_crawler_jobs.sh [batdongsan|chotot|all] [list|detail|both]

# Màu sắc cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Thư mục gốc của dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Kiểm tra tham số
if [ $# -lt 2 ]; then
    echo -e "${RED}Thiếu tham số!${NC}"
    echo -e "Sử dụng: $0 [batdongsan|chotot|all] [list|detail|both]"
    exit 1
fi

SOURCE=$1
JOB_TYPE=$2
CURRENT_DATE=$(date +%Y-%m-%d)

# Kiểm tra xem Docker đã chạy chưa
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}Docker không chạy. Vui lòng khởi động Docker trước.${NC}"
    exit 1
fi

# Kiểm tra xem crawler container đã chạy chưa
if ! docker ps | grep -q "list-crawler"; then
    echo -e "${YELLOW}Crawler container chưa chạy. Đang khởi động hệ thống...${NC}"
    ./scripts/start_all.sh
    sleep 10
fi

# Hàm chạy crawler danh sách
run_list_crawler() {
    local src=$1
    local crawler_type="playwright"
    local concurrent=5

    if [ "$src" == "chotot" ]; then
        crawler_type="api"
        concurrent=10
    fi

    echo -e "${BLUE}[INFO]${NC} Đang chạy crawler danh sách cho $src..."

    docker exec -it list-crawler python -m crawler.services.list_crawler.main \
        --source $src \
        --crawler_type $crawler_type \
        --start_page 1 --end_page 20 \
        --concurrent $concurrent \
        --storage_type hdfs \
        --output_dir /data/realestate/$src/list_$CURRENT_DATE

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}[SUCCESS]${NC} Crawler danh sách $src hoàn thành!"
    else
        echo -e "${RED}[ERROR]${NC} Crawler danh sách $src thất bại!"
    fi
}

# Hàm chạy crawler chi tiết
run_detail_crawler() {
    local src=$1
    local concurrent=10

    if [ "$src" == "chotot" ]; then
        concurrent=15
    fi

    echo -e "${BLUE}[INFO]${NC} Đang chạy crawler chi tiết cho $src..."

    docker exec -it detail-crawler python -m crawler.services.detail_crawler.main \
        --source $src \
        --input_dir /data/realestate/$src/list_$CURRENT_DATE \
        --concurrent $concurrent \
        --storage_type hdfs \
        --output_dir /data/realestate/$src/detail_$CURRENT_DATE

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}[SUCCESS]${NC} Crawler chi tiết $src hoàn thành!"
    else
        echo -e "${RED}[ERROR]${NC} Crawler chi tiết $src thất bại!"
    fi

    # Xử lý và lưu trữ dữ liệu
    echo -e "${BLUE}[INFO]${NC} Đang xử lý và lưu trữ dữ liệu $src..."

    docker exec -it storage-service python -m crawler.services.storage_service.main \
        --storage_type hdfs \
        --batch_size 200 \
        --flush_interval 60 \
        --input_dir /data/realestate/$src/detail_$CURRENT_DATE \
        --output_dir /data/realestate/processed/${src}_$CURRENT_DATE

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}[SUCCESS]${NC} Xử lý dữ liệu $src hoàn thành!"
    else
        echo -e "${RED}[ERROR]${NC} Xử lý dữ liệu $src thất bại!"
    fi
}

# Hàm chạy toàn bộ quy trình cho một nguồn
run_full_pipeline() {
    local src=$1
    run_list_crawler $src
    run_detail_crawler $src
}

# Chạy các job theo tham số
case $SOURCE in
    batdongsan|chotot)
        case $JOB_TYPE in
            list)
                run_list_crawler $SOURCE
                ;;
            detail)
                run_detail_crawler $SOURCE
                ;;
            both)
                run_full_pipeline $SOURCE
                ;;
            *)
                echo -e "${RED}Loại job không hợp lệ. Sử dụng: list, detail hoặc both${NC}"
                exit 1
                ;;
        esac
        ;;
    all)
        case $JOB_TYPE in
            list)
                run_list_crawler batdongsan
                run_list_crawler chotot
                ;;
            detail)
                run_detail_crawler batdongsan
                run_detail_crawler chotot
                ;;
            both)
                run_full_pipeline batdongsan
                run_full_pipeline chotot
                ;;
            *)
                echo -e "${RED}Loại job không hợp lệ. Sử dụng: list, detail hoặc both${NC}"
                exit 1
                ;;
        esac
        ;;
    *)
        echo -e "${RED}Nguồn không hợp lệ. Sử dụng: batdongsan, chotot hoặc all${NC}"
        exit 1
        ;;
esac

echo -e "${GREEN}Hoàn thành công việc!${NC}"
exit 0
