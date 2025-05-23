hdfs://namenode:9870/data/realestate/
│
├── raw/ # Dữ liệu thô từ các nguồn (định dạng JSON ưu tiên)
│ ├── batdongsan/
│ │ ├── house/ # Nhà ở
│ │ │ └── yyyy/mm/ # Phân vùng theo ngày để tối ưu lưu trữ
│ │ └── other/ # Các loại khác
│ │ └── yyyy/mm/
│ └── chotot/
│ ├── house/ # Nhà ở
│ │ └── yyyy/mm/
│ └── other/ # Các loại khác
│ └── yyyy/mm/
│
├── processed/ # Dữ liệu đã xử lý (định dạng Parquet)
│ ├── cleaned/ # Dữ liệu đã làm sạch
│ │ ├── house/ # Nhà ở
│ │ │ └── yyyy/mm/
│ │ └── all/ # Tất cả các loại
│ │ └── yyyy/mm/
│ ├── integrated/ # Dữ liệu đã tích hợp từ nhiều nguồn
│ │ ├── house/ # Nhà ở
│ │ │ └── yyyy/mm/
│ │ └── all/ # Tất cả các loại
│ │ └── yyyy/mm/
│ └── analytics/ # Dữ liệu tổng hợp cho phân tích
│ ├── price_trends/
│ │ ├── house/yyyy/mm/
│ │ ├── apartment/yyyy/mm/
│ │ ├── land/yyyy/mm/
│ │ └── all/yyyy/mm/
│ ├── region_stats/
│ │ ├── house/yyyy/mm/
│ │ ├── apartment/yyyy/mm/
│ │ ├── land/yyyy/mm/
│ │ └── all/yyyy/mm/
│ └── property_metrics/
│ ├── house/yyyy/mm/
│ ├── apartment/yyyy/mm/
│ ├── land/yyyy/mm/
│ └── all/yyyy/mm/
│
├── ml/ # Dữ liệu và mô hình ML (định dạng Parquet)
│ ├── features/ # Đặc trưng cho ML
│ │ ├── house/yyyy/mm/
│ │ └── all/yyyy/mm/
│ ├── training/ # Dữ liệu huấn luyện
│ │ ├── house/yyyy/mm/
│ │ └── all/yyyy/mm/
│ ├── models/ # Các phiên bản mô hình
│ │ ├── house/
│ │ │ ├── price_model_v1/
│ │ │ └── price_model_v2/
│ │ ├── apartment/
│ │ │ ├── price_model_v1/
│ │ │ └── price_model_v2/
│ │ ├── land/
│ │ │ ├── price_model_v1/
│ │ │ └── price_model_v2/
│ │ └── all/
│ │ ├── price_model_v1/
│ │ └── price_model_v2/
│ └── predictions/ # Kết quả dự đoán
│ ├── house/yyyy/mm/
│ ├── apartment/yyyy/mm/
│ ├── land/yyyy/mm/
│ ├── commercial/yyyy/mm/
│ └── all/yyyy/mm/
│
├── app/ # Dữ liệu phục vụ web/API
│ ├── listings/ # Dữ liệu tin đăng
│ ├── insights/ # Dữ liệu phân tích thị trường
│ └── recommendations/ # Dữ liệu đề xuất
│
├── temp/ # Dữ liệu tạm thời (được lưu trữ cục bộ, không ở HDFS)
│ ├── stage/ # Khu vực tạm cho quá trình xử lý
│ └── checkpoints/ # Checkpoints cho Spark Streaming
│ └── predictions/yyyy/mm/ # Kết quả dự đoán
│
├── app/ # Dữ liệu phục vụ web/API
│ ├── listings/ # Dữ liệu tin đăng
│ ├── insights/ # Dữ liệu phân tích thị trường
│ └── recommendations/ # Dữ liệu đề xuất
│
└── temp/ # Dữ liệu tạm thời
├── stage/ # Khu vực tạm cho quá trình xử lý
└── checkpoints/ # Checkpoints cho Spark Streaming

## Cấu trúc lưu trữ HDFS

Dữ liệu được lưu trữ trên HDFS với cấu trúc phân cấp theo nguồn, loại và thời gian. Định dạng lưu trữ mặc định là Parquet để tối ưu hiệu suất đọc/ghi và tiết kiệm không gian.

### Đặc điểm của lưu trữ HDFS

1. **Phân vùng theo thời gian**: Dữ liệu được phân tổ chức theo năm/tháng để dễ dàng quản lý và truy vấn
2. **Định dạng Parquet**: Sử dụng định dạng cột để tối ưu hiệu suất đọc và tiết kiệm không gian
3. **Metadata phong phú**: Lưu thông tin về nguồn, thời gian thu thập, và các thuộc tính khác

## Cơ chế Timeout cho Storage Service

Storage Service được thiết kế để hoạt động với luồng Airflow, với cơ chế timeout tự động khi không còn dữ liệu từ Kafka:

1. **Chế độ auto-stop**: Khi được kích hoạt bằng cờ `--once` hoặc biến môi trường `RUN_ONCE_MODE=true`, service sẽ tự động dừng sau khi không nhận được message nào từ Kafka trong một khoảng thời gian nhất định.

2. **Thời gian chờ**: Thời gian chờ mặc định là 600 giây (10 phút) và có thể được điều chỉnh bằng cờ `--idle-timeout` hoặc biến môi trường `IDLE_TIMEOUT`.

3. **Đảm bảo dữ liệu không bị mất**: Trước khi dừng, service sẽ flush tất cả các buffer còn lại để đảm bảo dữ liệu được lưu trữ đầy đủ.

4. **Tích hợp với Airflow**: Khi được chạy từ Airflow, cơ chế timeout giúp đảm bảo rằng DAG sẽ kết thúc khi đã xử lý hết tất cả dữ liệu từ crawler, tránh tình trạng treo vô thời hạn.

### Cách sử dụng

Để kích hoạt chế độ timeout khi chạy Storage Service:

```bash
python -m services.storage_service.main --storage-type hdfs --once --idle-timeout 600
```

Hoặc khi chạy từ Airflow (đã được cấu hình):

```yaml
environment:
    RUN_ONCE_MODE: 'true'
    IDLE_TIMEOUT: '600'
```
