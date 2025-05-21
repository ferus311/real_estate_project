# Hướng dẫn lưu trữ HDFS

Tài liệu này mô tả các quy tắc và định dạng lưu trữ dữ liệu trên HDFS cho hệ thống bất động sản.

## Cấu trúc thư mục

```
/realestate/
│
├── raw/                          # Dữ liệu thô từ các nguồn
│   ├── batdongsan/
│   │   ├── list/yyyy/mm/         # Phân vùng theo năm/tháng
│   │   └── detail/yyyy/mm/
│   └── chotot/
│       └── api/yyyy/mm/
│
├── processed/                    # Dữ liệu đã xử lý
│   ├── cleaned/                  # Dữ liệu đã làm sạch
│   │   └── yyyy/mm/
│   ├── integrated/              # Dữ liệu đã tích hợp từ nhiều nguồn
│   │   └── yyyy/mm/
│   └── analytics/               # Dữ liệu tổng hợp cho phân tích
│       ├── price_trends/yyyy/mm/
│       ├── region_stats/yyyy/mm/
│       └── property_metrics/yyyy/mm/
│
├── ml/                          # Dữ liệu và mô hình ML
│   ├── features/yyyy/mm/        # Đặc trưng cho ML
│   ├── training/yyyy/mm/        # Dữ liệu huấn luyện
│   ├── models/                  # Các phiên bản mô hình
│   │   ├── price_model_v1/
│   │   └── price_model_v2/
│   └── predictions/yyyy/mm/     # Kết quả dự đoán
│
├── app/                         # Dữ liệu phục vụ web/API
│   ├── listings/                # Dữ liệu tin đăng
│   ├── insights/                # Dữ liệu phân tích thị trường
│   └── recommendations/         # Dữ liệu đề xuất
│
└── temp/                        # Dữ liệu tạm thời
    ├── stage/                   # Khu vực tạm cho quá trình xử lý
    └── checkpoints/             # Checkpoints cho Spark Streaming
```

## Định dạng lưu trữ

Hệ thống hỗ trợ các định dạng lưu trữ sau:

### 1. JSON (Mặc định cho dữ liệu thô)

-   **Sử dụng cho**: Dữ liệu thô từ Kafka
-   **Ưu điểm**: Dễ đọc, linh hoạt với thay đổi schema, dễ debug
-   **Đường dẫn**: `/realestate/raw/*`
-   **Mặc định**: Được chọn tự động cho tất cả dữ liệu thô (raw)

### 2. Avro

-   **Sử dụng cho**: Dữ liệu thô cần hiệu năng cao
-   **Ưu điểm**: Hiệu quả hơn JSON về không gian và tốc độ
-   **Đường dẫn**: `/realestate/raw/*` (chỉ khi chỉ định rõ)

### 3. Parquet

-   **Sử dụng cho**: Dữ liệu đã xử lý, dữ liệu cho phân tích
-   **Ưu điểm**: Hiệu suất truy vấn cao, nén hiệu quả
-   **Đường dẫn**: `/realestate/processed/*`, `/realestate/ml/*`

## Quy tắc đặt tên file

Mỗi file lưu trữ có định dạng tên như sau:

```
{source}_{data_type}_{timestamp}_{hostname}.{format}
```

Ví dụ:

```
batdongsan_detail_20250520_123045_worker1.json
chotot_api_20250520_123045_worker2.avro
```

## Tự động chọn định dạng

Hệ thống sẽ tự động chọn định dạng tối ưu cho từng loại dữ liệu:

-   Raw data (từ Kafka): **JSON** (mặc định)
-   Dữ liệu đã xử lý: Parquet
-   Dữ liệu ML: Parquet

Có thể ghi đè định dạng tự động bằng cách thiết lập biến môi trường `FILE_FORMAT`. Nhưng khuyến nghị sử dụng JSON cho dữ liệu thô để dễ dàng debug và xử lý lỗi.
