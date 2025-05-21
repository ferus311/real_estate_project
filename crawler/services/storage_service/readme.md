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

Lợi ích của kiến trúc này
Tên thư mục trực quan, có ý nghĩa:

raw: Dữ liệu thô từ nguồn
processed: Dữ liệu đã qua xử lý
ml: Liên quan đến Machine Learning
app: Phục vụ cho ứng dụng
Cân bằng giữa tổ chức và truy cập:

Phân vùng đến cấp tháng, không quá chi tiết đến ngày/giờ
Phân chia logic theo nguồn, loại dữ liệu và quy trình xử lý
Linh hoạt trong quản lý vòng đời dữ liệu:

Dễ dàng áp dụng chính sách lưu trữ hoặc archive theo thời gian
Có thể định kỳ di chuyển dữ liệu cũ sang storage rẻ hơn
Tương thích với định dạng đa dạng:

Raw: JSON/Avro cho tốc độ ghi và debugging
Processed: Parquet cho hiệu suất truy vấn và phân tích
ML: Định dạng tương thích với framework ML
Hỗ trợ cả xử lý batch và streaming:

Thư mục temp/stage cho xử lý trung gian
Thư mục temp/checkpoints cho Spark Streaming
