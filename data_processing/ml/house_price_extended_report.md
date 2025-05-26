# Báo cáo mô hình dự đoán giá nhà mở rộng

## Thông tin dữ liệu
- Số dòng ban đầu: 14,270
- Số dòng sau khi lọc (không thiếu giá trị): 749
- Tỷ lệ dữ liệu giữ lại: 5.25%

## Hiệu suất mô hình
- R² score: 0.839 (mô hình giải thích được 83.9% phương sai của giá nhà)
- Mean Absolute Error: 4,326,060,667 VND

## Tầm quan trọng của các đặc trưng (từ cao đến thấp)
1. floor_area_ratio (tích số tầng và diện tích): 42.1%
2. log_price_per_m2 (logarit của giá/m²): 12.3%
3. price_per_m2 (giá/m²): 10.9%
4. log_area (logarit của diện tích): 9.6%
5. area (diện tích): 7.6%
6. facade_width (chiều rộng mặt tiền): 3.6%
7. area_per_bedroom (diện tích/phòng ngủ): 3.1%
8. longitude (kinh độ): 2.5%
9. latitude (vĩ độ): 2.4%
10. facade_width_ratio (tỷ lệ mặt tiền/diện tích): 1.8%
11. bathroom_per_bedroom (tỷ lệ phòng tắm/phòng ngủ): 1.1%
12. bathroom (số phòng tắm): 1.1%
13. bedroom (số phòng ngủ): 1.0%
14. floor_count (số tầng): 1.0%

## Nhận xét
- Mô hình mở rộng có hiệu suất tốt hơn đáng kể (R² = 0.839) so với mô hình cơ bản (R² = 0.615)
- Đặc trưng quan trọng nhất là floor_area_ratio (tích số tầng và diện tích), chiếm 42.1% tầm quan trọng
- Các đặc trưng về giá/m² và diện tích vẫn đóng vai trò quan trọng
- Vị trí địa lý (latitude, longitude) có ảnh hưởng đáng kể đến giá nhà
- Số phòng ngủ và phòng tắm có ảnh hưởng ít hơn so với các yếu tố khác
