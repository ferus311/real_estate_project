
import pandas as pd
import numpy as np
import joblib

# Load mô hình đã lưu
model_data = joblib.load('house_price_extended_model.joblib')
model = model_data['model']
scaler = model_data['scaler']
features = model_data['features']

# Hàm dự đoán giá nhà với các đặc trưng mở rộng
def predict_house_price(area, price_per_m2, bedroom, bathroom, 
                        latitude, longitude, floor_count, facade_width):
    # Tạo các đặc trưng phái sinh
    area_per_bedroom = area / bedroom if bedroom > 0 else np.nan
    bathroom_per_bedroom = bathroom / bedroom if bedroom > 0 else np.nan
    log_area = np.log1p(area)
    log_price_per_m2 = np.log1p(price_per_m2)
    facade_width_ratio = facade_width / area
    floor_area_ratio = floor_count * area

    # Tạo mảng đặc trưng theo thứ tự đúng
    features_array = np.array([
        area, price_per_m2, bedroom, bathroom,
        latitude, longitude, floor_count, facade_width,
        area_per_bedroom, bathroom_per_bedroom,
        log_area, log_price_per_m2,
        facade_width_ratio, floor_area_ratio
    ]).reshape(1, -1)

    # Chuẩn hóa đặc trưng
    features_scaled = scaler.transform(features_array)

    # Dự đoán giá
    predicted_price = model.predict(features_scaled)[0]

    return predicted_price

# Ví dụ sử dụng
if __name__ == "__main__":
    # Ví dụ 1: Căn hộ 80m2, giá 75 triệu/m2, 2 phòng ngủ, 2 phòng tắm
    # Vị trí: latitude=10.7769, longitude=106.7009 (TPHCM)
    # 15 tầng, mặt tiền 5m
    price1 = predict_house_price(
        area=80, price_per_m2=75000000, bedroom=2, bathroom=2,
        latitude=10.7769, longitude=106.7009, floor_count=15, facade_width=5
    )
    print(f"Giá dự đoán cho căn hộ 80m2, 2PN, 2PT tại TPHCM: {price1:,.0f} VND")

    # Ví dụ 2: Nhà phố 120m2, giá 100 triệu/m2, 3 phòng ngủ, 3 phòng tắm
    # Vị trí: latitude=21.0285, longitude=105.8542 (Hà Nội)
    # 4 tầng, mặt tiền 8m
    price2 = predict_house_price(
        area=120, price_per_m2=100000000, bedroom=3, bathroom=3,
        latitude=21.0285, longitude=105.8542, floor_count=4, facade_width=8
    )
    print(f"Giá dự đoán cho nhà phố 120m2, 3PN, 3PT tại Hà Nội: {price2:,.0f} VND")

    # Ví dụ 3: Biệt thự 200m2, giá 150 triệu/m2, 4 phòng ngủ, 4 phòng tắm
    # Vị trí: latitude=16.0544, longitude=108.2022 (Đà Nẵng)
    # 3 tầng, mặt tiền 12m
    price3 = predict_house_price(
        area=200, price_per_m2=150000000, bedroom=4, bathroom=4,
        latitude=16.0544, longitude=108.2022, floor_count=3, facade_width=12
    )
    print(f"Giá dự đoán cho biệt thự 200m2, 4PN, 4PT tại Đà Nẵng: {price3:,.0f} VND")
