import pandas as pd
import numpy as np
import re
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.impute import KNNImputer
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
import warnings

warnings.filterwarnings("ignore")

# ----- Bước 1: Đọc dữ liệu gốc -----
print("📊 Đang đọc dữ liệu...")
df = pd.read_csv("raw_house.csv")

# ----- Bước 2: Bỏ cột không cần thiết -----
print("🧹 Đang làm sạch dữ liệu...")
df.drop(
    [
        "short_description",
        "url",
        "crawl_timestamp",
        "timestamp",
        "source",
        "title",
        "description",
    ],
    axis=1,
    inplace=True,
    errors="ignore",
)


# ----- Bước 3: Sửa nhầm giữa price và price_per_m2 -----
def correct_price_swap(row):
    price, ppm2 = str(row["price"]), str(row["price_per_m2"])
    if "/m²" in price or "/m2" in price:
        if "tỷ" in ppm2 or "triệu" in ppm2:
            return ppm2, price
    return price, ppm2


df[["price", "price_per_m2"]] = df.apply(
    correct_price_swap, axis=1, result_type="expand"
)


# ----- Bước 4: Hàm chuẩn hóa đơn giá -----
def parse_price(text):
    if pd.isna(text):
        return np.nan
    text = text.lower().replace(",", ".")
    nums = re.findall(r"\d+\.\d+|\d+", text)
    if not nums:
        return np.nan
    value = float(nums[0])
    if "tỷ" in text:
        return value * 1_000_000_000
    elif "triệu" in text:
        return value * 1_000_000
    return np.nan


# ----- Bước 5: Hàm chuẩn hóa diện tích -----
def parse_area(text):
    if pd.isna(text):
        return np.nan
    text = str(text).replace(",", ".")
    nums = re.findall(r"\d+\.\d+|\d+", text)
    return float(nums[0]) if nums else np.nan


# ----- Bước 6: Ước lượng giá nếu là "thỏa thuận" -----
def estimate_price(row):
    text = str(row["price"]).lower()
    if any(
        kw in text for kw in ["thỏa thuận", "thoả thuận", "liên hệ", "đang cập nhật"]
    ):
        # Kiểm tra xem có đủ thông tin không
        ppm2 = parse_price(row["price_per_m2"])
        area = parse_area(row["area"])

        # Nếu thiếu một trong hai thông tin thì bỏ qua
        if pd.isna(ppm2) or pd.isna(area):
            return np.nan

        # Kiểm tra đơn vị của price_per_m2
        ppm2_text = str(row["price_per_m2"]).lower()
        if "tỷ/m²" in ppm2_text or "ty/m²" in ppm2_text:
            return ppm2 * 1_000_000_000 * area
        elif "triệu/m²" in ppm2_text:
            return ppm2 * 1_000_000 * area
        else:
            return np.nan
    else:
        return parse_price(row["price"])


df["price_vnd"] = df.apply(estimate_price, axis=1)
df["price_per_m2_vnd"] = df["price_per_m2"].apply(parse_price)
df["area_m2"] = df["area"].apply(parse_area)

# Bỏ các hàng không có giá hoặc giá không hợp lệ
df = df[df["price_vnd"].notna() & (df["price_vnd"] > 0)]
df.drop(["price", "price_per_m2", "area"], axis=1, inplace=True)

# In thông tin về giá sau khi xử lý
print("\nThông tin về giá sau khi xử lý:")
print(f"Số lượng bản ghi còn lại: {len(df)}")
print("\nThống kê giá:")
print(df["price_vnd"].describe())
print("\nThống kê giá/m2:")
print(df["price_per_m2_vnd"].describe())


# ----- Bước 7: Tách tọa độ -----
def split_coords(coord):
    try:
        lat, lon = map(float, coord.split(","))
        return lat, lon
    except:
        return np.nan, np.nan


df[["latitude", "longitude"]] = df["coordinates"].apply(
    lambda x: pd.Series(split_coords(x))
)
df.drop("coordinates", axis=1, inplace=True)


# ----- Bước 8: Trích số từ chuỗi -----
def extract_number(text):
    if pd.isna(text):
        return np.nan
    nums = re.findall(r"\d+", str(text))
    return int(nums[0]) if nums else np.nan


for col in ["bedroom", "bathroom", "floor_count", "facade_width", "road_width"]:
    df[col] = df[col].apply(extract_number)

# ----- Bước 9: Mã hóa biến phân loại -----
print("🔤 Đang mã hóa biến phân loại...")

# Bỏ các cột không cần thiết
columns_to_drop = ["house_direction", "balcony_direction", "legal_status", "interior"]
df.drop(columns=columns_to_drop, inplace=True)

# ----- Bước 10: Feature Engineering -----
print("🔧 Đang tạo các biến mới...")

# 1. Tính toán các chỉ số tương đối
df["price_per_m2_ratio"] = df["price_per_m2_vnd"] / df["price_per_m2_vnd"].mean()
df["area_ratio"] = df["area_m2"] / df["area_m2"].mean()

# 2. Tạo các biến tương tác
df["area_per_room"] = df["area_m2"] / df["bedroom"].replace(0, np.nan)
df["comfort_index"] = (
    df["bedroom"].fillna(0) + df["bathroom"].fillna(0) + df["floor_count"].fillna(0)
)

# 3. Tính toán các tỷ lệ diện tích
df["facade_area_ratio"] = df["facade_width"] / df["area_m2"]  # Tỷ lệ mặt tiền/diện tích
df["road_facade_ratio"] = df["road_width"] / df["facade_width"]  # Tỷ lệ đường/mặt tiền


# 4. Tính khoảng cách đến trung tâm
def calculate_distance_to_center(lat, lon):
    center_lat, center_lon = 10.762622, 106.660172  # Tọa độ trung tâm HCM
    return np.sqrt((lat - center_lat) * 2 + (lon - center_lon) * 2)


df["distance_to_center"] = df.apply(
    lambda x: calculate_distance_to_center(x["latitude"], x["longitude"]), axis=1
)

# 5. Phân cụm khu vực
print("🗺️ Đang phân cụm khu vực...")
# Tạo một Series mới với cùng index như df
df["location_cluster"] = -1  # Giá trị mặc định

# Chỉ phân cụm cho các bản ghi có tọa độ hợp lệ
valid_coords = df[["latitude", "longitude"]].dropna()
if len(valid_coords) > 0:
    kmeans = KMeans(n_clusters=10, random_state=42)
    clusters = kmeans.fit_predict(valid_coords)
    # Cập nhật lại các giá trị cluster cho các bản ghi có tọa độ hợp lệ
    df.loc[valid_coords.index, "location_cluster"] = clusters

# ----- Bước 11: Xử lý ngoại lai -----
print("🔍 Đang xử lý ngoại lai...")


def remove_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    return df[~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]


numeric_cols = ["price_vnd", "area_m2", "price_per_m2_vnd", "distance_to_center"]
for col in numeric_cols:
    df = remove_outliers(df, col)

# ----- Bước 12: Xử lý dữ liệu thiếu -----
print("🔄 Đang xử lý dữ liệu thiếu...")
imputer = KNNImputer(n_neighbors=5)
df[numeric_cols] = imputer.fit_transform(df[numeric_cols])

# Tách các biến cần giữ nguyên giá trị gốc
original_cols = ["price_vnd", "price_per_m2_vnd", "area_m2", "latitude", "longitude"]
derived_cols = [col for col in numeric_cols if col not in original_cols]

# ----- Bước 13: Chuẩn hóa dữ liệu -----
print("📏 Đang chuẩn hóa dữ liệu...")
scaler = StandardScaler()
# Chỉ chuẩn hóa các biến phụ trợ
df[derived_cols] = scaler.fit_transform(df[derived_cols])

# Lưu các thông số chuẩn hóa
scaler_params = {"mean": scaler.mean_, "scale": scaler.scale_}
np.save("scaler_params.npy", scaler_params)

# ----- Bước 14: Lưu kết quả -----
print("💾 Đang lưu kết quả...")
df.to_csv("clean_train_data.csv", index=False)
print("✅ Dữ liệu đã xử lý xong và lưu tại: clean_train_data.csv")

# ----- Bước 15: In thông tin về dữ liệu -----
print("\n📊 Thông tin về dữ liệu đã xử lý:")
print(f"Số lượng bản ghi: {len(df)}")
print(f"Số lượng cột: {len(df.columns)}")
print("\nCác cột trong dữ liệu:")
for col in df.columns:
    print(f"- {col}")

# In ra một số ví dụ về giá trị
print("\nVí dụ về giá trị:")
print("\nCác biến gốc:")
for col in original_cols:
    print(f"\n{col}:")
    print(df[col].head())

print("\nCác biến đã chuẩn hóa:")
for col in derived_cols:
    print(f"\n{col}:")
    print(df[col].head())
