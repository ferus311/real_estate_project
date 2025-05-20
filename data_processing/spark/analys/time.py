import datetime

# Giá trị list_time từ dữ liệu (đơn vị: milliseconds)
list_time_ms = 1747094400

# Chuyển sang giây
# list_time_s = list_time_ms / 1000

# Chuyển thành datetime
dt = datetime.datetime.fromtimestamp(list_time_ms)

# Hiển thị kết quả
print("📌 list_time (timestamp):", list_time_ms)
print("🕒 Giờ hệ thống hiện tại  :", datetime.datetime.now())
print("📅 Thời gian từ list_time:", dt.strftime("%Y-%m-%d %H:%M:%S"))


rawl_timestamp = int(datetime.datetime.now().timestamp())
print(rawl_timestamp)

