import os
import pandas as pd
import glob

# Đường dẫn đến thư mục chứa các file .parquet
parquet_dir = './parquet_files'  # ← thay bằng đường dẫn của bạn
output_csv = './merged_output.csv'

# Tìm tất cả file .parquet trong thư mục
parquet_files = glob.glob(os.path.join(parquet_dir, '*.parquet'))

# Danh sách chứa các DataFrame
dfs = []

for file in parquet_files:
    try:
        df = pd.read_parquet(file)
        dfs.append(df)
        print(f"Đã đọc: {file} với {len(df)} dòng")
    except Exception as e:
        print(f"Lỗi khi đọc file {file}: {e}")

# Gộp tất cả lại thành một DataFrame
if dfs:
    merged_df = pd.concat(dfs, ignore_index=True)

    # (Tùy chọn) chuyển toàn bộ cột sang string nếu cần
    # merged_df = merged_df.astype("string")

    # Ghi ra 1 file CSV
    merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"Đã lưu CSV: {output_csv} với {len(merged_df)} dòng")
else:
    print("Không có file nào được đọc thành công.")
