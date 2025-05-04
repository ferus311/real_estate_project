import os
import json

def save_checkpoint(file_path, page_number, success=True):
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    data = load_checkpoint(file_path)
    data[str(page_number)] = success

    # Ghi dữ liệu vào file (tạo mới nếu chưa có)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

def load_checkpoint(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    return {}
