import os
import json
import time
from datetime import datetime


def save_checkpoint(file_path, page_number, success=True):
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    data = load_checkpoint(file_path)
    data[str(page_number)] = success

    # Ghi dữ liệu vào file (tạo mới nếu chưa có)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def save_checkpoint_with_timestamp(file_path, page_number, success=True):
    """Lưu checkpoint với timestamp để hỗ trợ force crawl"""
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    data = load_checkpoint(file_path)
    data[str(page_number)] = {
        "success": success,
        "timestamp": time.time(),
        "datetime": datetime.now().isoformat(),
    }

    # Ghi dữ liệu vào file (tạo mới nếu chưa có)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def load_checkpoint(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    return {}


def should_force_crawl(file_path, page_number, force_crawl_interval_hours):
    """
    Kiểm tra xem có nên force crawl trang này không dựa trên khoảng thời gian đã cấu hình

    Args:
        file_path: Đường dẫn đến file checkpoint
        page_number: Số trang cần kiểm tra
        force_crawl_interval_hours: Số giờ sau đó cần crawl lại

    Returns:
        bool: True nếu trang cần được crawl lại, False nếu không
    """
    if force_crawl_interval_hours <= 0:
        return False

    data = load_checkpoint(file_path)
    page_data = data.get(str(page_number))

    # Nếu chưa crawl hoặc không có timestamp, cần crawl
    if not page_data or not isinstance(page_data, dict) or "timestamp" not in page_data:
        return True

    # Tính khoảng thời gian đã trôi qua kể từ lần cuối crawl
    last_crawl_time = page_data["timestamp"]
    current_time = time.time()
    hours_since_last_crawl = (current_time - last_crawl_time) / 3600

    # Nếu đã quá thời gian force_crawl_interval_hours, cần crawl lại
    return hours_since_last_crawl >= force_crawl_interval_hours


def load_file_checkpoint(path):
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return load_checkpoint(path)


def mark_file_done(filename, path):
    # os.makedirs(os.path.dirname(path), exist_ok=True)
    save_checkpoint(path, filename, success=True)


def is_file_done(filename, checkpoint):
    checkpoint_data = checkpoint.get(filename)
    if not checkpoint_data:
        return False

    # Hỗ trợ cả định dạng legacy và mới (có timestamp)
    if isinstance(checkpoint_data, bool):
        return checkpoint_data
    elif isinstance(checkpoint_data, dict):
        return checkpoint_data.get("success", False)

    return False
