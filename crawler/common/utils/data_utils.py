import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def determine_data_type(message: Dict[str, Any]) -> str:
    """
    Xác định loại dữ liệu từ message một cách nhất quán

    Args:
        message: Message từ Kafka hoặc dữ liệu khác

    Returns:
        str: Loại dữ liệu (list, detail, api)
    """
    # 1. Nếu có trường data_type, sử dụng nó
    if "data_type" in message:
        return message["data_type"]

    # 2. Kiểm tra các trường đặc trưng của từng loại dữ liệu
    # Đặc trưng của dữ liệu chi tiết (detail) là có mô tả chi tiết
    if "description" in message and message.get("description"):
        return "detail"

    # Đặc trưng của dữ liệu danh sách (list) là có danh sách URLs hoặc source_type chứa "list"
    if any(key in message for key in ["links", "urls", "list_data"]):
        return "list"
    if message.get("source_type", "").lower().find("list") >= 0:
        return "list"

    # Đặc trưng của dữ liệu API là từ nguồn API hoặc từ Chotot (vì Chotot chủ yếu dùng API)
    if message.get("source_type", "").lower().find("api") >= 0:
        return "api"
    if message.get("source", "").lower() == "chotot":
        return "api"

    # 3. Dựa vào cấu trúc dữ liệu để phân loại tiếp
    if "items" in message and isinstance(message["items"], list):
        return "list"

    # 4. Kiểm tra các thuộc tính đặc trưng của dữ liệu chi tiết
    if any(
        key in message for key in ["price", "area", "address", "bedroom", "bathroom"]
    ):
        return "detail"

    # Nếu không xác định được, mặc định là detail
    logger.warning(
        f"Could not determine data_type from message, defaulting to 'detail'"
    )
    return "detail"


def ensure_data_metadata(
    message: Dict[str, Any], data_type: Optional[str] = None
) -> Dict[str, Any]:
    """
    Đảm bảo message có các trường metadata cần thiết

    Args:
        message: Message cần xử lý
        data_type: Loại dữ liệu đã xác định (nếu có)

    Returns:
        Dict[str, Any]: Message đã được bổ sung metadata
    """
    import time

    # Clone message để không thay đổi đối tượng gốc
    result = message.copy()

    # Thêm timestamp nếu chưa có
    if "crawl_time" not in result:
        result["crawl_time"] = int(time.time())

    # Xác định và thêm data_type nếu chưa có
    if "data_type" not in result:
        if data_type:
            result["data_type"] = data_type
        else:
            result["data_type"] = determine_data_type(result)

    return result


def is_list_data(message: Dict[str, Any]) -> bool:
    """
    Kiểm tra xem dữ liệu có phải là dữ liệu danh sách không

    Args:
        message: Message cần kiểm tra

    Returns:
        bool: True nếu là dữ liệu danh sách, False nếu không
    """
    # Kiểm tra data_type trước
    if message.get("data_type") == "list":
        return True

    # Kiểm tra các đặc điểm của dữ liệu danh sách
    if any(key in message for key in ["links", "urls", "list_data"]):
        return True
    if "items" in message and isinstance(message["items"], list):
        return True
    if message.get("source_type", "").lower().find("list") >= 0:
        return True

    return False


def normalize_list_data(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Chuẩn hóa dữ liệu danh sách để đảm bảo tính nhất quán

    Args:
        message: Message cần chuẩn hóa

    Returns:
        Dict[str, Any]: Message đã được chuẩn hóa
    """
    if not is_list_data(message):
        return message

    # Clone message để không thay đổi đối tượng gốc
    result = message.copy()

    # Đảm bảo có trường data_type
    result["data_type"] = "list"

    # Chuẩn hóa các trường chứa danh sách
    urls = []

    # Thu thập URLs từ các trường khác nhau
    if "urls" in result and isinstance(result["urls"], list):
        urls.extend(result["urls"])
    if "links" in result and isinstance(result["links"], list):
        urls.extend(result["links"])
    if "list_data" in result and isinstance(result["list_data"], list):
        for item in result["list_data"]:
            if isinstance(item, dict) and "url" in item:
                urls.append(item["url"])
    if "items" in result and isinstance(result["items"], list):
        for item in result["items"]:
            if isinstance(item, dict) and "url" in item:
                urls.append(item["url"])

    # Loại bỏ trùng lặp và None
    urls = [url for url in urls if url]
    urls = list(dict.fromkeys(urls))  # Loại bỏ trùng lặp giữ nguyên thứ tự

    # Cập nhật trường urls với danh sách đã chuẩn hóa
    result["urls"] = urls

    return result
