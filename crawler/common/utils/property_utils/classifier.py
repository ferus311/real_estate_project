from typing import Dict, Any


def get_property_type(data: Dict[str, Any]) -> str:
    """
    Xác định loại bất động sản dựa trên dữ liệu

    Args:
        data: Dictionary chứa dữ liệu bất động sản

    Returns:
        str: Loại bất động sản (house, apartment, land, commercial, other)
    """
    # Nếu đã có trường data_type thì sử dụng luôn
    if "data_type" in data:
        property_type = data["data_type"].lower()
        # Chuyển đổi để phù hợp với các loại đã định nghĩa
        if property_type in [
            "house",
            "nhà",
            "nhà ở",
            "biệt thự",
            "villa",
            "nhà riêng",
            "nhà phố",
        ]:
            return "house"
        elif property_type in [
            "apartment",
            "chung cư",
            "căn hộ",
            "penthouse",
            "duplex",
        ]:
            return "apartment"
        elif property_type in ["land", "đất", "đất nền", "đất thổ cư"]:
            return "land"
        elif property_type in [
            "commercial",
            "văn phòng",
            "cửa hàng",
            "shophouse",
            "mặt bằng",
        ]:
            return "commercial"
        else:
            return "other"

    # Thử các trường phổ biến để xác định loại bất động sản
    property_type = ""
    for field in [
        "category",
        "property_type",
        "type",
        "category_name",
        "property_category",
    ]:
        if field in data and data[field]:
            property_type = str(data[field]).lower()
            break

    # Nếu không có trường data_type, thử phân tích từ title hoặc các trường khác
    title = data.get("title", "").lower()
    description = data.get("description", "").lower()

    # Tạo nội dung kết hợp để tìm kiếm từ khóa
    content = f"{title} {description} {property_type}"

    # Các từ khóa để nhận dạng loại BĐS
    house_keywords = [
        "nhà",
        "biệt thự",
        "villa",
        "nhà phố",
        "nhà riêng",
        "townhouse",
    ]
    apartment_keywords = ["chung cư", "căn hộ", "penthouse", "duplex", "apartment"]
    land_keywords = ["đất", "đất nền", "thổ cư", "lô đất", "nền đất"]
    commercial_keywords = [
        "văn phòng",
        "cửa hàng",
        "shophouse",
        "mặt bằng",
        "kinh doanh",
        "thương mại",
    ]

    # Kiểm tra từ khóa trong nội dung
    if any(keyword in content for keyword in house_keywords):
        return "house"
    elif any(keyword in content for keyword in apartment_keywords):
        return "apartment"
    elif any(keyword in content for keyword in land_keywords):
        return "land"
    elif any(keyword in content for keyword in commercial_keywords):
        return "commercial"

    # Mặc định là house
    return "house"
