from dataclasses import dataclass
from typing import Optional


@dataclass
class HouseDetailItem:
    # Trường bắt buộc (cốt lõi, có ở cả hai trang)
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[str] = None
    area: Optional[str] = None
    price_per_m2: Optional[str] = None
    bedroom: Optional[str] = None
    bathroom: Optional[str] = None
    legal_status: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    location: Optional[str] = None

    # Trường tùy chọn (đặc thù của từng trang)
    house_direction: Optional[str] = None
    # balcony_direction: Optional[str] = None
    facade_width: Optional[str] = None
    road_width: Optional[str] = None
    floor_count: Optional[str] = None
    interior: Optional[str] = None
    # deposit: Optional[str] = None
    seller_info: Optional[dict] = None  # Lưu thông tin người bán của chotot

    # Các trường khác nếu cần
    source: Optional[str] = None  # Để biết dữ liệu từ batdongsan hay chotot
    posted_date: Optional[str] = None  # Ngày đăng tin theo giây epoch
