"""
Common field mappings for real estate data standardization
Used by both Chotot and Batdongsan transformation processes
"""

# ===================== MAIN MAPPING DICTIONARIES =====================

HOUSE_DIRECTION_MAPPING = {
    "Đông": 1,
    "Tây": 2,
    "Nam": 3,
    "Bắc": 4,
    "Đông Bắc": 5,
    "Đông Nam": 6,
    "Tây Bắc": 7,
    "Tây Nam": 8,
}

LEGAL_STATUS_MAPPING = {
    "Đã có sổ": 1,
    "Đang chờ sổ": 2,
    "Không có sổ": 4,
    "Sổ chung / Vi bằng": 5,
    "Giấy tờ viết tay": 6,
}

INTERIOR_MAPPING = {"Cao cấp": 1, "Đầy đủ": 2, "Cơ bản": 3, "Bàn giao thô": 4}

HOUSE_TYPE_MAPPING = {
    "Nhà mặt phố / Mặt tiền": 1,
    "Biệt thự": 2,
    "Nhà trong ngõ / hẻm": 3,
    "Nhà phố liền kề": 4,
}

# ===================== REVERSE MAPPINGS FOR CHOTOT =====================

REVERSE_HOUSE_DIRECTION_MAPPING = {v: k for k, v in HOUSE_DIRECTION_MAPPING.items()}
REVERSE_LEGAL_STATUS_MAPPING = {v: k for k, v in LEGAL_STATUS_MAPPING.items()}
REVERSE_INTERIOR_MAPPING = {v: k for k, v in INTERIOR_MAPPING.items()}
REVERSE_HOUSE_TYPE_MAPPING = {v: k for k, v in HOUSE_TYPE_MAPPING.items()}

# ===================== CONSTANTS =====================

UNKNOWN_TEXT = "unknown"
UNKNOWN_ID = -1
UNKNOWN_ID = -1
