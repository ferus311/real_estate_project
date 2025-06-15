// Constants for form field mappings
export const HOUSE_DIRECTION_OPTIONS = [
  { value: 1, label: '🌅 Đông', description: 'Hướng Đông' },
  { value: 2, label: '🌇 Tây', description: 'Hướng Tây' },
  { value: 3, label: '☀️ Nam', description: 'Hướng Nam' },
  { value: 4, label: '❄️ Bắc', description: 'Hướng Bắc' },
  { value: 5, label: '🌄 Đông Bắc', description: 'Hướng Đông Bắc' },
  { value: 6, label: '🏖️ Đông Nam', description: 'Hướng Đông Nam' },
  { value: 7, label: '🏔️ Tây Bắc', description: 'Hướng Tây Bắc' },
  { value: 8, label: '🌆 Tây Nam', description: 'Hướng Tây Nam' },
  { value: -1, label: '❓ Không rõ', description: 'Không xác định' },
];

export const LEGAL_STATUS_OPTIONS = [
  { value: 1, label: '✅ Đã có sổ', description: 'Đã có sổ đỏ/sổ hồng' },
  { value: 2, label: '⏳ Đang chờ sổ', description: 'Đang chờ cấp sổ' },
  { value: 4, label: '❌ Không có sổ', description: 'Chưa có sổ' },
  { value: 5, label: '📑 Sổ chung', description: 'Sổ chung/sổ tập thể' },
  { value: 6, label: '✍️ Giấy tờ viết tay', description: 'Giấy tờ viết tay' },
  { value: -1, label: '❓ Không rõ', description: 'Không xác định' },
];

export const INTERIOR_OPTIONS = [
  { value: 1, label: '💎 Cao cấp', description: 'Nội thất cao cấp' },
  { value: 2, label: '🏡 Đầy đủ', description: 'Nội thất đầy đủ' },
  { value: 3, label: '🔧 Cơ bản', description: 'Nội thất cơ bản' },
  { value: 4, label: '🏗️ Bàn giao thô', description: 'Bàn giao thô' },
  { value: -1, label: '❓ Không rõ', description: 'Không xác định' },
];

export const CATEGORY_OPTIONS = [
  { value: 1, label: '🏠 Nhà riêng', description: 'Nhà riêng/nhà phố' },
  { value: 2, label: '🏢 Chung cư', description: 'Căn hộ chung cư' },
  { value: 3, label: '🏛️ Biệt thự', description: 'Biệt thự' },
  { value: 4, label: '🏘️ Nhà liền kề', description: 'Nhà liền kề' },
  { value: 5, label: '🏬 Shophouse', description: 'Shophouse/nhà mặt tiền' },
  { value: 6, label: '🏞️ Đất nền', description: 'Đất nền' },
  { value: 7, label: '🏭 Kho xưởng', description: 'Kho bãi/xưởng' },
  { value: 8, label: '🏪 Văn phòng', description: 'Văn phòng' },
  { value: -1, label: '❓ Không rõ', description: 'Loại hình khác/không xác định' },
];

// Helper functions to get option details
export const getDirectionOption = (code: number) =>
  HOUSE_DIRECTION_OPTIONS.find(option => option.value === code);

export const getLegalStatusOption = (code: number) =>
  LEGAL_STATUS_OPTIONS.find(option => option.value === code);

export const getInteriorOption = (code: number) =>
  INTERIOR_OPTIONS.find(option => option.value === code);

export const getCategoryOption = (code: number) =>
  CATEGORY_OPTIONS.find(option => option.value === code);

// Default values
export const DEFAULT_CODES = {
  HOUSE_DIRECTION: 3, // Nam (most preferred in Vietnam)
  LEGAL_STATUS: 1, // Đã có sổ (most common)
  INTERIOR: 2, // Đầy đủ (most common)
  CATEGORY: -1, // Không rõ (unknown by default)
} as const;
