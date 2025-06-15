# Auto-Geocoding & Enhanced Form Features

## 🔄 **Auto-Geocoding System**

### **Automatic Address to Coordinates Conversion:**

-   **Debounced API calls** - Tránh spam Google Geocoding API
-   **Smart triggering** - Tự động chuyển đổi khi user thay đổi địa chỉ
-   **Silent mode** - Không hiện notification khi auto-geocode

### **Trigger Conditions:**

```typescript
- Province change → 2 second delay
- District change → 1.5 second delay
- Ward change → 1 second delay
- Street dropdown → 0.8 second delay
- Custom street typing → 1.5 second delay
```

### **Smart Logic:**

-   Only triggers when có đủ Province + District
-   Debounce timer cancels previous requests
-   Auto mode không hiện notification lỗi/thành công
-   Manual mode (click 📍) vẫn hiện notifications

## 🎯 **Enhanced Form Options**

### **Constants-Based Dropdowns:**

Thay thế hard-coded values bằng constants từ `formOptions.ts`:

```typescript
// Before (Hard-coded)
<Option value={1}>🌅 Đông</Option>;

// After (Constants-based)
{
    HOUSE_DIRECTION_OPTIONS.map((option) => (
        <Option
            key={option.value}
            value={option.value}
            title={option.description}
        >
            {option.label}
        </Option>
    ));
}
```

### **Added Category Field:**

-   **🏘️ Loại hình** - Property category selection
-   **Optional field** với allowClear
-   **Default value**: -1 (Không rõ)
-   **Options**: Nhà riêng, Chung cư, Biệt thự, etc.

### **Enhanced Tooltips:**

-   Mỗi option có `title` attribute với description
-   Hover để xem thông tin chi tiết
-   Consistent với UX pattern

## 🛠️ **Technical Implementation**

### **Debounce System:**

```typescript
const [geocodingTimer, setGeocodingTimer] = useState<NodeJS.Timeout | null>(
    null
);

const triggerAutoGeocode = useCallback(
    (delay = 1500) => {
        if (geocodingTimer) clearTimeout(geocodingTimer);

        const timer = setTimeout(() => {
            handleAddressToCoordinates(true); // Auto mode
        }, delay);

        setGeocodingTimer(timer);
    },
    [geocodingTimer]
);
```

### **Enhanced Geocoding Function:**

```typescript
const handleAddressToCoordinates = async (auto = false) => {
    // auto = true: No notifications, silent operation
    // auto = false: Show success/error notifications
};
```

### **Smart State Management:**

-   Auto-clears dependent fields when parent changes
-   Triggers geocoding after state updates complete
-   Cancels previous timers to prevent race conditions

## 🎨 **User Experience Improvements**

### **Seamless Workflow:**

1. User chọn Province → Auto-geocode sau 2s
2. User chọn District → Auto-geocode sau 1.5s
3. User chọn Ward/Street → Auto-geocode ngay lập tức
4. Map tự động update với tọa độ mới

### **Feedback System:**

-   **Loading indicators** khi geocoding
-   **Silent auto-geocoding** không làm phiền user
-   **Manual geocoding** vẫn có full notifications
-   **Error handling** graceful cho failed requests

### **Smart Defaults:**

-   Category defaults to `-1` (Không rõ)
-   Other codes use meaningful defaults từ constants
-   Form validation vẫn enforce required fields

## 📊 **Constants Mapping**

All \_code fields now use constants:

```typescript
DEFAULT_CODES = {
    HOUSE_DIRECTION: 3, // Nam (most preferred)
    LEGAL_STATUS: 1, // Đã có sổ (most common)
    INTERIOR: 2, // Đầy đủ (most common)
    CATEGORY: -1, // Không rõ (unknown by default)
};
```

Each field có option `-1` cho "Không rõ" để handle unknown cases.

## 🚀 **Benefits**

1. **Better UX**: Tự động chuyển đổi địa chỉ → tọa độ
2. **Reduced friction**: Không cần manual click 📍 button
3. **Smart geocoding**: Debounced để tránh spam API
4. **Consistent data**: Constants đảm bảo mapping chính xác
5. **Enhanced tooltips**: Better user guidance
6. **Flexible categories**: Support nhiều loại BDS khác nhau

System bây giờ intelligent hơn và user-friendly hơn nhiều! 🎯
