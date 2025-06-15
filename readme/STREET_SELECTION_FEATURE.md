# Street Selection Feature Documentation

## Overview

The address selection system now supports comprehensive Vietnam address data including:

-   **Province** (Tỉnh/Thành phố) - Required
-   **District** (Quận/Huyện) - Required
-   **Ward** (Phường/Xã) - Optional
-   **Street** (Đường) - Optional but recommended for better geocoding

## New Features Added

### 🛣️ **Street Selection Options:**

1. **Dropdown Street Selection (`street_id`):**

    - Choose from predefined streets in the selected district
    - Includes street prefix (e.g., "Đường Nguyễn Huệ")
    - Searchable dropdown with filtering
    - Auto-clears when changing district

2. **Custom Address Input (`street`):**
    - Free text input for specific house numbers, alleys, etc.
    - Placeholder: "Số nhà, ngõ..."
    - Supports detailed addresses not in the dropdown

### 🔄 **Smart Address Handling:**

-   **Mutual Exclusivity:** Selecting from dropdown clears custom input and vice versa
-   **Cascading Resets:** Changing province/district resets all dependent fields
-   **Intelligent Geocoding:** Uses either dropdown street or custom input for address conversion

### 📊 **Data Structure:**

```typescript
interface Street {
    id: string;
    name: string;
    prefix?: string; // e.g., "Đường", "Phố"
}
```

### 🎯 **Usage Flow:**

1. **Method 1 - Dropdown Selection:**

    - Select Province → District → Ward (optional) → Street from dropdown
    - Click 📍 to convert to coordinates

2. **Method 2 - Custom Input:**

    - Select Province → District → Ward (optional)
    - Type specific address in custom field
    - Click 📍 to convert to coordinates

3. **Method 3 - Hybrid:**
    - Select Province → District → Ward → Street from dropdown
    - Add specific details in custom field
    - System combines both for geocoding

### 🏗️ **Technical Implementation:**

#### Hook Functions Added:

-   `getStreetOptions(provinceId, districtId)` - Get street dropdown options
-   `getStreetById(provinceId, districtId, streetId)` - Get street details
-   `getFullAddress()` - Updated to include street parameter

#### State Management:

```typescript
const [selectedStreet, setSelectedStreet] = useState<string>('');
```

#### Form Fields:

-   `street_id` - For dropdown selection
-   `street` - For custom input

### 🎨 **UI/UX Improvements:**

1. **Responsive Layout:** 5-column layout (Province, District, Ward, Street Dropdown, Custom Input)
2. **Clear Labels:** Distinct labels for dropdown vs custom input
3. **Address Summary:** Shows both selected street and custom input
4. **Smart Clearing:** Auto-clears conflicting fields
5. **Better Geocoding:** More precise address conversion

### 📍 **Geocoding Logic:**

```typescript
// Priority: Custom input > Dropdown selection
const finalStreet = customStreet || streetName;
const fullAddress = buildFullAddress(finalStreet, ward, district, province);
```

### ✅ **Benefits:**

1. **Accuracy:** More precise geocoding with official street names
2. **Flexibility:** Support both structured and free-form address input
3. **User Experience:** Multiple ways to input address based on user preference
4. **Data Quality:** Leverages official Vietnam address database
5. **Error Reduction:** Dropdown prevents spelling mistakes

This enhancement provides a comprehensive address selection system that balances structure with flexibility, improving both user experience and geocoding accuracy! 🚀
