# REAL ESTATE WEBSITE ARCHITECTURE

# Sử dụng Gold Schema Data

## 🏗️ TECH STACK ĐƯỜNG NGỢI

### Frontend (Client)

-   **React.js** + TypeScript
-   **Material-UI** hoặc **Tailwind CSS** cho giao diện
-   **React Router** cho navigation
-   **React Query** cho data fetching
-   **Leaflet/Google Maps** cho bản đồ
-   **Chart.js** cho biểu đồ thống kê

### Backend (API Server)

-   **Node.js** + **Express.js** hoặc **FastAPI (Python)**
-   **PostgreSQL** cho database
-   **Redis** cho caching
-   **JWT** cho authentication

### Database

-   **PostgreSQL** với schema tối ưu từ Gold data
-   **Connection pooling** cho performance

## 📱 CHỨC NĂNG WEBSITE

### 1. TRANG CHỦ (Homepage)

```
┌─────────────────────────────────────────┐
│           HEADER + NAVIGATION           │
├─────────────────────────────────────────┤
│              HERO SECTION               │
│        [Tìm Kiếm Nhanh Form]           │
├─────────────────────────────────────────┤
│           THỐNG KÊ TỔNG QUAN            │
│  [Tổng tin] [Giá TB] [Khu vực phổ biến] │
├─────────────────────────────────────────┤
│            TIN ĐĂNG MỚI NHẤT            │
│        [Grid 3x4 properties]           │
├─────────────────────────────────────────┤
│            TÌM KIẾM THEO VÙNG           │
│      [Map với markers + filters]       │
└─────────────────────────────────────────┘
```

### 2. TRANG TÌM KIẾM (Search/Listing)

```
┌─────────────────────────────────────────┐
│              SEARCH FILTERS             │
│ [Tỉnh] [Quận] [Giá] [Diện tích] [Phòng] │
│ [Loại nhà] [Hướng] [Pháp lý] [Nội thất] │
├─────────────────────────────────────────┤
│           SORT & VIEW OPTIONS           │
│ [Sắp xếp] [Grid/List view] [Kết quả/trang]│
├─────────────────────────────────────────┤
│              RESULTS GRID               │
│ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐        │
│ │Img │ │ Img │ │ Img │ │ Img │        │
│ │Title│ │Title│ │Title│ │Title│        │
│ │Price│ │Price│ │Price│ │Price│        │
│ └─────┘ └─────┘ └─────┘ └─────┘        │
├─────────────────────────────────────────┤
│              PAGINATION                 │
│        [← Prev] [1,2,3...] [Next →]     │
└─────────────────────────────────────────┘
```

### 3. TRANG CHI TIẾT (Property Detail)

```
┌─────────────────────────────────────────┐
│              BREADCRUMB                 │
│        Trang chủ > HCM > Q1 > ...       │
├─────────────────────────────────────────┤
│            IMAGE GALLERY                │
│      [Carousel ảnh + thumbnails]        │
├─────────────────────────────────────────┤
│   THÔNG TIN CHÍNH      │   GIÁ & LIÊN HỆ │
│ • Tiêu đề              │ • Giá: X tỷ      │
│ • Diện tích: X m²      │ • Giá/m²: Y     │
│ • Phòng: X ngủ, Y tắm  │ • [Liên hệ btn] │
│ • Địa chỉ đầy đủ       │ • [Yêu thích]   │
├─────────────────────────────────────────┤
│              CHI TIẾT NHỆN              │
│ [Tabs: Mô tả | Đặc điểm | Vị trí]      │
├─────────────────────────────────────────┤
│                BẢN ĐỒ                  │
│        [Map với marker vị trí]          │
├─────────────────────────────────────────┤
│            TIN TƯƠNG TỰ                 │
│       [4-6 properties cùng khu vực]     │
└─────────────────────────────────────────┘
```

### 4. TRANG BẢN ĐỒ (Map View)

```
┌─────────────────────────────────────────┐
│               FILTERS BAR               │
│ [Tỉnh] [Quận] [Giá] [Loại] [Toggle]   │
├─────────────────────────────────────────┤
│                                         │
│              FULL MAP                   │
│         [Interactive markers]           │
│                                         │
│  ┌─────────────────┐                   │
│  │   POPUP CARD    │                   │
│  │  [Img] [Title]  │                   │
│  │  [Price] [Area] │                   │
│  │  [View Detail]  │                   │
│  └─────────────────┘                   │
└─────────────────────────────────────────┘
```

### 5. TRANG THỐNG KÊ (Analytics)

```
┌─────────────────────────────────────────┐
│              FILTER PANEL               │
│      [Chọn tỉnh] [Thời gian]           │
├─────────────────────────────────────────┤
│             OVERVIEW CARDS              │
│ [Tổng tin] [Giá TB] [Diện tích TB]     │
├─────────────────────────────────────────┤
│               BIỂU ĐỒ                   │
│ ┌─────────────────────────────────────┐ │
│ │        Giá trung bình theo quận      │ │
│ │         [Bar chart]                │ │
│ └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│ ┌─────────────────────────────────────┐ │
│ │      Phân bố theo khoảng giá        │ │
│ │         [Pie chart]                │ │
│ └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│              BẢNG THỐNG KÊ              │
│    [Table: Quận | Số tin | Giá TB]     │
└─────────────────────────────────────────┘
```

## 🔍 TÍN NĂNG TÌM KIẾM NÂNG CAO

### Search Parameters (từ Gold Schema)

```javascript
const searchParams = {
    // Location
    province: string,
    district: string,
    ward: string,

    // Price & Area
    priceMin: number,
    priceMax: number,
    areaMin: number,
    areaMax: number,

    // Property Details
    bedrooms: number,
    bathrooms: number,
    floors: number,
    dataType: 'sell' | 'rent',

    // Advanced Features
    houseType: string,
    houseDirection: string,
    legalStatus: string,
    interior: string,

    // Dimensions
    widthMin: number,
    lengthMin: number,
    facadeWidthMin: number,
    roadWidthMin: number,

    // Sorting
    sortBy: 'price' | 'area' | 'pricePerM2' | 'posted_date',
    sortOrder: 'asc' | 'desc',
};
```

## 📊 API ENDPOINTS

### Core APIs

```
GET  /api/properties              - List properties with filters
GET  /api/properties/:id          - Get property detail
GET  /api/properties/search       - Advanced search
GET  /api/properties/map          - Get properties for map view
GET  /api/properties/similar/:id  - Get similar properties

GET  /api/locations/provinces     - Get all provinces
GET  /api/locations/districts/:provinceId - Get districts
GET  /api/locations/wards/:districtId     - Get wards

GET  /api/analytics/overview      - Dashboard overview stats
GET  /api/analytics/price-trends  - Price trends by location
GET  /api/analytics/distribution  - Property distribution stats
```

## 🗺️ MAP INTEGRATION

### Features từ Gold Schema

-   **Clustered markers** theo tọa độ lat/lng
-   **Filter trên map** theo giá, loại, diện tích
-   **Heatmap** hiển thị mật độ bất động sản
-   **Draw polygon** tìm kiếm theo vùng tự chọn
-   **Street view integration**
-   **Directions** đến property

## 📈 ANALYTICS & INSIGHTS

### Dashboard Metrics

```sql
-- Tổng quan thị trường
SELECT
  COUNT(*) as total_properties,
  AVG(price) as avg_price,
  AVG(area) as avg_area,
  AVG(price_per_m2) as avg_price_per_m2
FROM properties;

-- Top khu vực hot
SELECT province, district, COUNT(*), AVG(price)
FROM properties
GROUP BY province, district
ORDER BY COUNT(*) DESC;

-- Phân tích theo loại nhà
SELECT house_type, COUNT(*), AVG(price), AVG(area)
FROM properties
WHERE house_type IS NOT NULL
GROUP BY house_type;

-- Trendline giá theo thời gian
SELECT
  DATE_TRUNC('month', posted_date) as month,
  AVG(price) as avg_price,
  COUNT(*) as listings_count
FROM properties
WHERE posted_date >= NOW() - INTERVAL '12 months'
GROUP BY month
ORDER BY month;
```

## 🎨 UI/UX FEATURES

### Modern Web Features

1. **Responsive Design** - Mobile-first
2. **Progressive Web App** - Offline capability
3. **Lazy Loading** - Fast page loads
4. **Image Optimization** - WebP format
5. **SEO Optimization** - Meta tags, structured data
6. **Social Sharing** - Share properties on social media

### User Experience

1. **Save Favorites** - Bookmark properties
2. **Price Alerts** - Notify price changes
3. **Comparison Tool** - Compare multiple properties
4. **Virtual Tour** - 360° view if available
5. **Mortgage Calculator** - Calculate payments
6. **Nearby Services** - Schools, hospitals, malls

## 🚀 DEPLOYMENT ARCHITECTURE

```
Internet
    ↓
[Nginx Load Balancer]
    ↓
[React App] ←→ [API Server] ←→ [PostgreSQL]
                    ↓              ↑
                [Redis Cache]  [ETL Pipeline]
                                   ↑
                           [Spark Gold Data]
```

---

Bạn muốn tôi bắt đầu implement phần nào trước? Frontend, Backend API, hay Database setup?
