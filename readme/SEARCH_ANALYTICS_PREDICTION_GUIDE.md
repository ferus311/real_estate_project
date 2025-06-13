# SCHEMA CHO TÌM KIẾM, THỐNG KÊ VÀ DỰ ĐOÁN GIÁ

## 🎯 TỔNG QUAN

Schema này được thiết kế để hỗ trợ 3 chức năng chính:

1. **🔍 TÌM KIẾM** - Advanced search với full-text, filters, geo search
2. **📊 THỐNG KÊ** - Market analytics, price trends, distribution analysis
3. **🔮 DỰ ĐOÁN** - Price prediction, market demand forecasting

## 📋 CẤU TRÚC DATABASE

### 🏠 CORE TABLES

#### `properties` - Bảng chính

```sql
-- Thông tin cơ bản
property_id, title, description, price, area, price_per_m2

-- Địa chỉ (denormalized cho search nhanh)
province, district, ward, street, location
-- IDs cho performance joins
province_id, district_id, ward_id, street_id

-- Tọa độ cho map
latitude, longitude, geom (PostGIS)

-- Đặc điểm (quan trọng cho prediction)
bedrooms, bathrooms, house_type, house_direction, legal_status

-- Kích thước (ảnh hưởng giá)
width, length, facade_width, road_width

-- Computed fields cho search
price_tier, area_tier, search_vector
```

#### Reference Tables

-   `provinces`, `districts`, `wards`, `streets` - Complete address hierarchy
-   `property_types`, `legal_statuses` - Master data với coefficients
-   `house_directions`, `interior_types` - Feature references

### 📊 ANALYTICS TABLES

#### `market_analytics` - Pre-computed market stats

```sql
-- Thống kê theo location + time
province_id, district_id, month, data_type
total_listings, avg_price, median_price
price_change_indicators
```

#### `price_trends` - Xu hướng giá

```sql
-- Trend data theo tuần
week, province_id, district_id, house_type_code
avg_price, price_change_week/month/year
demand_indicator, inventory_level
```

### 🤖 ML PREDICTION TABLES

#### `ml_models` - Model metadata

```sql
model_name, model_version, algorithm
r2_score, mae, rmse, mape
feature_importance (JSON)
is_active
```

#### `price_predictions` - Cached predictions

```sql
property_id, model_id
predicted_price, confidence_score
prediction_range_min/max
feature_contributions (SHAP values)
```

## 🔍 CHỨC NĂNG TÌM KIẾM

### 1. Advanced Search Function

```sql
SELECT * FROM advanced_search_properties(
    search_province => 'Hồ Chí Minh',
    search_district => 'Quận 1',
    search_street => 'Nguyễn Huệ', -- Hỗ trợ tìm theo đường
    price_min => 2000000000,
    price_max => 5000000000,
    area_min => 50,
    bedrooms_min => 2,
    house_type_filter => 'Nhà riêng',
    search_text => 'mặt tiền đường lớn',
    sort_by => 'price_per_m2',
    page_num => 1,
    page_size => 20
);
```

**Hỗ trợ:**

-   ✅ Location filters (province/district/ward/street)
-   ✅ Price & area ranges
-   ✅ Property characteristics
-   ✅ Full-text search (Vietnamese)
-   ✅ Geographic bounding box
-   ✅ Street-level search and filtering
-   ✅ Quality filtering
-   ✅ Ranking & pagination

### 2. Geographic Radius Search

```sql
SELECT * FROM search_properties_by_radius(
    center_lat => 10.762622,
    center_lng => 106.660172,
    radius_meters => 5000,
    price_min => 1000000000,
    limit_results => 50
);
```

**Use cases:**

-   🗺️ Map-based search
-   📍 "Tìm nhà gần đây"
-   🚇 Tìm theo trạm metro/bus

### 3. Search Features

#### Full-Text Search (Vietnamese)

-   Search trong title, description, location
-   Vietnamese text processing
-   Ranking by relevance

#### Performance Features

-   25+ optimized indexes
-   PostGIS spatial indexing
-   Full-text search vectors
-   Composite indexes cho common queries

## 📊 CHỨC NĂNG THỐNG KÊ

### 1. Market Overview

```sql
SELECT * FROM get_market_overview(
    target_province => 'Hà Nội',
    target_district => 'Ba Đình',
    months_back => 12,
    data_type_filter => 'sell'
);
```

**Kết quả:**

```
province | district | total_properties | avg_price | median_price | market_activity_score
Hà Nội   | Ba Đình  | 1,234           | 8.5B      | 7.2B         | 8.5
```

### 2. Price Trends Analysis

```sql
SELECT * FROM get_price_trends(
    target_province => 'Hồ Chí Minh',
    target_district => 'Quận 2',
    months_back => 24
);
```

**Kết quả:**

```
month    | avg_price | price_change_pct | listings_count
2024-01  | 5.2B      | +2.3%            | 156
2024-02  | 5.4B      | +3.8%            | 142
```

### 3. Market Distribution

```sql
SELECT * FROM get_market_distribution(
    target_province => 'Đà Nẵng',
    target_district => 'Hải Châu'
);
```

**Phân tích:**

-   📊 Price tier distribution (budget/mid-range/luxury)
-   🏠 House type distribution
-   📐 Area tier distribution
-   💹 Average price by category

### 4. Analytics Dashboard Queries

#### Real-time Market Stats

```sql
-- Tổng quan thị trường hiện tại
SELECT
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    COUNT(DISTINCT province) as provinces_covered,
    AVG(data_quality_score) as avg_quality
FROM properties
WHERE processing_date >= CURRENT_DATE - 7;

-- Top 10 khu vực hot nhất
SELECT province, district, COUNT(*), AVG(price)
FROM properties
WHERE posted_date >= NOW() - INTERVAL '30 days'
GROUP BY province, district
ORDER BY COUNT(*) DESC
LIMIT 10;
```

#### Market Comparison

```sql
-- So sánh giá/m² giữa các quận
SELECT
    district,
    AVG(price_per_m2) as avg_price_per_m2,
    COUNT(*) as sample_size
FROM properties
WHERE province = 'Hồ Chí Minh'
    AND price_per_m2 IS NOT NULL
GROUP BY district
ORDER BY avg_price_per_m2 DESC;
```

## 🔮 CHỨC NĂNG DỰ ĐOÁN

### 1. Price Estimation (Rule-based)

```sql
SELECT * FROM estimate_property_price(
    input_province => 'Hồ Chí Minh',
    input_district => 'Quận 7',
    input_area => 80,
    input_bedrooms => 3,
    input_house_type => 'Nhà phố',
    input_facade_width => 5,
    similar_properties_count => 30
);
```

**Kết quả:**

```
estimated_price | price_range_min | price_range_max | confidence_score
4,200,000,000   | 3,800,000,000   | 4,600,000,000   | 0.85
```

**Algorithm:**

1. Tìm properties tương tự (location, area, type)
2. Tính trọng số theo độ tương tự
3. Estimate price từ comparable properties
4. Calculate confidence based on data quality

### 2. Market Demand Prediction

```sql
SELECT * FROM predict_market_demand(
    target_province => 'Hà Nội',
    target_district => 'Cầu Giấy',
    prediction_months => 6
);
```

**Kết quả:**

```
prediction_month | predicted_listings | predicted_avg_price | demand_level | confidence
2024-07         | 89                 | 6,800,000,000       | high         | 0.8
2024-08         | 76                 | 7,100,000,000       | normal       | 0.7
```

**Factors:**

-   📈 Historical trends
-   🗓️ Seasonal patterns
-   📊 Market activity indicators
-   🏗️ Supply/demand dynamics

### 3. Advanced ML Integration

#### Model Management

```sql
-- Register new ML model
INSERT INTO ml_models (
    model_name, model_version, algorithm,
    r2_score, mae, rmse,
    feature_importance,
    is_active
) VALUES (
    'price_predictor_v2', '2.1.0', 'xgboost',
    0.89, 285000000, 425000000,
    '{"area": 0.35, "location": 0.28, "house_type": 0.15}'::jsonb,
    true
);

-- Cache predictions
INSERT INTO price_predictions (
    property_id, model_id,
    predicted_price, confidence_score,
    feature_contributions
) VALUES (
    'PROP123', 1,
    4200000000, 0.87,
    '{"area_impact": +15%, "location_impact": +8%}'::jsonb
);
```

#### Feature Engineering for ML

```sql
-- Extract features for ML training
WITH property_features AS (
    SELECT
        property_id,
        price,
        area,
        bedrooms,
        bathrooms,

        -- Location features
        province_id,
        district_id,

        -- Encoded categorical features
        house_type_code,
        legal_status_code,
        house_direction_code,

        -- Dimension features
        width,
        facade_width,
        road_width,

        -- Derived features
        price_per_m2,
        width * length as total_land_area,
        facade_width / width as facade_ratio,

        -- Time features
        EXTRACT(MONTH FROM posted_date) as posted_month,
        EXTRACT(YEAR FROM posted_date) as posted_year,

        -- Quality features
        data_quality_score

    FROM properties
    WHERE price IS NOT NULL
        AND area IS NOT NULL
        AND data_quality_score > 0.7
)
SELECT * FROM property_features;
```

## 🚀 DEPLOYMENT & USAGE

### 1. Setup Database

```bash
# Create schema
psql -h localhost -U postgres -d realestate \
    -f database/schemas/search_analytics_prediction_schema.sql

# Add functions
psql -h localhost -U postgres -d realestate \
    -f database/schemas/search_analytics_prediction_functions.sql
```

### 2. Load Data

```bash
# Load data from Gold layer
spark-submit data_processing/spark/jobs/load/load_to_serving.py \
    --date 2024-06-14 \
    --property-type all
```

### 3. Website Integration

#### Search API

```python
@app.get("/api/search")
async def search_properties(
    province: str = None,
    district: str = None,
    price_min: int = None,
    price_max: int = None,
    area_min: float = None,
    area_max: float = None,
    bedrooms: int = None,
    house_type: str = None,
    search_text: str = None,
    sort_by: str = "posted_date",
    page: int = 1
):
    query = f"""
    SELECT * FROM advanced_search_properties(
        search_province => '{province}',
        search_district => '{district}',
        price_min => {price_min},
        price_max => {price_max},
        area_min => {area_min},
        area_max => {area_max},
        bedrooms_min => {bedrooms},
        house_type_filter => '{house_type}',
        search_text => '{search_text}',
        sort_by => '{sort_by}',
        page_num => {page}
    )
    """
    return execute_query(query)
```

#### Analytics API

```python
@app.get("/api/analytics/market-overview")
async def market_overview(province: str, district: str = None):
    query = f"""
    SELECT * FROM get_market_overview(
        target_province => '{province}',
        target_district => '{district}'
    )
    """
    return execute_query(query)

@app.get("/api/analytics/price-trends")
async def price_trends(province: str, district: str):
    query = f"""
    SELECT * FROM get_price_trends(
        target_province => '{province}',
        target_district => '{district}'
    )
    """
    return execute_query(query)
```

#### Prediction API

```python
@app.post("/api/predict/price")
async def predict_price(property_data: PropertyInput):
    query = f"""
    SELECT * FROM estimate_property_price(
        input_province => '{property_data.province}',
        input_district => '{property_data.district}',
        input_area => {property_data.area},
        input_bedrooms => {property_data.bedrooms},
        input_house_type => '{property_data.house_type}',
        input_facade_width => {property_data.facade_width}
    )
    """
    return execute_query(query)
```

## 🎯 KẾT LUẬN

Schema này cung cấp foundation hoàn chỉnh cho:

### ✅ TÌM KIẾM MẠNH MẼ

-   Advanced filters với 20+ criteria
-   Full-text search Vietnamese
-   Geographic radius search
-   High performance với optimized indexes

### ✅ THỐNG KÊ CHUYÊN SÂU

-   Market overview by location/time
-   Price trends & change analysis
-   Distribution analysis
-   Real-time dashboard metrics

### ✅ DỰ ĐOÁN THÔNG MINH

-   Rule-based price estimation
-   Market demand forecasting
-   ML model integration ready
-   Feature engineering support

**Roadmap triển khai:**

1. **Week 1:** Setup schema + basic search
2. **Week 2:** Analytics dashboard
3. **Week 3:** Price prediction features
4. **Week 4:** Advanced ML integration

Bạn muốn implement phần nào trước?

## 🛣️ STREET-LEVEL ANALYTICS

### 1. Street Market Analysis

```sql
SELECT * FROM get_street_market_analysis(
    target_province => 'Hồ Chí Minh',
    target_district => 'Quận 1',
    target_street => 'Nguyễn Huệ',  -- Optional: specific street
    months_back => 12
);
```

**Kết quả:**

```
street              | total_properties | avg_price | median_price | street_premium_pct
Nguyễn Huệ         | 45               | 12.5B     | 11.8B        | +35.2%
Lê Lợi             | 38               | 9.8B      | 9.2B         | +8.7%
Đồng Khởi          | 28               | 15.2B     | 14.5B        | +68.9%
```

**Chỉ số:**

-   📊 Street premium so với district average
-   🏠 Property count per street
-   💰 Price statistics per street
-   📈 Market activity level

### 2. Hot Streets Analysis

```sql
SELECT * FROM get_hot_streets(
    target_province => 'Hà Nội',
    target_district => 'Ba Đình',
    top_n => 10
);
```

**Kết quả:**

```
street          | hotness_score | avg_price | total_listings | price_growth_rate
Nguyễn Thái Học | 9.2          | 8.5B      | 32             | +15.3%
Đội Cấn         | 8.8          | 7.8B      | 28             | +12.7%
Hoàng Hoa Thám  | 8.1          | 6.9B      | 24             | +8.9%
```

**Hotness Score tính từ:**

-   🔥 Listing activity (số lượng tin đăng)
-   💹 Price growth rate
-   ⏱️ Time on market (thời gian bán)
-   👥 User engagement metrics

### 3. Street Price Estimation

```sql
SELECT * FROM estimate_price_by_street(
    target_province => 'Hồ Chí Minh',
    target_district => 'Quận 3',
    target_street => 'Võ Văn Tần',
    input_area => 75,
    input_house_type => 'Nhà phố'
);
```

**Kết quả:**

```
estimated_price | street_coefficient | district_avg | street_premium
5,850,000,000   | 1.18              | 4,950,000,000 | +18.2%
```

### 4. Same Street Properties

```sql
SELECT * FROM find_properties_same_street(
    reference_property_id => 'PROP12345',
    limit_results => 20
);
```

**Use cases:**

-   🏘️ "Xem nhà cùng đường"
-   💰 Price comparison within street
-   📊 Street market analysis
-   🎯 Targeted recommendations
