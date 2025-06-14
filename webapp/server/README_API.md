# Real Estate API Documentation

## 🚀 Overview

API backend cho website bất động sản với 3 chức năng chính:

1. **🔮 PREDICTION** - Dự đoán giá nhà bằng 4 models ML
2. **🔍 SEARCH** - Tìm kiếm bất động sản nâng cao
3. **📊 ANALYTICS** - Thống kê và phân tích thị trường

## 🏗️ Architecture

```
webapp/server/
├── prediction/          # ML price prediction APIs
├── search/             # Property search APIs
├── analytics/          # Market analytics APIs
├── properties/         # Core property CRUD (existing)
└── realestate_api/     # Main Django project
```

## 📚 API Endpoints

### 🔮 PREDICTION APIs

**Load models từ HDFS và dự đoán giá**

#### 1. Linear Regression

```http
POST /api/prediction/linear-regression/
Content-Type: application/json

{
    "area": 80.0,
    "latitude": 10.762622,
    "longitude": 106.660172,
    "bedroom": 3,
    "bathroom": 2,
    "floor_count": 3,
    "house_direction_code": 3,
    "legal_status_code": 1,
    "interior_code": 2,
    "province_id": 79,
    "district_id": 769,
    "ward_id": 27000
}
```

#### 2. XGBoost

```http
POST /api/prediction/xgboost/
```

#### 3. LightGBM

```http
POST /api/prediction/lightgbm/
```

#### 4. Ensemble (All Models)

```http
POST /api/prediction/ensemble/
```

#### Model Information

```http
GET /api/prediction/model-info/
GET /api/prediction/feature-info/
```

**Response Format:**

```json
{
    "success": true,
    "model": "xgboost",
    "predicted_price": 4200000000,
    "predicted_price_formatted": "4,200,000,000 VND",
    "model_metrics": {
        "r2": 0.89,
        "rmse": 285000000,
        "mae": 425000000
    },
    "input_features": { ... }
}
```

### 🔍 SEARCH APIs

#### Advanced Search

```http
GET /api/search/advanced/?province=Hồ Chí Minh&district=Quận 1&price_min=2000000000&price_max=5000000000&bedroom_min=2&area_min=50&sort_by=price&page=1

POST /api/search/advanced/
{
    "province": "Hồ Chí Minh",
    "district": "Quận 1",
    "price_min": 2000000000,
    "price_max": 5000000000,
    "bedroom_min": 2,
    "area_min": 50,
    "house_type": "Nhà phố",
    "search_text": "mặt tiền",
    "sort_by": "price",
    "page": 1
}
```

#### Location Search

```http
GET /api/search/location/?province=Hà Nội&district=Ba Đình
```

#### Radius Search

```http
GET /api/search/radius/?latitude=10.762622&longitude=106.660172&radius=5000
```

#### Autocomplete

```http
GET /api/search/suggestions/?q=Nguyễn&type=street
```

#### Property Detail

```http
GET /api/search/property/{property_id}/
```

### 📊 ANALYTICS APIs

#### Market Overview

```http
GET /api/analytics/market-overview/?province=Hồ Chí Minh&district=Quận 7
```

#### Price Distribution

```http
GET /api/analytics/price-distribution/?province=Hà Nội
```

#### Property Type Statistics

```http
GET /api/analytics/property-type-stats/?province=Đà Nẵng
```

#### Location Statistics

```http
GET /api/analytics/location-stats/?level=district&province=Hồ Chí Minh&limit=10
```

#### Price Trends

```http
GET /api/analytics/price-trends/?province=Hồ Chí Minh&months=12
```

#### Area Distribution

```http
GET /api/analytics/area-distribution/?province=Hà Nội
```

#### Dashboard Summary

```http
GET /api/analytics/dashboard-summary/
```

## 🎯 Feature Codes

### House Direction Codes

```
1: Đông
2: Tây
3: Nam
4: Bắc
5: Đông Bắc
6: Đông Nam
7: Tây Bắc
8: Tây Nam
```

### Legal Status Codes

```
1: Đã có sổ
2: Đang chờ sổ
4: Không có sổ
5: Sổ chung / Vi bằng
6: Giấy tờ viết tay
```

### Interior Codes

```
1: Cao cấp
2: Đầy đủ
3: Cơ bản
4: Bàn giao thô
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
cd /home/fer/data/real_estate_project/webapp/server
pip install -r requirements.txt
```

### 2. Run Server

```bash
python manage.py runserver 0.0.0.0:8000
```

### 3. Test APIs

```bash
# Health check
curl http://localhost:8000/api/health/

# API info
curl http://localhost:8000/api/info/

# Test prediction
curl -X POST http://localhost:8000/api/prediction/ensemble/ \
  -H "Content-Type: application/json" \
  -d '{
    "area": 80.0,
    "latitude": 10.762622,
    "longitude": 106.660172,
    "bedroom": 3,
    "bathroom": 2,
    "floor_count": 3,
    "house_direction_code": 3,
    "legal_status_code": 1,
    "interior_code": 2,
    "province_id": 79,
    "district_id": 769,
    "ward_id": 27000
  }'

# Test search
curl "http://localhost:8000/api/search/advanced/?province=Hồ Chí Minh&price_max=5000000000"

# Test analytics
curl "http://localhost:8000/api/analytics/market-overview/?province=Hà Nội"
```

## 🔧 Configuration

### Database

-   PostgreSQL connection configured in `settings.py`
-   Uses environment variables for database credentials

### HDFS Integration

-   Models are loaded from `/data/realestate/processed/ml/models/`
-   Automatic discovery of latest model version
-   Supports both Spark ML and sklearn models

### Error Handling

-   Graceful fallback if ML models not available
-   Comprehensive error messages
-   Logging for debugging

## 🎯 Model Loading Strategy

1. **Find Latest Models**: Scans HDFS for most recent training date
2. **Load Model Registry**: Reads model metadata and performance metrics
3. **Load Preprocessing**: Loads VectorAssembler + StandardScaler pipeline
4. **Load ML Models**: Loads all available Spark ML and sklearn models
5. **Ensemble Prediction**: Combines predictions from multiple models

## 📊 Response Formats

### Successful Response

```json
{
    "success": true,
    "data": { ... },
    "pagination": { ... },
    "filters": { ... }
}
```

### Error Response

```json
{
    "error": "Error message",
    "status": 500
}
```

## 🔍 Search Features

-   **Multi-criteria filtering**: Price, area, location, property type
-   **Full-text search**: Title, description, location
-   **Geographic search**: Radius-based location search
-   **Autocomplete**: Smart suggestions for location names
-   **Pagination**: Efficient result pagination
-   **Sorting**: Multiple sort options

## 📈 Analytics Features

-   **Market Overview**: Total properties, average prices, market activity
-   **Price Distribution**: Price ranges with counts and averages
-   **Location Analysis**: Top provinces/districts by activity
-   **Property Type Stats**: Analysis by house type
-   **Time Trends**: Monthly price and volume trends
-   **Area Distribution**: Property size distribution analysis

## 🎯 Next Steps

1. **Frontend Integration**: Connect React frontend to these APIs
2. **Caching**: Add Redis caching for frequently accessed data
3. **Authentication**: Add JWT authentication if needed
4. **Rate Limiting**: Add API rate limiting
5. **Monitoring**: Add API performance monitoring
6. **Documentation**: Auto-generate OpenAPI/Swagger docs

---

**Happy Coding! 🚀**
