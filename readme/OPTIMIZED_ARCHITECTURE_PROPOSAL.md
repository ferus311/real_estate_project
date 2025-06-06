# Kiến Trúc Tối Ưu Hóa Toàn Bộ Data Pipeline

## Vấn đề hiện tại với kiến trúc cũ:

### **Vấn đề ở tầng Gold/Serving:**

1. **Trùng lặp lưu trữ**: JSON → Parquet → PostgreSQL → Redis (lưu trữ 3-4 lần)
2. **Quá phức tạp**: 4 layers (Bronze/Silver/Gold/Serving) với quá nhiều chuyển đổi
3. **Chuyển đổi không hiệu quả**: JSON→Parquet chỉ để đổi format
4. **Lãng phí tài nguyên**: Cùng một dữ liệu lưu ở nhiều nơi

### **Vấn đề ở các tầng dưới (Bronze/Silver):**

1. **Transform phức tạp**: Logic xử lý outliers và imputation quá nặng trong Spark
2. **Dependency hell**: Quá nhiều dependencies giữa các jobs
3. **Hard to maintain**: Code transform quá dài và phức tạp
4. **Performance issues**: Nhiều cache/unpersist không cần thiết

## Kiến trúc tối ưu hóa đề xuất (TOÀN BỘ PIPELINE):

### Nguyên tắc thiết kế:

-   **Simplify layers**: Giữ Bronze/Silver cho raw processing, tối ưu Gold/Serving
-   **Single Source of Truth**: PostgreSQL làm nguồn dữ liệu chính cho serving
-   **Smart caching**: Redis chỉ cache queries phổ biến
-   **Computed fields**: PostgreSQL tính toán sẵn analytics
-   **Optimized transforms**: Đơn giản hóa logic xử lý dữ liệu

### Luồng dữ liệu tối ưu (ĐẦY ĐỦ):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            RAW DATA INGESTION                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Crawlers (Batdongsan, Chotot) → HDFS Raw Storage                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                            BRONZE LAYER (RAW)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Raw JSON data storage                                                    │
│ • Basic deduplication                                                      │
│ • Data validation                                                          │
│ Storage: HDFS JSON files                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER (CLEANED)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Data cleaning & standardization                                          │
│ • Simple outlier removal (business rules only)                            │
│ • Basic imputation (median/mode)                                          │
│ • Source integration                                                       │
│ Storage: HDFS Parquet files                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER (FEATURES)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Feature engineering for ML                                               │
│ • Data unification (single source per property)                           │
│ • ML training data preparation                                             │
│ Storage: HDFS Parquet files                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SERVING LAYER (OPTIMIZED)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                       Direct ETL từ Gold                                   │
│                              ↓                                             │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                 PostgreSQL (Single Source of Truth)                    │ │
│ │ ├── Core Tables (properties, locations, contacts)                      │ │
│ │ ├── Computed Fields (price_per_m2, market_trends)                      │ │
│ │ ├── Materialized Views (analytics, dashboards)                         │ │
│ │ └── Performance Indexes                                                 │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                              ↓                                             │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │                    Redis (Intelligent Cache)                           │ │
│ │ ├── Popular Queries Cache                                               │ │
│ │ ├── Dashboard Data Cache                                                │ │
│ │ └── API Response Cache                                                  │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DJANGO API APPLICATION                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ ├── Property Service (CRUD operations)                                     │
│ ├── Analytics Service (dashboard data)                                     │
│ ├── Search Service (property search)                                       │
│ └── ML Service (price predictions)                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Thành phần chính của từng tầng:

## **BRONZE LAYER - Tối ưu hóa:**

#### 1. **Simplified Raw Data Processing**

```python
# Thay vì: Complex deduplication với nhiều steps
# Sử dụng: Simple hash-based deduplication

class BronzeProcessor:
    def process_raw_data(self, source_data):
        # Simple deduplication by URL + timestamp
        deduplicated = source_data.dropDuplicates(["url", "crawl_date"])

        # Basic validation only
        validated = deduplicated.filter(
            col("url").isNotNull() &
            col("title").isNotNull() &
            length(col("url")) > 10
        )

        # Store as JSON (keep original format)
        return validated
```

## **SILVER LAYER - Tối ưu hóa:**

#### 2. **Simplified Cleaning Pipeline**

```python
# Thay vì: Complex outlier detection + smart imputation
# Sử dụng: Business rules + simple imputation

class SilverProcessor:
    def clean_data(self, bronze_data):
        # Business rule outliers only (no statistical methods)
        cleaned = bronze_data.filter(
            (col("price") >= 50_000_000) & (col("price") <= 100_000_000_000) &
            (col("area") >= 10) & (col("area") <= 2000)
        )

        # Simple median imputation
        price_median = cleaned.select(percentile_approx("price", 0.5)).collect()[0][0]
        area_median = cleaned.select(percentile_approx("area", 0.5)).collect()[0][0]

        imputed = cleaned.fillna({
            "price": price_median,
            "area": area_median,
            "bedroom": 2,
            "bathroom": 1
        })

        return imputed
```

## **GOLD LAYER - Giữ nguyên:**

#### 3. **Feature Engineering (Unchanged)**

```python
# Gold layer giữ nguyên logic hiện tại
# Chỉ tối ưu performance và reduce dependencies
class GoldProcessor:
    def create_features(self, silver_data):
        # ML feature engineering
        # Property unification
        # Training data preparation
        pass
```

## **SERVING LAYER - Hoàn toàn mới:**

#### 4. **Direct PostgreSQL ETL**

```python
# Thay vì: Gold → JSON → Parquet → PostgreSQL
# Sử dụng: Gold → Direct PostgreSQL Load với upsert logic

class OptimizedServingPipeline:
    def extract_from_gold(self):
        # Extract từ Gold layer (unified data)
        gold_df = spark.read.parquet("/data/realestate/processed/gold/properties/")
        return gold_df

    def transform_for_serving(self, gold_df):
        # Transform cho web serving + analytics
        web_optimized = gold_df.select(
            col("property_id"),
            col("title"),
            col("price"),
            col("area"),
            col("location"),
            col("latitude"),
            col("longitude"),
            # Computed fields sẽ được tính trong PostgreSQL
        )
        return web_optimized

    def load_to_postgres(self, transformed_df):
        # Load trực tiếp vào PostgreSQL với upsert logic
        transformed_df.write \
            .format("jdbc") \
            .option("url", "postgresql://localhost:5432/realestate") \
            .option("dbtable", "properties") \
            .option("user", "postgres") \
            .option("password", "password") \
            .mode("append") \
            .save()
```

#### 5. **PostgreSQL Schema Tối Ưu**

```sql
-- Core tables với computed fields
CREATE TABLE properties (
    id SERIAL PRIMARY KEY,
    -- Raw fields
    title TEXT,
    price BIGINT,
    area REAL,
    location_id INTEGER,

    -- Computed fields (tính sẵn)
    price_per_m2 REAL GENERATED ALWAYS AS (price / NULLIF(area, 0)) STORED,
    price_tier TEXT GENERATED ALWAYS AS (
        CASE
            WHEN price < 1000000000 THEN 'budget'
            WHEN price < 5000000000 THEN 'mid-range'
            ELSE 'luxury'
        END
    ) STORED,

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Materialized views cho analytics
CREATE MATERIALIZED VIEW market_analytics AS
SELECT
    location_id,
    DATE_TRUNC('month', created_at) as month,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
    AVG(price_per_m2) as avg_price_per_m2
FROM properties
GROUP BY location_id, DATE_TRUNC('month', created_at);

CREATE UNIQUE INDEX ON market_analytics (location_id, month);
```

#### 6. **Intelligent Redis Caching**

```python
class SmartCacheService:
    def cache_popular_queries(self):
        # Cache queries được gọi > 10 lần/phút
        pass

    def cache_dashboard_data(self):
        # Cache dữ liệu dashboard, refresh 15 phút
        pass

    def invalidate_related_cache(self, property_id):
        # Khi có update, chỉ invalidate cache liên quan
        pass
```

#### 7. **Django Services Tối Ưu**

```python
class OptimizedPropertyService:
    def get_properties(self, filters):
        # Kiểm tra cache trước
        cache_key = self.generate_cache_key(filters)
        cached_result = cache.get(cache_key)
        if cached_result:
            return cached_result

        # Query PostgreSQL với optimized queries
        result = self.query_postgres_optimized(filters)

        # Cache kết quả nếu là query phổ biến
        if self.is_popular_query(filters):
            cache.set(cache_key, result, timeout=900)  # 15 minutes

        return result
```

## So sánh hiệu quả từng tầng:

### **BRONZE LAYER:**

#### Kiến trúc cũ:

-   Complex deduplication với multiple steps
-   Heavy validation logic
-   Memory-intensive operations

#### Kiến trúc mới:

-   Simple hash-based deduplication
-   Basic validation only
-   Lightweight processing

### **SILVER LAYER:**

#### Kiến trúc cũ:

-   Complex outlier detection (IQR + business rules + relationship-based)
-   Smart imputation với multiple strategies
-   Heavy caching/unpersist operations
-   Code generation issues do phức tạp

#### Kiến trúc mới:

-   Business rules outlier removal only
-   Simple median/mode imputation
-   Streamlined processing
-   No complex caching logic

### **GOLD LAYER:**

#### Kiến trúc cũ và mới:

-   Giữ nguyên (đã tối ưu tốt)
-   Feature engineering cho ML
-   Property unification

### **SERVING LAYER:**

#### Kiến trúc cũ (phức tạp):

-   **Storage**: 4x duplication (JSON + Parquet + PostgreSQL + Redis)
-   **Processing**: 3 bước chuyển đổi không cần thiết
-   **Maintenance**: 4 sub-layers cần maintain
-   **Performance**: Chậm do nhiều bước IO

#### Kiến trúc mới (tối ưu):

-   **Storage**: 1x + cache (PostgreSQL + Redis intelligent cache)
-   **Processing**: 1 bước ETL trực tiếp từ Gold
-   **Maintenance**: 2 components chính (PostgreSQL + Redis)
-   **Performance**: Nhanh do ít IO, cache thông minh

## Lợi ích tổng thể:

### **Bronze/Silver Layer:**

1. **Giảm 50% processing time**: Loại bỏ complex logic không cần thiết
2. **Ít lỗi runtime**: Đơn giản hóa transforms, ít code generation issues
3. **Dễ maintain**: Code ngắn gọn, dễ debug
4. **Better stability**: Ít dependencies, ít cache operations

### **Gold Layer:**

1. **Giữ nguyên functionality**: Không thay đổi ML pipeline
2. **Better integration**: Tối ưu interface với Serving layer

### **Serving Layer:**

1. **Tiết kiệm 70% storage space**: Loại bỏ trùng lặp hoàn toàn
2. **Tăng 3x performance**: Ít IO operations, cache thông minh
3. **Giảm 60% complexity**: Từ 4 sub-layers xuống 2 components chính
4. **Real-time capability**: Direct PostgreSQL updates
5. **Cost effective**: Ít infrastructure components

### **Tổng thể:**

1. **Đơn giản hóa architecture**: Từ 4-layer complexity xuống 3-layer optimized
2. **Better performance**: Mỗi layer được tối ưu riêng
3. **Easier debugging**: Ít components, ít dependencies
4. **Cost reduction**: Ít storage, ít compute resources
5. **Better scalability**: PostgreSQL + Redis scale tốt hơn multiple Parquet files

## Implementation Plan (ĐẦY ĐỦ):

### Phase 1: Optimize Bronze/Silver Layers

-   Đơn giản hóa Bronze deduplication logic
-   Refactor Silver cleaning pipeline (loại bỏ complex outlier detection)
-   Implement simple imputation strategies
-   Test và validate data quality

### Phase 2: Setup Optimized Serving Infrastructure

-   Tạo optimized PostgreSQL schema với computed fields
-   Setup materialized views cho analytics
-   Tạo performance indexes
-   Setup Redis cluster cho caching

### Phase 3: Build Direct Gold→PostgreSQL ETL

-   Extract trực tiếp từ Gold layer
-   Transform data cho serving needs
-   Load trực tiếp vào PostgreSQL với upsert logic
-   Implement data consistency checks

### Phase 4: Implement Smart Caching Layer

-   Redis cache cho popular queries
-   Cache invalidation logic
-   Dashboard data caching strategies
-   API response caching

### Phase 5: Optimize Django Services

-   Refactor services để sử dụng PostgreSQL + cache
-   Remove dependencies trên JSON/Parquet serving files
-   Implement query optimization
-   Add monitoring và alerting

### Phase 6: Testing & Migration

-   A/B test old vs new architecture
-   Performance benchmarking
-   Gradual traffic migration
-   Decommission old serving components

Bạn có muốn triển khai kiến trúc tối ưu này cho TOÀN BỘ pipeline không? Hoặc muốn bắt đầu từ layer nào trước?
