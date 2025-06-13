# PostgreSQL Schema cho Real Estate Gold Data

## Tổng quan

Schema này được thiết kế để lưu trữ dữ liệu bất động sản từ Gold layer vào PostgreSQL serving layer một cách tối ưu, hỗ trợ các truy vấn web API và analytics.

## Kiến trúc Schema

### 1. Main Properties Table

-   **Table**: `properties`
-   **Chức năng**: Lưu trữ tất cả thông tin bất động sản
-   **Features**:
    -   Computed fields cho `price_per_m2`, `price_tier`, `area_tier`
    -   PostGIS geometry point cho geo queries
    -   Full-text search support
    -   Data quality tracking

### 2. Reference Tables

-   **provinces**: Danh sách tỉnh thành
-   **districts**: Danh sách quận/huyện
-   **wards**: Danh sách phường/xã
-   **property_types**: Loại hình bất động sản
-   **legal_statuses**: Tình trạng pháp lý

### 3. Analytics Views

-   **market_analytics**: Phân tích thị trường theo vùng và thời gian
-   **dashboard_summary**: Tổng quan dashboard
-   **geo_density**: Mật độ bất động sản theo địa lý

## Mapping từ Gold Schema

| Gold Field           | PostgreSQL Field | Type         | Notes                           |
| -------------------- | ---------------- | ------------ | ------------------------------- |
| `id`                 | `property_id`    | VARCHAR(100) | Primary key                     |
| `bedroom`            | `bedrooms`       | INTEGER      | Rename for clarity              |
| `bathroom`           | `bathrooms`      | INTEGER      | Rename for clarity              |
| `location`           | `location`       | TEXT         | Keep original + address parsing |
| `latitude/longitude` | `geom`           | GEOMETRY     | PostGIS point                   |

## Performance Optimization

### Indexes

```sql
-- Core search indexes
CREATE INDEX idx_properties_price ON properties(price);
CREATE INDEX idx_properties_area ON properties(area);
CREATE INDEX idx_properties_location ON properties(province, district);

-- Geographic index
CREATE INDEX idx_properties_geom ON properties USING GIST(geom);

-- Composite indexes for common queries
CREATE INDEX idx_properties_search_main ON properties(province, price_tier, area_tier, bedrooms);
```

### Materialized Views

-   Tự động refresh mỗi 4 giờ
-   Hỗ trợ concurrent refresh
-   Analytics pre-computed

## Data Loading Process

### 1. Direct Append (Preferred)

```python
serving_df.write.format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", "properties") \
    .mode("append") \
    .save()
```

### 2. Staging Table Approach

```python
# Load to staging first
serving_df.write.format("jdbc") \
    .option("dbtable", "properties_staging") \
    .mode("overwrite") \
    .save()

# Then upsert to main table
SELECT * FROM upsert_properties_from_staging();
```

## Key Functions

### 1. Upsert from Staging

```sql
SELECT * FROM upsert_properties_from_staging();
-- Returns: inserted_count, updated_count, total_processed
```

### 2. Geographic Search

```sql
SELECT * FROM find_properties_within_radius(
    10.762622,  -- latitude
    106.660172, -- longitude
    5000,       -- radius in meters
    50,         -- max results
    1000000000, -- min price
    5000000000  -- max price
);
```

### 3. Market Analytics

```sql
SELECT * FROM generate_market_report(
    'Hồ Chí Minh', -- province
    '2024-01-01',  -- start date
    '2024-12-31'   -- end date
);
```

### 4. Data Quality Validation

```sql
SELECT * FROM validate_properties_data();
-- Returns issues: missing_title, invalid_price, invalid_area, etc.
```

## Daily Maintenance

### Automated Tasks

```sql
SELECT daily_maintenance();
-- Performs:
-- 1. Remove duplicates
-- 2. Cleanup old low-quality data
-- 3. Refresh materialized views
-- 4. Update statistics
```

### Manual Maintenance

```sql
-- Cleanup duplicates
SELECT cleanup_duplicate_properties();

-- Remove old data (keep 1 year)
SELECT cleanup_old_properties(365);

-- Refresh analytics views
SELECT refresh_all_analytics();
```

## Data Quality Constraints

### Automatic Validation

-   Price: 10M - 1,000B VND
-   Area: 5m² - 1 hectare
-   Coordinates: Valid lat/lng ranges
-   Bedrooms/Bathrooms: 0-20/0-10

### Quality Scoring

-   Data quality score từ Gold layer được preserve
-   Automatic filtering cho serving layer
-   Quality metrics trong analytics

## Web API Support

### Common Queries

```sql
-- Search properties
SELECT property_id, title, price, area, province, district
FROM properties
WHERE province = 'Hồ Chí Minh'
  AND price_tier = 'mid-range'
  AND bedrooms >= 2
ORDER BY created_at DESC
LIMIT 20;

-- Map properties
SELECT property_id, title, price, ST_X(geom) as lng, ST_Y(geom) as lat
FROM properties
WHERE geom && ST_MakeEnvelope(106.6, 10.7, 106.8, 10.9, 4326)
  AND price IS NOT NULL;

-- Market stats
SELECT province, COUNT(*) as total, AVG(price) as avg_price
FROM properties
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY province
ORDER BY total DESC;
```

### Dashboard Metrics

```sql
-- Real-time dashboard
SELECT * FROM dashboard_summary;

-- Price trends
SELECT * FROM price_trends
WHERE province = 'Hồ Chí Minh'
ORDER BY week DESC
LIMIT 12;
```

## Deployment Steps

### 1. Create Schema

```bash
psql -h postgres -U postgres -d realestate -f gold_to_postgres_schema.sql
```

### 2. Add Procedures

```bash
psql -h postgres -U postgres -d realestate -f postgres_procedures.sql
```

### 3. Load Data

```bash
spark-submit --jars postgresql-driver.jar \
  data_processing/spark/jobs/load/load_to_serving.py \
  --date 2024-06-14 \
  --property-type all
```

### 4. Setup Monitoring

```sql
-- Check data quality
SELECT * FROM validate_properties_data();

-- Monitor load jobs
SELECT * FROM data_load_jobs ORDER BY started_at DESC LIMIT 10;

-- Check system logs
SELECT * FROM system_logs WHERE event_type = 'upsert_properties' ORDER BY created_at DESC LIMIT 5;
```

## Performance Considerations

### Connection Pooling

-   Khuyến nghị sử dụng PgBouncer
-   Connection pool size: 10-20 cho web app
-   Statement timeout: 30s

### Batch Loading

-   Batch size: 10,000 records
-   Parallel partitions: 4-8
-   Use staging table cho large datasets

### Query Optimization

-   Use appropriate indexes
-   Filter early trong WHERE clause
-   Limit results cho pagination
-   Use materialized views cho analytics

## Monitoring & Alerting

### Key Metrics

-   Daily load volume
-   Data quality scores
-   Query performance
-   Storage growth

### Alerts

-   Failed load jobs
-   Data quality degradation
-   Long-running queries
-   Storage usage > 80%

## Backup & Recovery

### Daily Backup

```bash
pg_dump -h postgres -U postgres realestate > backup_$(date +%Y%m%d).sql
```

### Point-in-time Recovery

-   WAL archiving enabled
-   7-day retention policy
-   Automated recovery testing

---

## Contact & Support

Để hỗ trợ và tối ưu hóa thêm, hãy tham khảo:

-   Database performance logs
-   Application query patterns
-   Business requirements changes
