-- Optimized PostgreSQL Schema for Serving Layer
-- Theo đề xuất trong OPTIMIZED_ARCHITECTURE_PROPOSAL.md

-- =======================
-- CORE TABLES
-- =======================

-- Bảng properties chính với computed fields
CREATE TABLE IF NOT EXISTS properties (
    -- Primary key
    property_id VARCHAR(100) PRIMARY KEY,

    -- Basic property information
    title TEXT NOT NULL,
    description TEXT,
    price BIGINT NOT NULL CHECK (price > 0),
    area REAL NOT NULL CHECK (area > 0),
    bedrooms INTEGER DEFAULT 0,
    bathrooms INTEGER DEFAULT 0,

    -- Location information
    province VARCHAR(100),
    district VARCHAR(100),
    ward VARCHAR(100),
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,

    -- Source information
    source VARCHAR(50) NOT NULL,
    url TEXT,

    -- Computed fields (calculated automatically)
    price_per_m2 REAL GENERATED ALWAYS AS (
        CASE
            WHEN area > 0 THEN price / area
            ELSE NULL
        END
    ) STORED,

    price_tier TEXT GENERATED ALWAYS AS (
        CASE
            WHEN price < 1000000000 THEN 'budget'
            WHEN price < 5000000000 THEN 'mid-range'
            ELSE 'luxury'
        END
    ) STORED,

    area_tier TEXT GENERATED ALWAYS AS (
        CASE
            WHEN area < 50 THEN 'small'
            WHEN area < 100 THEN 'medium'
            WHEN area < 200 THEN 'large'
            ELSE 'extra-large'
        END
    ) STORED,

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    processing_date DATE,

    -- Unique constraint để tránh duplicate
    UNIQUE(url, source)
);

-- Bảng locations để normalize địa điểm
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    province VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    ward VARCHAR(100),
    full_address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(province, district, ward)
);

-- =======================
-- INDEXES FOR PERFORMANCE
-- =======================

-- Performance indexes cho web queries
CREATE INDEX IF NOT EXISTS idx_properties_price ON properties(price);
CREATE INDEX IF NOT EXISTS idx_properties_area ON properties(area);
CREATE INDEX IF NOT EXISTS idx_properties_location ON properties(province, district);
CREATE INDEX IF NOT EXISTS idx_properties_price_tier ON properties(price_tier);
CREATE INDEX IF NOT EXISTS idx_properties_area_tier ON properties(area_tier);
CREATE INDEX IF NOT EXISTS idx_properties_price_per_m2 ON properties(price_per_m2);
CREATE INDEX IF NOT EXISTS idx_properties_created_at ON properties(created_at);
CREATE INDEX IF NOT EXISTS idx_properties_source ON properties(source);

-- Composite indexes cho common queries
CREATE INDEX IF NOT EXISTS idx_properties_search ON properties(province, price_tier, area_tier);
CREATE INDEX IF NOT EXISTS idx_properties_map ON properties(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- GiST index cho geo queries
CREATE INDEX IF NOT EXISTS idx_properties_geo ON properties USING GIST(
    point(longitude, latitude)
) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- =======================
-- MATERIALIZED VIEWS FOR ANALYTICS
-- =======================

-- Market analytics view (refresh mỗi 30 phút)
CREATE MATERIALIZED VIEW IF NOT EXISTS market_analytics AS
SELECT
    province,
    district,
    DATE_TRUNC('month', created_at) as month,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price_per_m2) as avg_price_per_m2,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2) as median_price_per_m2,
    AVG(area) as avg_area,
    COUNT(CASE WHEN price_tier = 'budget' THEN 1 END) as budget_count,
    COUNT(CASE WHEN price_tier = 'mid-range' THEN 1 END) as midrange_count,
    COUNT(CASE WHEN price_tier = 'luxury' THEN 1 END) as luxury_count
FROM properties
WHERE created_at >= NOW() - INTERVAL '2 years'
GROUP BY province, district, DATE_TRUNC('month', created_at);

CREATE UNIQUE INDEX ON market_analytics (province, district, month);

-- Dashboard summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_summary AS
SELECT
    'total_properties' as metric,
    COUNT(*)::TEXT as value,
    NOW() as updated_at
FROM properties
UNION ALL
SELECT
    'avg_price' as metric,
    ROUND(AVG(price))::TEXT as value,
    NOW() as updated_at
FROM properties
UNION ALL
SELECT
    'avg_area' as metric,
    ROUND(AVG(area), 1)::TEXT as value,
    NOW() as updated_at
FROM properties
UNION ALL
SELECT
    'provinces_count' as metric,
    COUNT(DISTINCT province)::TEXT as value,
    NOW() as updated_at
FROM properties;

-- Price trends view
CREATE MATERIALIZED VIEW IF NOT EXISTS price_trends AS
SELECT
    DATE_TRUNC('week', created_at) as week,
    province,
    COUNT(*) as listings_count,
    AVG(price) as avg_price,
    AVG(price_per_m2) as avg_price_per_m2,
    CASE
        WHEN LAG(AVG(price)) OVER (PARTITION BY province ORDER BY DATE_TRUNC('week', created_at)) IS NOT NULL
        THEN ((AVG(price) - LAG(AVG(price)) OVER (PARTITION BY province ORDER BY DATE_TRUNC('week', created_at)))
              / LAG(AVG(price)) OVER (PARTITION BY province ORDER BY DATE_TRUNC('week', created_at)) * 100)
        ELSE 0
    END as price_change_percent
FROM properties
WHERE created_at >= NOW() - INTERVAL '6 months'
GROUP BY DATE_TRUNC('week', created_at), province;

CREATE INDEX ON price_trends (province, week);

-- =======================
-- TRIGGERS FOR MAINTENANCE
-- =======================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_properties_updated_at
    BEFORE UPDATE ON properties
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =======================
-- FUNCTIONS FOR COMMON OPERATIONS
-- =======================

-- Function để refresh materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY market_analytics;
    REFRESH MATERIALIZED VIEW dashboard_summary;
    REFRESH MATERIALIZED VIEW price_trends;
END;
$$ LANGUAGE plpgsql;

-- Function để tìm properties gần nhất
CREATE OR REPLACE FUNCTION find_nearby_properties(
    input_lat DOUBLE PRECISION,
    input_lng DOUBLE PRECISION,
    radius_km DOUBLE PRECISION DEFAULT 5.0,
    limit_count INTEGER DEFAULT 20
)
RETURNS TABLE(
    property_id VARCHAR(100),
    title TEXT,
    price BIGINT,
    area REAL,
    distance_km DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.property_id,
        p.title,
        p.price,
        p.area,
        (
            6371 * acos(
                cos(radians(input_lat))
                * cos(radians(p.latitude))
                * cos(radians(p.longitude) - radians(input_lng))
                + sin(radians(input_lat))
                * sin(radians(p.latitude))
            )
        ) as distance_km
    FROM properties p
    WHERE p.latitude IS NOT NULL
      AND p.longitude IS NOT NULL
      AND (
          6371 * acos(
              cos(radians(input_lat))
              * cos(radians(p.latitude))
              * cos(radians(p.longitude) - radians(input_lng))
              + sin(radians(input_lat))
              * sin(radians(p.latitude))
          )
      ) <= radius_km
    ORDER BY distance_km
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- =======================
-- SAMPLE DATA VALIDATION
-- =======================

-- Constraints để đảm bảo data quality
ALTER TABLE properties ADD CONSTRAINT check_price_reasonable
    CHECK (price BETWEEN 50000000 AND 100000000000);

ALTER TABLE properties ADD CONSTRAINT check_area_reasonable
    CHECK (area BETWEEN 10 AND 2000);

ALTER TABLE properties ADD CONSTRAINT check_coordinates_valid
    CHECK (
        (latitude IS NULL AND longitude IS NULL) OR
        (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
    );

-- =======================
-- COMMENTS FOR DOCUMENTATION
-- =======================

COMMENT ON TABLE properties IS 'Main properties table with computed fields for serving layer';
COMMENT ON COLUMN properties.price_per_m2 IS 'Automatically computed price per square meter';
COMMENT ON COLUMN properties.price_tier IS 'Automatically computed price category (budget/mid-range/luxury)';
COMMENT ON MATERIALIZED VIEW market_analytics IS 'Market analytics aggregated by location and month';
COMMENT ON FUNCTION refresh_analytics_views() IS 'Refresh all materialized views for analytics';
COMMENT ON FUNCTION find_nearby_properties(DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER) IS 'Find properties within specified radius using haversine formula';

-- Initial refresh of materialized views
SELECT refresh_analytics_views();
