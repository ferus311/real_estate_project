-- PostgreSQL Schema for Real Estate Django Backend
-- Clean schema with tables, constraints, and indexes only
-- No stored procedures/functions - Django ORM will handle all logic

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

--==============================================================================
-- REFERENCE TABLES
--==============================================================================

-- Provinces (63 tỉnh thành)
CREATE TABLE provinces (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    code VARCHAR(10) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Province population density lookup table
CREATE TABLE province_population_density (
    id SERIAL PRIMARY KEY,
    province_id INTEGER NOT NULL REFERENCES provinces(id),
    province_name VARCHAR(100) NOT NULL,
    average_population_thousand DECIMAL(10,2),
    population_density DECIMAL(10,2),
    area_km2 DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(province_id)
);

-- Districts
CREATE TABLE districts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    province_id INTEGER NOT NULL REFERENCES provinces(id),
    code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, province_id)
);

-- Wards
CREATE TABLE wards (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    district_id INTEGER NOT NULL REFERENCES districts(id),
    code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, district_id)
);

-- Streets
CREATE TABLE streets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    ward_id INTEGER REFERENCES wards(id),
    district_id INTEGER REFERENCES districts(id),
    province_id INTEGER REFERENCES provinces(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Property Types (Optional - Gold already has house_type and house_type_code)
-- CREATE TABLE property_types (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(100) NOT NULL UNIQUE,
--     category VARCHAR(50), -- 'residential', 'commercial', 'land', etc.
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Legal Status (Optional - Gold already has legal_status and legal_status_code)
-- CREATE TABLE legal_statuses (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(100) NOT NULL UNIQUE,
--     description TEXT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- House Directions (Optional - Gold already has house_direction and house_direction_code)
-- CREATE TABLE house_directions (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(50) NOT NULL UNIQUE,
--     angle_degrees INTEGER, -- for sorting/filtering
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- Interior Types (Optional - Gold already has interior and interior_code)
-- CREATE TABLE interior_types (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(100) NOT NULL UNIQUE,
--     level INTEGER, -- 1=basic, 2=medium, 3=luxury
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

--==============================================================================
-- MAIN PROPERTIES TABLE
--==============================================================================

CREATE TABLE properties (
    -- Primary Key (from Gold: id as STRING -> map to UUID or keep as VARCHAR)
    id VARCHAR(255) PRIMARY KEY, -- Keep as VARCHAR to match Gold exactly

    -- Source Info (from Gold)
    source VARCHAR(50) NOT NULL, -- 'batdongsan', 'chotot', etc.
    url TEXT,

    -- Basic Info (from Gold)
    title TEXT NOT NULL,
    description TEXT,
    location TEXT, -- full location text from Gold
    data_type VARCHAR(50), -- from Gold: data_type

    -- Location (Foreign Keys from Gold)
    province_id INTEGER REFERENCES provinces(id),
    district_id INTEGER REFERENCES districts(id),
    ward_id INTEGER REFERENCES wards(id),
    street_id INTEGER REFERENCES streets(id),

    -- Location (Text Fields from Gold for search flexibility)
    province VARCHAR(100), -- from Gold: province
    district VARCHAR(100), -- from Gold: district
    ward VARCHAR(100), -- from Gold: ward
    street VARCHAR(200), -- from Gold: street

    -- Geographic (from Gold)
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),

    -- Property Details
    area DECIMAL(10, 2), -- m2 (from Gold: area)
    living_size DECIMAL(10, 2), -- m2 built area (from Gold: living_size)
    width DECIMAL(10, 2), -- width in meters (from Gold: width)
    length DECIMAL(10, 2), -- length in meters (from Gold: length)
    facade_width DECIMAL(10, 2), -- facade width (from Gold: facade_width)
    road_width DECIMAL(10, 2), -- road width (from Gold: road_width)
    floor_count INTEGER, -- number of floors (from Gold: floor_count)
    bedroom DECIMAL(8, 2), -- bedrooms (from Gold: bedroom)
    bathroom DECIMAL(8, 2), -- bathrooms (from Gold: bathroom)

    -- Pricing
    price BIGINT, -- VND (from Gold: price)
    price_per_m2 BIGINT, -- VND/m2 (from Gold: price_per_m2)

    -- Additional Info (from Gold - text values and their category codes)
    legal_status VARCHAR(100), -- text from Gold
    legal_status_code INTEGER, -- category code from Gold
    house_direction VARCHAR(50), -- text from Gold
    house_direction_code INTEGER, -- category code from Gold
    interior VARCHAR(100), -- text from Gold (interior_type)
    interior_code INTEGER, -- category code from Gold
    house_type VARCHAR(100), -- text from Gold
    house_type_code INTEGER, -- category code from Gold

    -- Features (JSON for flexibility)
    features JSONB DEFAULT '{}',

    -- Status
    is_active BOOLEAN DEFAULT true,
    is_verified BOOLEAN DEFAULT false,

    -- Timestamps (from Gold)
    posted_date TIMESTAMP, -- from Gold: posted_date
    crawl_timestamp TIMESTAMP, -- from Gold: crawl_timestamp
    processing_timestamp TIMESTAMP, -- from Gold: processing_timestamp

    -- Processing metadata (from Gold)
    data_quality_score DECIMAL(3, 2), -- from Gold: data_quality_score
    processing_id VARCHAR(255), -- from Gold: processing_id

    -- Serving layer timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    UNIQUE(source, id), -- unique combination of source and Gold ID
    CHECK (price IS NULL OR price >= 0),
    CHECK (area IS NULL OR area > 0),
    CHECK (floor_count IS NULL OR floor_count >= 0),
    CHECK (bedroom IS NULL OR bedroom >= 0),
    CHECK (bathroom IS NULL OR bathroom >= 0)
);

--==============================================================================
-- INDEXES FOR PERFORMANCE
--==============================================================================

-- Streets Indexes (thêm indexes bị thiếu từ table definition)
CREATE INDEX idx_streets_name ON streets USING gin(name gin_trgm_ops);
CREATE INDEX idx_streets_location ON streets(province_id, district_id, ward_id);

-- Location Indexes
CREATE INDEX idx_properties_location ON properties(province_id, district_id, ward_id);
CREATE INDEX idx_properties_street ON properties(street_id) WHERE street_id IS NOT NULL;
CREATE INDEX idx_properties_coordinates ON properties(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Search Indexes (sửa field names cho đúng)
CREATE INDEX idx_properties_title ON properties USING gin(title gin_trgm_ops);
CREATE INDEX idx_properties_description ON properties USING gin(description gin_trgm_ops) WHERE description IS NOT NULL;
CREATE INDEX idx_properties_location_text ON properties USING gin((province || ' ' || district || ' ' || ward || ' ' || street) gin_trgm_ops);

-- Filter Indexes (sửa field names)
CREATE INDEX idx_properties_house_type ON properties(house_type_code) WHERE house_type_code IS NOT NULL;
CREATE INDEX idx_properties_price ON properties(price) WHERE price IS NOT NULL;
CREATE INDEX idx_properties_area ON properties(area) WHERE area IS NOT NULL;
CREATE INDEX idx_properties_price_per_m2 ON properties(price_per_m2) WHERE price_per_m2 IS NOT NULL;
CREATE INDEX idx_properties_bedroom ON properties(bedroom) WHERE bedroom IS NOT NULL;
CREATE INDEX idx_properties_bathroom ON properties(bathroom) WHERE bathroom IS NOT NULL;

-- Compound Indexes for common queries
CREATE INDEX idx_properties_type_location ON properties(house_type_code, province_id, district_id);
CREATE INDEX idx_properties_price_range ON properties(house_type_code, province_id, price) WHERE price IS NOT NULL;

-- Time-based Indexes
CREATE INDEX idx_properties_created_at ON properties(created_at);
CREATE INDEX idx_properties_processing_timestamp ON properties(processing_timestamp) WHERE processing_timestamp IS NOT NULL;

-- JSONB Index for features
CREATE INDEX idx_properties_features ON properties USING gin(features);

-- Source tracking
CREATE INDEX idx_properties_source ON properties(source, is_active);

--==============================================================================
-- UPDATED_AT TRIGGER
--==============================================================================

-- Function to update updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for properties table
CREATE TRIGGER update_properties_updated_at
    BEFORE UPDATE ON properties
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

--==============================================================================
-- SAMPLE DATA INSERTS (Optional)
--==============================================================================

-- Property Types
INSERT INTO property_types (name, category) VALUES
('Nhà riêng', 'residential'),
('Chung cư', 'residential'),
('Biệt thự', 'residential'),
('Nhà mặt phố', 'commercial'),
('Đất nền', 'land'),
('Kho xưởng', 'commercial'),
('Văn phòng', 'commercial');

-- Legal Status
INSERT INTO legal_statuses (name, description) VALUES
('Sổ đỏ/Sổ hồng', 'Có giấy chứng nhận quyền sử dụng đất'),
('Hợp đồng mua bán', 'Có hợp đồng mua bán hợp lệ'),
('Giấy tờ khác', 'Các loại giấy tờ pháp lý khác');

-- House Directions
INSERT INTO house_directions (name, angle_degrees) VALUES
('Đông', 90),
('Tây', 270),
('Nam', 180),
('Bắc', 0),
('Đông Nam', 135),
('Đông Bắc', 45),
('Tây Nam', 225),
('Tây Bắc', 315);

-- Interior Types
INSERT INTO interior_types (name, level) VALUES
('Cơ bản', 1),
('Đầy đủ', 2),
('Cao cấp', 3),
('Sang trọng', 3);

--==============================================================================
-- COMMENTS
--==============================================================================

COMMENT ON TABLE properties IS 'Main properties table for Django real estate backend';
COMMENT ON COLUMN properties.features IS 'JSON field for flexible property features (parking, elevator, etc.)';
COMMENT ON COLUMN properties.price_per_m2 IS 'Calculated field: price / area';
COMMENT ON INDEX idx_properties_location_text IS 'Full-text search index for location names';
COMMENT ON INDEX idx_properties_features IS 'GIN index for JSON features search';
