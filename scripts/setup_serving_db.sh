#!/bin/bash

# Setup PostgreSQL Serving Database
# Run this script before running load_to_serving.py

set -e

# Database configuration
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
DB_NAME=${DB_NAME:-"realestate"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"password"}

# PostgreSQL connection string
export PGPASSWORD=$DB_PASSWORD

echo "🏗️ Setting up PostgreSQL serving database..."

# 1. Create database if not exists
echo "📦 Creating database '$DB_NAME' if not exists..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME;" 2>/dev/null || echo "Database already exists"

# 2. Run Django schema
echo "🗃️ Creating database schema..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f /home/fer/data/real_estate_project/database/schemas/django_schema.sql

# 3. Create indexes for performance
echo "🚀 Creating additional indexes..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << EOF
-- Additional performance indexes
CREATE INDEX IF NOT EXISTS idx_properties_source_id ON properties(source, id);
CREATE INDEX IF NOT EXISTS idx_properties_created_at ON properties(created_at);
CREATE INDEX IF NOT EXISTS idx_properties_processing_timestamp ON properties(processing_timestamp);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_properties_location_price ON properties(province_id, district_id, price) WHERE price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_properties_type_price ON properties(house_type_code, price) WHERE price IS NOT NULL AND house_type_code IS NOT NULL;

-- Text search indexes
CREATE INDEX IF NOT EXISTS idx_properties_title_search ON properties USING gin(to_tsvector('english', title));
CREATE INDEX IF NOT EXISTS idx_properties_description_search ON properties USING gin(to_tsvector('english', description)) WHERE description IS NOT NULL;

ANALYZE properties;
EOF

echo "✅ Database setup completed successfully!"

# 4. Show database info
echo "📊 Database information:"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT
    schemaname,
    tablename,
    attname as column_name,
    typname as data_type
FROM pg_tables t
JOIN pg_attribute a ON a.attrelid = (schemaname||'.'||tablename)::regclass
JOIN pg_type ty ON a.atttypid = ty.oid
WHERE schemaname = 'public' AND tablename = 'properties'
ORDER BY attnum;
"

echo "🎯 Ready for data loading!"
echo ""
echo "Usage:"
echo "  python load_to_serving.py --date 2024-12-01"
echo "  python load_to_serving.py --property-type all"
