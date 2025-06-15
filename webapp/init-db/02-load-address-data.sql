-- Load Vietnam address data from JSON file
-- This script loads provinces, districts, wards, and streets data

-- Function to load address data from JSON
CREATE OR REPLACE FUNCTION load_address_data()
RETURNS void AS $$
DECLARE
    json_data jsonb;
    province_record jsonb;
    district_record jsonb;
    ward_record jsonb;
    street_record jsonb;
    province_id INTEGER;
    district_id INTEGER;
BEGIN
    -- Read JSON data from file
    SELECT pg_read_file('/docker-entrypoint-initdb.d/vietnamaddress_utf8.json')::jsonb INTO json_data;

    -- Loop through provinces
    FOR province_record IN SELECT * FROM jsonb_array_elements(json_data)
    LOOP
        -- Insert province
        INSERT INTO provinces (id, code, name)
        VALUES (
            (province_record->>'id')::INTEGER,
            province_record->>'code',
            province_record->>'name'
        )
        ON CONFLICT (id) DO UPDATE SET
            code = EXCLUDED.code,
            name = EXCLUDED.name;

        province_id := (province_record->>'id')::INTEGER;

        -- Loop through districts
        FOR district_record IN SELECT * FROM jsonb_array_elements(province_record->'districts')
        LOOP
            -- Insert district
            INSERT INTO districts (id, name, province_id)
            VALUES (
                (district_record->>'id')::INTEGER,
                district_record->>'name',
                province_id
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                province_id = EXCLUDED.province_id;

            district_id := (district_record->>'id')::INTEGER;

            -- Loop through wards
            FOR ward_record IN SELECT * FROM jsonb_array_elements(district_record->'wards')
            LOOP
                INSERT INTO wards (id, name, prefix, district_id)
                VALUES (
                    (ward_record->>'id')::INTEGER,
                    ward_record->>'name',
                    COALESCE(ward_record->>'prefix', ''),
                    district_id
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    prefix = EXCLUDED.prefix,
                    district_id = EXCLUDED.district_id;
            END LOOP;

            -- Loop through streets
            FOR street_record IN SELECT * FROM jsonb_array_elements(district_record->'streets')
            LOOP
                INSERT INTO streets (id, name, prefix, district_id)
                VALUES (
                    (street_record->>'id')::INTEGER,
                    street_record->>'name',
                    COALESCE(street_record->>'prefix', ''),
                    district_id
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    prefix = EXCLUDED.prefix,
                    district_id = EXCLUDED.district_id;
            END LOOP;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'Address data loaded successfully!';
END;
$$ LANGUAGE plpgsql;

-- Execute the function to load data
SELECT load_address_data();

-- Drop the function after use
DROP FUNCTION load_address_data();

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_districts_province_id ON districts(province_id);
CREATE INDEX IF NOT EXISTS idx_wards_district_id ON wards(district_id);
CREATE INDEX IF NOT EXISTS idx_streets_district_id ON streets(district_id);

-- Add default 'unknown' entries for data consistency
INSERT INTO provinces (id, code, name)
VALUES (-1, 'UNK', 'unknown')
ON CONFLICT (id) DO NOTHING;

INSERT INTO districts (id, name, province_id)
VALUES (-1, 'unknown', -1)
ON CONFLICT (id) DO NOTHING;

INSERT INTO wards (id, name, prefix, district_id)
VALUES (-1, 'unknown', '', -1)
ON CONFLICT (id) DO NOTHING;

INSERT INTO streets (id, name, prefix, district_id)
VALUES (-1, 'unknown', '', -1)
ON CONFLICT (id) DO NOTHING;


-- Show summary of loaded data
SELECT
    'Provinces' as table_name, COUNT(*) as count
FROM provinces
UNION ALL
SELECT
    'Districts' as table_name, COUNT(*) as count
FROM districts
UNION ALL
SELECT
    'Wards' as table_name, COUNT(*) as count
FROM wards
UNION ALL
SELECT
    'Streets' as table_name, COUNT(*) as count
FROM streets
ORDER BY table_name;
