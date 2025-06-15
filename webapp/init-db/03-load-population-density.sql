-- Load province population density data
-- This file loads the population density data for feature engineering

-- First load the CSV data into temp table
CREATE TEMP TABLE temp_population_density (
    province VARCHAR(100),
    average_population_thousand DECIMAL(10,2),
    population_density DECIMAL(10,2),
    area_km2 DECIMAL(10,2),
    province_id_temp DECIMAL(5,1)
);

-- Copy data from CSV file
COPY temp_population_density(province, average_population_thousand, population_density, area_km2, province_id_temp)
FROM '/docker-entrypoint-initdb.d/province_population_density.csv'
DELIMITER ','
CSV HEADER;

-- Insert into the main table, matching with existing provinces
INSERT INTO province_population_density (
    province_id,
    province_name,
    average_population_thousand,
    population_density,
    area_km2
)
SELECT
    p.id as province_id,
    temp.province as province_name,
    temp.average_population_thousand,
    temp.population_density,
    temp.area_km2
FROM temp_population_density temp
JOIN provinces p ON (
    -- Match by name (handling different formats)
    UPPER(TRIM(p.name)) = UPPER(TRIM(temp.province))
    OR UPPER(TRIM(p.name)) = UPPER(TRIM(REPLACE(temp.province, 'TP.', 'Thành phố ')))
    OR UPPER(TRIM(REPLACE(p.name, 'Thành phố ', 'TP.'))) = UPPER(TRIM(temp.province))
    -- Special case for Ho Chi Minh City
    OR (UPPER(TRIM(p.name)) LIKE '%HỒ CHÍ MINH%' AND UPPER(TRIM(temp.province)) LIKE '%HỒ CHÍ MINH%')
)
ON CONFLICT (province_id) DO UPDATE SET
    province_name = EXCLUDED.province_name,
    average_population_thousand = EXCLUDED.average_population_thousand,
    population_density = EXCLUDED.population_density,
    area_km2 = EXCLUDED.area_km2;

-- Show results
SELECT
    p.name as province_name,
    ppd.population_density,
    ppd.average_population_thousand
FROM provinces p
LEFT JOIN province_population_density ppd ON p.id = ppd.province_id
ORDER BY ppd.population_density DESC NULLS LAST
LIMIT 10;

-- Drop temp table
DROP TABLE temp_population_density;
