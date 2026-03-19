CREATE OR REPLACE VIEW humidity_data AS

-- DMI
SELECT
    value AS humidity,
    observed_at,
    pulled_at,
    'DMI' AS source,
    'outside' AS location
FROM "DMI"
WHERE parameter_id = 'humidity'

UNION ALL

-- BME280 sensor
SELECT
    humidity,
    observed_at,
    pulled_at,
    'BME280' AS source,
    location
FROM "BME280"

UNION ALL

-- SCD41 sensor
SELECT
    humidity AS humidity,
    observed_at,
    pulled_at,
    'SCD41' as source,
    'inside' as location
FROM "SCD41";