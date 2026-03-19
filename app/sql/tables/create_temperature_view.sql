CREATE OR REPLACE VIEW temperature_data AS

-- DMI
SELECT
    value AS temperature,
    observed_at,
    pulled_at,
    'DMI' AS source,
    'outside' AS location
FROM "DMI"
WHERE parameter_id = 'temperature'

UNION ALL

-- DS18B20 sensor
SELECT
    temperature AS temperature,
    observed_at,
    pulled_at,
    'DS18B20' AS source,
    location
FROM "DS18B20"

UNION ALL

-- BME280 sensor
SELECT
    temperature AS temperature,
    observed_at,
    pulled_at,
    'BME280' AS source,
    location
FROM "BME280"

UNION ALL

-- SCD41 sensor
SELECT
    temperature AS temperature,
    observed_at,
    pulled_at,
    'SCD41' AS source,
    'inside' AS location
FROM "SCD41";