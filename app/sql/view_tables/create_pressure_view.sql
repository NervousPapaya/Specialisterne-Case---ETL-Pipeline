CREATE OR REPLACE VIEW pressure_data AS

-- DMI
SELECT
    value AS pressure,
    observed_at,
    pulled_at,
    'DMI' AS source,
    'outside' AS location
FROM "raw"."DMI"
WHERE parameter_id = 'pressure'

UNION ALL

-- BME280 sensor
SELECT
    pressure AS pressure,
    observed_at,
    pulled_at,
    'BME280' AS source,
    location
FROM "raw"."BME280";