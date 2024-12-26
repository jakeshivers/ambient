{{ config(
    schema = 'marts',
    materialized = 'incremental'
) }}

SELECT
    sensor_type,
    COUNT(*) AS total_sensors,
    AVG(VALUE) AS avg_value,
    MAX(TIMESTAMP) AS last_reported
FROM
    {{ ref('stg_iot_sensor_data') }}
GROUP BY
    sensor_type
ORDER BY
    total_sensors DESC
