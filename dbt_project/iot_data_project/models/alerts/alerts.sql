{{ config(
    schema = 'alerts',
    materialized = 'incremental'
) }}

WITH water_leak_high_severity AS (

    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'water_leak'
        AND severity = 0
    UNION ALL
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'water_leak'
        AND severity > 0.2
),
high_light_value AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'light'
        AND VALUE > 500
),
high_temperature AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'temperature'
        AND ROUND((VALUE * 9 / 5), 0) + 32 > 80
),
poor_air_quality AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'air_quality'
        AND VALUE > 100
),
high_co2_levels AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'co2_sensor'
        AND VALUE > 850
),
high_humidity AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
    WHERE
        sensor_type = 'humidity'
        AND VALUE > 50
)
SELECT
    *
FROM
    water_leak_high_severity
UNION ALL
SELECT
    *
FROM
    high_light_value
UNION ALL
SELECT
    *
FROM
    high_temperature
UNION ALL
SELECT
    *
FROM
    poor_air_quality
UNION ALL
SELECT
    *
FROM
    high_co2_levels
UNION ALL
SELECT
    *
FROM
    high_humidity
