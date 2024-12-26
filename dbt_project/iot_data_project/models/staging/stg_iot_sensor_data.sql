{{ config(
    schema = 'staging',
    materialized = 'incremental'
) }}

WITH updated_rows AS (

    SELECT
        iot.sensor_id,
        iot.sensor_type,
        iot.value,
        iot.timestamp,
        iot.latitude,
        iot.longitude,
        iot.city,
        iot.state,
        iot.device_model,
        iot.manufacturer,
        iot.firmware_version,
        iot.energy_usage,
        iot.weather_conditions,
        iot.duration,
        iot.severity,
        COALESCE(
            iot.postal_code,
            pc.postal_code
        ) AS postal_code
    FROM
        {{ source(
            'raw',
            'iot_sensor_data'
        ) }}
        iot
        LEFT JOIN {{ source(
            'raw',
            'tbl_postal_code_mapping'
        ) }}
        pc
        ON ROUND(
            iot.latitude,
            2
        ) = ROUND(
            pc.latitude,
            2
        )
        AND ROUND(
            iot.longitude,
            2
        ) = ROUND(
            pc.longitude,
            2
        )
    WHERE
        iot.postal_code IS NULL
        AND pc.postal_code IS NOT NULL
)
SELECT
    *
FROM
    updated_rows

{% if is_incremental() %}
WHERE
    TIMESTAMP > (
        SELECT
            MAX(TIMESTAMP)
        FROM
            {{ this }}
    )
{% endif %}
