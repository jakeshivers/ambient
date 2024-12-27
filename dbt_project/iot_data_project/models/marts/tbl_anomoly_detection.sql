{{ config(
    schema = 'marts'
) }}

WITH value_stats AS (

    SELECT
        sensor_type,
        AVG(VALUE) AS avg_value,
        STDDEV(VALUE) AS stddev_value
    FROM
        {{ ref('stg_iot_sensor_data') }}
    GROUP BY
        sensor_type
),
anomalies AS (
    SELECT
        s.*,
        CASE
            WHEN s.value > v.avg_value + 2.0 * v.stddev_value THEN 'High'
            WHEN s.value <= v.avg_value - 2.0 * v.stddev_value THEN 'Low'
            ELSE 'Normal'
        END AS value_anomaly,
        v.avg_value + 3 * v.stddev_value val
    FROM
        {{ ref('stg_iot_sensor_data') }}
        s
        JOIN value_stats v
        ON s.sensor_type = v.sensor_type
)
SELECT
    *
FROM
    anomalies
WHERE
    value_anomaly != 'Normal'
