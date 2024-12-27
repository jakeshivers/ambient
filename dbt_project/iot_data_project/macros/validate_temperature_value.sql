{% test validate_temperature_value(model) %}
SELECT
    *
FROM
    {{ ref('stg_iot_sensor_data') }}
WHERE
    sensor_type = 'temperature'
    AND (
        VALUE < 20
        OR VALUE > 100
    ) {% endtest %}
