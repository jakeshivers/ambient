SELECT
    *
FROM
    {{ source(
        'raw',
        'iot_sensor_data'
    ) }}
