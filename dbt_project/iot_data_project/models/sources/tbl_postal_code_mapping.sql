SELECT
    *
FROM
    {{ source(
        'raw',
        'tbl_postal_code_mapping'
    ) }}
