{{
    config(
        materialized = 'view',
        -- schema = 'staging',
        tags = ['staging']
    )
}}

SELECT
    CAST(store_id AS STRING)          AS store_id,
    INITCAP(TRIM(store_name))         AS store_name,
    TRIM(address)                     AS address,
    INITCAP(TRIM(city))               AS city,
    UPPER(TRIM(state))                AS state,
    TRIM(zip_code)                    AS zip_code,
    INITCAP(TRIM(country))            AS country,
    CAST(store_size_sqft AS INT)      AS store_size_sqft,
    CAST(opening_date AS DATE)        AS opening_date,
    CAST(manager_id AS STRING)        AS manager_id,
    CAST(is_active AS BOOLEAN)        AS is_active
FROM {{ source('raw', 'stores_raw') }}
WHERE store_id IS NOT NULL
