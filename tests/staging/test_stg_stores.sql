-- Test for stg_stores
WITH source_data AS (
    SELECT * FROM {{ ref('stg_stores') }}
)

SELECT
    store_id,
    store_name,
    city
FROM source_data
WHERE 1=1
    -- Check for nulls
    AND (store_id IS NULL
         OR store_name IS NULL
         OR city IS NULL)
UNION ALL
SELECT
    store_id,
    store_name,
    city
FROM source_data
WHERE 1=1
    -- Check for valid dates
    AND opening_date > CURRENT_DATE()
UNION ALL
SELECT
    store_id,
    store_name,
    city
FROM source_data
WHERE 1=1
    -- Check for valid store size
    AND store_size_sqft <= 0
