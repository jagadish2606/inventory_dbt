-- Test for stg_products
WITH source_data AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    product_name,
    category
FROM source_data
WHERE 1=1
    -- Check for nulls
    AND (product_id IS NULL
         OR product_name IS NULL
         OR category IS NULL)
UNION ALL
SELECT
    product_id,
    product_name,
    category
FROM source_data
WHERE 1=1
    -- Check for valid prices
    AND (cost_price <= 0
         OR retail_price <= 0
         OR retail_price < cost_price)
UNION ALL
SELECT
    product_id,
    product_name,
    category
FROM source_data
WHERE 1=1
    -- Check for valid stock levels
    AND (min_stock_level < 0
         OR max_stock_level <= 0
         OR reorder_point < min_stock_level
         OR reorder_point > max_stock_level)
