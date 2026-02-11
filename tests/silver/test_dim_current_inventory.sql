-- Test for dim_current_inventory
WITH inventory_data AS (
    SELECT * FROM {{ ref('dim_current_inventory') }}
)

SELECT
    store_id,
    product_id,
    current_quantity,
    stock_status
FROM inventory_data
WHERE 1=1
    -- Check for negative inventory
    AND current_quantity < 0
