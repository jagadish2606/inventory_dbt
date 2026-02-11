-- Test for stg_inventory_transactions
WITH source_data AS (
    SELECT * FROM {{ ref('stg_inventory_transactions') }}
)

SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_type
FROM source_data
WHERE 1=1
    -- Check for nulls in critical fields
    AND (transaction_id IS NULL
         OR store_id IS NULL
         OR product_id IS NULL
         OR transaction_type IS NULL)
UNION ALL
SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_type
FROM source_data
WHERE 1=1
    -- Check for valid transaction types
    AND transaction_type NOT IN ('STOCK_IN', 'SALE', 'RETURN', 'ADJUSTMENT')
