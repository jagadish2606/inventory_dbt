-- Test for fct_inventory_transactions
WITH transaction_data AS (
    SELECT * FROM {{ ref('fct_inventory_transactions') }}
)

SELECT
    transaction_id,
    transaction_type,
    quantity_change,
    cost_value_change
FROM transaction_data
WHERE 1=1
    -- Check financial calculations
    AND ABS(cost_value_change - (quantity_change * unit_cost)) > 0.01
UNION ALL
SELECT
    transaction_id,
    transaction_type,
    quantity_change,
    cost_value_change
FROM transaction_data
WHERE 1=1
    -- Check retail value calculations
    AND ABS(retail_value_change - (quantity_change * selling_price)) > 0.01
UNION ALL
SELECT
    transaction_id,
    transaction_type,
    quantity_change,
    cost_value_change
FROM transaction_data
WHERE 1=1
    -- Check business logic
    AND (transaction_type = 'SALE' AND quantity_change > 0)
    OR (transaction_type = 'RETURN' AND quantity_change < 0)
