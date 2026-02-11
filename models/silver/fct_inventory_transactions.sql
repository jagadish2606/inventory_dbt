{{
    config(
        materialized = 'view',
        tags = ['silver', 'cleaned']
    )
}}

SELECT
    t.transaction_id,
    t.store_id,
    t.product_id,
    t.product_name,
    t.transaction_type,
    t.quantity_change,
    t.unit_cost,
    t.selling_price,
    t.transaction_date,
    t.transaction_date_only,

    -- Financial calculations
    t.quantity_change * t.unit_cost     AS cost_value_change,
    t.quantity_change * t.selling_price AS retail_value_change,

    -- Business logic - updated for your transaction types
    CASE
        WHEN t.quantity_change > 0 THEN 'STOCK_INCREASE'
        WHEN t.quantity_change < 0 THEN 'STOCK_DECREASE'
        ELSE 'NO_CHANGE'
    END AS stock_movement_type,

    CASE
        WHEN t.transaction_type = 'SALE' THEN 'REVENUE_GENERATING'
        WHEN t.transaction_type = 'RETURN' THEN 'REVENUE_RETURN'
        WHEN t.transaction_type = 'ADJUSTMENT' THEN 'INVENTORY_ADJUSTMENT'
        WHEN t.transaction_type = 'STOCK_IN' THEN 'STOCK_RECEIPT'
        ELSE 'OTHER'
    END AS transaction_category,

    -- Date parts
    EXTRACT(YEAR  FROM t.transaction_date) AS transaction_year,
    EXTRACT(MONTH FROM t.transaction_date) AS transaction_month,
    EXTRACT(DAY   FROM t.transaction_date) AS transaction_day,
    EXTRACT(HOUR  FROM t.transaction_date) AS transaction_hour,

    -- Metadata
    t.employee_id,
    t.reason,
    t.supplier_id,
    t.batch_number,
    t.source_system

FROM {{ ref('stg_inventory_transactions') }} t
WHERE t.quantity_change <> 0
