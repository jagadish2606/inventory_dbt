{{
    config(
        materialized = 'view',
        -- schema = 'staging',
        tags = ['staging']
    )
}}

SELECT
    CAST(transaction_id AS STRING)        AS transaction_id,
    CAST(store_id AS STRING)              AS store_id,
    CAST(product_id AS STRING)            AS product_id,
    TRIM(product_name)                    AS product_name,
    UPPER(TRIM(transaction_type))         AS transaction_type,
    CAST(quantity_change AS INT)          AS quantity_change,
    CAST(unit_cost AS DECIMAL(10,2))      AS unit_cost,
    CAST(selling_price AS DECIMAL(10,2))  AS selling_price,
    CAST(transaction_date AS TIMESTAMP)   AS transaction_date,
    DATE(transaction_date)                AS transaction_date_only,
    CAST(employee_id AS STRING)           AS employee_id,
    TRIM(reason)                          AS reason,
    CAST(supplier_id AS STRING)           AS supplier_id,
    CAST(batch_number AS STRING)          AS batch_number,
    CAST(source_system AS STRING)          AS source_system
FROM {{ source('raw', 'inventory_transactions_raw') }}
WHERE transaction_date IS NOT NULL
  AND product_id IS NOT NULL
  AND store_id IS NOT NULL
