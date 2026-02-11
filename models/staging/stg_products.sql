{{
    config(
        materialized = 'view',
        -- schema = 'staging',
        tags = ['staging']
    )
}}

SELECT
    CAST(product_id AS STRING)             AS product_id,
    INITCAP(TRIM(product_name))            AS product_name,
    INITCAP(TRIM(category))                AS category,
    INITCAP(TRIM(subcategory))             AS subcategory,
    TRIM(brand)                            AS brand,
    CAST(supplier_id AS STRING)            AS supplier_id,
    CAST(cost_price AS DECIMAL(10,2))      AS cost_price,
    CAST(retail_price AS DECIMAL(10,2))    AS retail_price,
    CAST(min_stock_level AS INT)           AS min_stock_level,
    CAST(max_stock_level AS INT)           AS max_stock_level,
    CAST(reorder_point AS INT)             AS reorder_point,
    CAST(is_active AS BOOLEAN)             AS is_active
FROM {{ source('raw', 'products_raw') }}
WHERE product_id IS NOT NULL
