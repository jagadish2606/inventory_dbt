-- Test for view_low_stock_alerts
WITH alerts_data AS (
    SELECT * FROM {{ ref('view_low_stock_alerts') }}
)

SELECT
    store_name,
    product_name,
    available_units,
    alert_severity
FROM alerts_data
WHERE 1=1
    -- Check alert severity logic (only for low stock items)
    AND stock_status IN ('CRITICALLY_LOW', 'LOW_STOCK')
    AND ((available_units <= minimum_required AND alert_severity != 'URGENT - Order immediately')
         OR (available_units <= reorder_level AND available_units > minimum_required AND alert_severity != 'Warning - Reorder soon'))
UNION ALL
SELECT
    store_name,
    product_name,
    available_units,
    alert_severity
FROM alerts_data
WHERE 1=1
    -- Check for non-low stock items in alert view
    AND stock_status NOT IN ('CRITICALLY_LOW', 'LOW_STOCK')
