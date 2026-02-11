{% macro check_negative_sales() %}
    {% set query %}
        SELECT 
            metric_date,
            store_id,
            product_id,
            sales_revenue,
            stock_out_qty,
            transaction_count
        FROM {{ ref('fct_daily_inventory_metrics') }}
        WHERE sales_revenue < 0
        LIMIT 10
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {{ log("Records with negative sales revenue:", info=true) }}
        {% for row in results %}
            {{ log(row, info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
