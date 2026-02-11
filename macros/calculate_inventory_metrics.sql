{% macro calculate_sell_through_rate(stock_out_qty, closing_stock) %}
    -- Calculate sell-through rate percentage
    ROUND(
        {{ stock_out_qty }} * 100.0
        / NULLIF({{ stock_out_qty }} + {{ closing_stock }}, 0),
        2
    )
{% endmacro %}

{% macro calculate_days_of_coverage(closing_stock, stock_out_qty) %}
    -- Calculate days of inventory coverage
    CASE
        WHEN {{ closing_stock }} > 0 AND {{ stock_out_qty }} > 0
        THEN ROUND({{ closing_stock }} / {{ stock_out_qty }}, 1)
        ELSE NULL
    END
{% endmacro %}

{% macro determine_stock_status(current_quantity, min_stock, reorder_point, max_stock) %}
    -- Determine stock status based on inventory levels
    CASE
        WHEN {{ current_quantity }} <= {{ min_stock }} THEN 'CRITICALLY_LOW'
        WHEN {{ current_quantity }} <= {{ reorder_point }} THEN 'LOW_STOCK'
        WHEN {{ current_quantity }} >= {{ max_stock }} THEN 'OVERSTOCKED'
        ELSE 'OPTIMAL'
    END
{% endmacro %}

{% macro calculate_days_of_supply(current_quantity, avg_daily_sales) %}
    -- Calculate estimated days of supply
    CASE
        WHEN {{ avg_daily_sales }} > 0
        THEN ROUND({{ current_quantity }} / {{ avg_daily_sales }}, 1)
        ELSE 0
    END
{% endmacro %}
