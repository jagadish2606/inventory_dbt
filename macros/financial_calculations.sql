{% macro calculate_profit(revenue, cost) %}
    -- Calculate profit
    COALESCE({{ revenue }}, 0) - COALESCE({{ cost }}, 0)
{% endmacro %}

{% macro calculate_margin(revenue, cost) %}
    -- Calculate profit margin percentage
    CASE
        WHEN {{ revenue }} > 0
        THEN ROUND(({{ revenue }} - {{ cost }}) * 100.0 / {{ revenue }}, 2)
        ELSE 0
    END
{% endmacro %}

{% macro calculate_revenue_per_sqft(revenue, sqft) %}
    -- Calculate revenue per square foot
    CASE
        WHEN {{ sqft }} > 0
        THEN ROUND({{ revenue }} / {{ sqft }}, 2)
        ELSE 0
    END
{% endmacro %}
