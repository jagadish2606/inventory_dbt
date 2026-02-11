{% macro check_negative_profit() %}
    {% set query %}
        SELECT 
            store_id,
            store_name,
            lifetime_revenue,
            lifetime_cost,
            lifetime_profit
        FROM {{ ref('dim_store_performance') }}
        WHERE lifetime_profit < 0
        LIMIT 10
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {{ log("Stores with negative profit:", info=true) }}
        {% for row in results %}
            {{ log(row, info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
