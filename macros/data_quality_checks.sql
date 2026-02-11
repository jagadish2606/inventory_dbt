{% macro check_positive_value(column_name) %}
    -- Ensure value is positive
    {{ column_name }} > 0
{% endmacro %}

{% macro check_not_null(column_name) %}
    -- Ensure column is not null
    {{ column_name }} IS NOT NULL
{% endmacro %}

{% macro check_valid_date(column_name) %}
    -- Ensure date is valid and not in the future
    {{ column_name }} <= CURRENT_DATE()
    AND {{ column_name }} IS NOT NULL
{% endmacro %}
