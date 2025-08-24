{% macro clean_numeric(expr) %}
nullif(regexp_replace({{ expr }}, '[^0-9.-]', '', 'g'),'')::numeric
{% endmacro %}