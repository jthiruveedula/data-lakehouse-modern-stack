{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro get_layer_path(layer, table_name) %}
    gs://{{ env_var('GCS_BUCKET') }}/{{ layer }}/{{ table_name }}
{% endmacro %}
