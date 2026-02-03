{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {#
        Generates database names with environment suffix.
        Uses DBT_ENV environment variable to determine environment.

        Examples:
            - landing + dev = landing_dev
            - staging + prod = staging_prod
    #}

    {%- set default_database = target.database -%}

    {%- if custom_database_name is none -%}
        {{ default_database }}
    {%- else -%}
        {{ custom_database_name | trim }}
    {%- endif -%}

{%- endmacro %}
