{% macro get_env_database(layer) -%}
    {#
        Returns the environment-specific database name for a given layer.

        Args:
            layer: The medallion layer name (landing, staging, intermediate, reporting)

        Returns:
            Database name with environment suffix (e.g., landing_dev, staging_prod)

        Usage in models:
            {{ config(database=get_env_database('landing')) }}
    #}

    {%- set env = var('env', 'dev') -%}
    {{ layer }}_{{ env }}

{%- endmacro %}
