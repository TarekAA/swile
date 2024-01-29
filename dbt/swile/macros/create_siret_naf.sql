{% macro create_siret_naf_staging_table(table_name) %}

    {% set query %}
    CREATE TABLE {{ schema }}.{{ table_name }} (
        siret VARCHAR(14),
        naf_code VARCHAR(6)
    );
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}