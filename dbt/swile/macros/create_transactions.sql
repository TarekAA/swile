{% macro create_transaction_staging_table(schema_name, table_name) %}

    {% set query %}
    CREATE TABLE {{ schema }}.{{ table_name }} (

        id UUID PRIMARY KEY,
        type VARCHAR(255),
        amount NUMERIC(10, 2),
        status VARCHAR(50),
        created_at TIMESTAMP WITH TIME ZONE,
        wallet_id UUID,
        siret VARCHAR(14)
    );
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}