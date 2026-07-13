-- macro: get_brazil_region
-- Reusable macro to classify Brazilian states into macro-regions.
-- Usage: {{ get_brazil_region('column_name') }}
-- Eliminates the repeated CASE WHEN blocks across dim_customers, dim_sellers, dim_geography.

{% macro get_brazil_region(state_column) %}
    CASE
        WHEN {{ state_column }} IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN {{ state_column }} IN ('PR', 'SC', 'RS') THEN 'Sul'
        WHEN {{ state_column }} IN ('BA', 'PE', 'CE', 'MA', 'PB', 'RN', 'AL', 'SE', 'PI') THEN 'Nordeste'
        WHEN {{ state_column }} IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN {{ state_column }} IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
        ELSE 'Desconhecido'
    END
{% endmacro %}
