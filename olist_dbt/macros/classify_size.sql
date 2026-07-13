-- macro: classify_size
-- Reusable product size classification based on volume.
-- Usage: {{ classify_size('volume_column') }}

{% macro classify_size(volume_column) %}
    CASE
        WHEN {{ volume_column }} IS NULL THEN 'unknown'
        WHEN {{ volume_column }} < 1000 THEN 'small'
        WHEN {{ volume_column }} < 10000 THEN 'medium'
        WHEN {{ volume_column }} < 50000 THEN 'large'
        ELSE 'extra_large'
    END
{% endmacro %}
