--gpto3 for help one how a macro would work for sources
{% macro get_nba_source() %}
    {# Set a variable 'live_mode' (default false) #}
    {% set live_mode = var('live_mode', false) %}
    {% if live_mode | as_bool %}
       {{ source('maddata_db', 'nba_live_data') }}
    {% else %}
       {{ source('maddata_db', 'nba_raw_data') }}
    {% endif %}
{% endmacro %}