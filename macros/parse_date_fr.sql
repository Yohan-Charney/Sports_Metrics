{% macro parse_date_fr(field) %}
    {% if target.type == 'bigquery' %}
        coalesce(
            SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim({{ field }}), 'oct.', 'Oct'),'nov.', 'Nov'),'déc.', 'Dec')),
            SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim({{ field }}), 'janv.', 'Jan'),'févr.', 'Feb'),'avr.', 'Apr')),
            SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim({{ field }}), 'mai', 'May'),'juin', 'Jun'),'juil.', 'Jul')),
            SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim({{ field }}), 'août', 'Aug'),'sept.', 'Sep'),'mars', 'Mar'))
        )
    {% elif target.type == 'snowflake' %}
        coalesce(
            TRY_TO_DATE(replace(replace(replace(trim({{ field }}), 'oct.', 'Oct'),'nov.', 'Nov'),'déc.', 'Dec'), 'Mon DD, YYYY'),
            TRY_TO_DATE(replace(replace(replace(trim({{ field }}), 'janv.', 'Jan'),'févr.', 'Feb'),'avr.', 'Apr'), 'Mon DD, YYYY'),
            TRY_TO_DATE(replace(replace(replace(trim({{ field }}), 'mai', 'May'),'juin', 'Jun'),'juil.', 'Jul'), 'Mon DD, YYYY'),
            TRY_TO_DATE(replace(replace(replace(trim({{ field }}), 'août', 'Aug'),'sept.', 'Sep'),'mars', 'Mar'), 'Mon DD, YYYY')
        )
    {% endif %}
{% endmacro %}