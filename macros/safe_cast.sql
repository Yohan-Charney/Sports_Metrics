{% macro safe_cast(field, type) %}
    {% if target.type == 'bigquery' %}
        safe_cast({{ field }} as {{ type }})
    {% elif target.type == 'snowflake' %}
        {% if type == 'int64' %}
            {{ field }}::integer
        {% elif type == 'float64' %}
            {{ field }}::float
        {% elif type == 'timestamp' %}
            {{ field }}::timestamp
        {% else %}
            {{ field }}::{{ type }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro split_part(field, delimiter, part) %}
    {% if target.type == 'bigquery' %}
        safe_cast(split({{ field }}, '{{ delimiter }}')[safe_offset({{ part - 1 }})] as float64)
    {% elif target.type == 'snowflake' %}
        SPLIT_PART({{ field }}, '{{ delimiter }}', {{ part }})::float
    {% endif %}
{% endmacro %}

{% macro cast_int(field) %}
    {% if target.type == 'bigquery' %}
        cast({{ field }} as int64)
    {% elif target.type == 'snowflake' %}
        cast({{ field }} as integer)
    {% endif %}
{% endmacro %}

{% macro date_to_int(field) %}
    {% if target.type == 'bigquery' %}
        unix_date({{ field }})
    {% elif target.type == 'snowflake' %}
        datediff('day', '1970-01-01'::date, {{ field }})
    {% endif %}
{% endmacro %}