{% macro performance_score(points, rebounds, assists, steals, blocks, turnovers, faults) %}
    ({{ points }} + {{ rebounds }} + {{ assists }} + {{ steals }} + {{ blocks }} - {{ turnovers }} - {{ faults }})
{% endmacro %}