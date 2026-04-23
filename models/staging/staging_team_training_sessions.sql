{{ config(
    materialized='view',
) }}

with donnees_source as (
    select *
    from {{ source('Sports_Metrics', 'team_training_sessions') }}
),

pi as (
    select *
    from {{ ref('Players_info') }}
),

nettoyage as (
    select
        SESSION_ID as session_id,
        {{ safe_cast('d.PLAYER_ID', 'int64') }} as player_id,
        {{ safe_cast('NEXT_MATCH_ID', 'int64') }} as Next_Match_ID,
        {{ safe_cast('SESSION_DATE', 'timestamp') }}
        {% if target.type == 'snowflake' %}::date{% endif %} as session_date,
        {{ safe_cast('DURATION_MIN', 'float64') }} as Duration_min,
        coalesce({{ safe_cast('HEART_RATE', 'float64') }}, avg(HEART_RATE) over(partition by pi.Age, pi.position)) as Heart_rate,
        coalesce({{ safe_cast('STRENGTH_SCORE', 'float64') }}, avg(STRENGTH_SCORE) over(partition by pi.Age, pi.position)) as Strength_Score,
        coalesce({% if target.type == 'snowflake' %}"SHOOTING_ACCURACY_%"::float{% else %}safe_cast(`Shooting_Accuracy_%` as float64){% endif %},
            avg({% if target.type == 'snowflake' %}"SHOOTING_ACCURACY_%"{% else %}`Shooting_Accuracy_%`{% endif %}) over(partition by pi.Age, pi.position)
)       as Shooting_Accuracy_pct,
        {% if target.type == 'snowflake' %}"PASSING_ACCURACY_%"::float{% else %}safe_cast(`Passing_Accuracy_%` as float64){% endif %} as Passing_Accuracy_pct,
        {{ safe_cast('FOCUS_LEVEL', 'float64') }} as Focus_Level,
        {{ safe_cast('WEEKLY_TRAINING_HOURS', 'float64') }} as Weekly_Training_Hours,
        coalesce({{ safe_cast('LOAD_INTENSITY_SCORE', 'float64') }}, avg(LOAD_INTENSITY_SCORE) over(partition by pi.Age, pi.position)) as Load_Intensity_Score,
        coalesce(FATIGUE_LEVEL, 'Low') as Fatigue_Level,
        coalesce({{ safe_cast('INJURY_RISK', 'int64') }}, 0) as Injury_Risk,
        coalesce(INJURY_RISK_LEVEL, 'Low') as Injury_Risk_Level,
        {{ safe_cast('RECOVERY_TIME_HOURS', 'float64') }} as Recovery_Time_hours,
        coalesce({{ safe_cast('PERFORMANCE_SCORE', 'float64') }}, avg(PERFORMANCE_SCORE) over(partition by pi.Age, pi.position)) as Performance_Score,
        {{ safe_cast('DAYS_BEFORE_MATCH', 'int64') }} as Days_Before_Match
    from donnees_source d
    left join pi on pi.player_id = d.PLAYER_ID
    where d.PLAYER_ID is not null
      and pi.Age is not null
      and SESSION_ID is not null
      and DAYS_BEFORE_MATCH is not null
      and RECOVERY_TIME_HOURS is not null
      and DURATION_MIN is not null
)

select
    case
        {% if target.type == 'snowflake' %}
            when session_date <= '2020-07-31'::date then '2019-2020'
            when session_date <= '2021-07-31'::date then '2020-2021'
            when session_date <= '2022-07-31'::date then '2021-2022'
            when session_date <= '2023-07-31'::date then '2022-2023'
        {% else %}
            when date(session_date) <= date '2020-07-31' then '2019-2020'
            when date(session_date) <= date '2021-07-31' then '2020-2021'
            when date(session_date) <= date '2022-07-31' then '2021-2022'
            when date(session_date) <= date '2023-07-31' then '2022-2023'
        {% endif %}
        else '2023-2024'
    end as Season, n.*
from nettoyage n