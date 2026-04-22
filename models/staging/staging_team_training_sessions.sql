{{ config(
    materialized='view',
) }}

-- Objectif :
-- Nettoyer team_training_sessions, reconstruire les données manquantes,
-- supprimer les colonnes inutiles, uniformiser les formats

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

        d."Session_ID" as session_id,
        {{ safe_cast('d."Player_ID"', 'int64') }} as player_id,
        {{ safe_cast('"Next_Match_ID"', 'int64') }} as Next_Match_ID,
        {{ safe_cast('"Session_Date"', 'timestamp') }}::date as session_date,
        {{ safe_cast('"Duration_min"', 'float64') }} as Duration_min,
        coalesce({{ safe_cast('"Heart_Rate"', 'float64') }}, avg("Heart_Rate") over(partition by pi.Age, pi.position)) as Heart_rate,
        coalesce({{ safe_cast('"Strength_Score"', 'float64') }}, avg("Strength_Score") over(partition by pi.Age, pi.position)) as Strength_Score,
        coalesce({{ safe_cast('"Shooting_Accuracy_%"', 'float64') }}, avg("Shooting_Accuracy_%" ) over(partition by pi.Age, pi.position)) as Shooting_Accuracy_pct,
        {{ safe_cast('"Passing_Accuracy_%"', 'float64') }} as Passing_Accuracy_pct,
        {{ safe_cast('"Focus_Level"', 'float64') }} as Focus_Level,
        {{ safe_cast('"Weekly_Training_Hours"', 'float64') }} as Weekly_Training_Hours,
        coalesce({{ safe_cast('"Load_Intensity_Score"', 'float64') }}, avg("Load_Intensity_Score") over(partition by pi.Age, pi.position)) as Load_Intensity_Score,
        coalesce("Fatigue_Level", 'Low') as Fatigue_Level,
        coalesce({{ safe_cast('"Injury_Risk"', 'int64') }}, 0) as Injury_Risk,
        coalesce("Injury_Risk_Level", 'Low') as Injury_Risk_Level,
        {{ safe_cast('"Recovery_Time_hours"', 'float64') }} as Recovery_Time_hours,
        coalesce({{ safe_cast('"Performance_Score"', 'float64') }}, avg("Performance_Score") over(partition by pi.Age, pi.position)) as Performance_Score,
        {{ safe_cast('"Days_Before_Match"', 'int64') }} as Days_Before_Match

    from donnees_source d
    left join pi on pi.player_id = d."Player_ID" -- au cas ou un joueur est dans training session mais pas encore dans player_info
    where d."Player_ID" is not null
      and pi.Age is not null
      and Session_ID is not null
      and Days_Before_Match is not null
      and Recovery_Time_hours is not null
      and Duration_min is not null

)


select 
    case
            {% if target.type == 'snowflake' %}
                when session_date <= '2020-07-31'::date then '2019-2020'
                when session_date <= '2021-07-31'::date then '2020-2021'
                when session_date <= '2022-07-31'::date then '2021-2022'
                when session_date <= '2023-07-31'::date then '2022-2023'
            {% else %}
                when session_date <= date '2020-07-31' then '2019-2020'
                when session_date <= date '2021-07-31' then '2020-2021'
                when session_date <= date '2022-07-31' then '2021-2022'
                when session_date <= date '2023-07-31' then '2022-2023'
            {% endif %}
            end as Season, n.*
from nettoyage n



