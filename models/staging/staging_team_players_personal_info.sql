{{ config(
    materialized='table'
) }}

with donnees_source as (

    select *
    from {{ source('Sports_Metrics','team_players_personal_info') }}

),

nettoyage as (

select

    PLAYER_ID as player_id,
    PLAYER_NAME as player_name,
    FIRST_NAME as first_name,
    LAST_NAME as last_name,
    date(safe_cast(BIRTHDATE as timestamp)) as Birthdate,
    AGE as Age,
    coalesce(HEIGHT_CM, avg(HEIGHT_CM) over(partition by AGE)) as Height_cm,
    coalesce(WEIGHT_KG,avg(WEIGHT_KG) over(partition by AGE)) as Weight_kg,
    coalesce(nullif(POSITION,''), case when coalesce(HEIGHT_CM, avg(HEIGHT_CM) over(partition by AGE)) > 200 then 'Forward' 
                                       when coalesce(HEIGHT_CM, avg(HEIGHT_CM) over(partition by AGE)) > 0 then 'Guard'
                                       else 'Inconnu'
    end) as Position,
    SCHOOL,
    COUNTRY,
    safe_cast(SEASON_EXP as int64) as Season_exp

from donnees_source
where PLAYER_ID is not null
    and BIRTHDATE is not null
    and AGE is not null

)

select * from nettoyage