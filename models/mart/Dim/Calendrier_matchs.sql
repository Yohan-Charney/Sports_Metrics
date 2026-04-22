{{ config(
    materialized='table',
) }}

with donnees_source as (
    select *
    from {{ ref('staging_team_games_dataset') }}
),

nettoyage as (
    select
        game_id,
        game_date,
        case
            {% if target.type == 'snowflake' %}
            when game_date <= '2020-07-31'::date then '2019-2020'
            when game_date <= '2021-07-31'::date then '2020-2021'
            when game_date <= '2022-07-31'::date then '2021-2022'
            when game_date <= '2023-07-31'::date then '2022-2023'
            {% else %}
            when game_date <= date '2020-07-31' then '2019-2020'
            when game_date <= date '2021-07-31' then '2020-2021'
            when game_date <= date '2022-07-31' then '2021-2022'
            when game_date <= date '2023-07-31' then '2022-2023'
            {% endif %}
            else '2023-2024'
        end as Season,
        extract(year from game_date) as annee,
        extract(month from game_date) as mois,
        extract(day from game_date) as jour,
        case
            when Matchup like '%vs.%' then 'Domicile'
            when Matchup like '%@%' then 'Exterieur'
            else null
        end as Place,
        case
            {% if target.type == 'snowflake' %}
            when Matchup like '%vs.%' then trim(SPLIT_PART(Matchup, 'vs.', 2))
            when Matchup like '%@%' then trim(SPLIT_PART(Matchup, '@', 2))
            {% else %}
            when Matchup like '%vs.%' then trim(split(Matchup, 'vs.')[safe_offset(1)])
            when Matchup like '%@%' then trim(split(Matchup, '@')[safe_offset(1)])
            {% endif %}
            else null
        end as Oppenent
    from donnees_source
    where game_id is not null
)

select * from nettoyage