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
        extract(year from date(game_date)) as annee,
        extract(month from date(game_date)) as mois,
        extract(day from date(game_date)) as jour,

        case
            when Matchup like '%vs.%' then 'Domicile'
            when Matchup like '%@%' then 'Exterieur'
            else null
        end as Place,

        case
            when Matchup like '%vs.%' then trim(split(Matchup, 'vs.')[safe_offset(1)])
            when Matchup like '%@%' then trim(split(Matchup, '@')[safe_offset(1)])
            else null
        end as Oppenent

    from donnees_source
    where game_id is not null

)

select * from nettoyage