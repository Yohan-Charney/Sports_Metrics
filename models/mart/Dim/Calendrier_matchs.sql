{{ config(
    materialized='table',
) }}

with donnees_source as (

    select *
    from {{ source('Sports_Metrics', 'team_games_dataset') }}

),

nettoyage as (

    select
        GAME_ID as game_id,
        coalesce( SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim(GAME_DATE), 'oct.', 'Oct'),'nov.', 'Nov'),'déc.', 'Dec')),
                  SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim(GAME_DATE), 'janv.', 'Jan'),'févr.', 'Feb'),'avr.', 'Apr')),
                  SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim(GAME_DATE), 'mai', 'May'),'juin', 'Jun' ),'juil.', 'Jul')),
                  SAFE.PARSE_DATE('%b %d, %Y', replace(replace(replace(trim(GAME_DATE), 'août', 'Aug'),'sept.', 'Sep'),'mars', 'Mar'))
        ) as game_date,

        case
            when MATCHUP like '%vs.%' then 'Domicile'
            when MATCHUP like '%@%' then 'Exterieur'
            else null
        end as Place,

        case
            when MATCHUP like '%vs.%' then trim(split(MATCHUP, 'vs.')[safe_offset(1)])
            when MATCHUP like '%@%' then trim(split(MATCHUP, '@')[safe_offset(1)])
            else null
        end as Oppenent

    from donnees_source
    where GAME_ID is not null

)

select * from nettoyage