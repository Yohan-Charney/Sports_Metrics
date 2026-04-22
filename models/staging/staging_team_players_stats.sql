{{ config(
    materialized='view',
) }}

with donnees_source as (

    select *
    from {{ source('Sports_Metrics', 'team_players_stats') }}

),

nettoyage as (

    select
        {{ safe_cast('GAME_ID', 'int64') }} as game_id,
        {{ safe_cast('PLAYER_ID', 'int64') }} as player_id,

        case
            when trim(START_POSITION) = '' or START_POSITION is null then 'Bench'
            else trim(START_POSITION)
        end as Start_position,

        case 
               when MIN like '%:%' then round(({{ split_part('MIN', ':', 1) }} + {{ split_part('MIN', ':', 2) }} / 60), 2)
               else {{ safe_cast('MIN', 'float64') }} -- au cas qlqun rentre des minutes sans secondes
        end as Minutes_played, -- conversion MIN (MM:SS) -> minutes décimales

        {{ safe_cast('PTS', 'int64') }} as Points,
        {{ safe_cast('FGM', 'int64') }} as Field_goal_made,
        {{ safe_cast('FGA', 'int64') }} as Field_goal_attempt,
        {{ safe_cast('FG_PCT', 'float64') }} as FG_PCT,

        {{ safe_cast('FG3M', 'int64') }} as Field_goal_3pts_made,
        {{ safe_cast('FG3A', 'int64') }} as Field_goal_3pts_attempt,
        {{ safe_cast('FG3_PCT', 'float64') }} as FG3_PCT,

        {{ safe_cast('FTM', 'int64') }} as Free_throws_made,
        {{ safe_cast('FTA', 'int64') }} as Free_throws_attempt,
        {{ safe_cast('FT_PCT', 'float64') }} as FT_PCT,

        {{ safe_cast('OREB', 'int64') }} as Offensive_rebounds,
        {{ safe_cast('DREB', 'int64') }} as Defensive_rebounds,
        {{ safe_cast('REB', 'int64') }} as Total_rebounds,

        {{ safe_cast('AST', 'int64') }} as Assists,
        {{ safe_cast('STL', 'int64') }} as Steals,
        {{ safe_cast('BLK', 'int64') }} as Blocks,
        {{ safe_cast('"TO"', 'int64') }} as Turnover,
        {{ safe_cast('PF', 'int64') }} as Player_fault,

        {{ safe_cast('PLUS_MINUS', 'float64') }} as Plus_minus

    from donnees_source
    where COMMENT = '' -- Ici je supprime les joueurs non sélectionnés par le coach
),


doublons as ( 
    select * from nettoyage 
    where player_id is not null 
    qualify row_number() over ( partition by player_id, game_id order by player_id desc ) = 1
)

select * from doublons
