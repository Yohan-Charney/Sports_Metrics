{{ config(
    materialized='view',
) }}

with donnees_source as (

    select *
    from {{ source('Sports_Metrics', 'team_players_stats') }}

),

nettoyage as (

    select
        safe_cast(GAME_ID as int64) as game_id,
        safe_cast(PLAYER_ID as int64) as player_id,

        case
            when trim(START_POSITION) = '' or START_POSITION is null then 'Bench'
            else trim(START_POSITION)
        end as Start_position,

        case 
            when MIN like '%:%' then round((safe_cast(split(MIN, ':')[safe_offset(0)] as float64) + safe_cast(split(MIN, ':')[safe_offset(1)] as float64) / 60),2)
            else safe_cast(MIN as float64) -- au cas qlqun rentre des minutes sans secondes
        end as Minutes_played, -- conversion MIN (MM:SS) -> minutes décimales

        safe_cast(PTS as int64) as Points,
        safe_cast(FGM as int64) as Field_goal_made,
        safe_cast(FGA as int64) as Field_goal_attempt,
        safe_cast(FG_PCT as float64) as FG_PCT,

        safe_cast(FG3M as int64) as Field_goal_3pts_made,
        safe_cast(FG3A as int64) as Field_goal_3pts_attempt,
        safe_cast(FG3_PCT as float64) as FG3_PCT,

        safe_cast(FTM as int64) as Free_throws_made,
        safe_cast(FTA as int64) as Free_throws_attempt,
        safe_cast(FT_PCT as float64) as FT_PCT,

        safe_cast(OREB as int64) as Offensive_rebounds,
        safe_cast(DREB as int64) as Defensive_rebounds,
        safe_cast(REB as int64) as Total_rebounds,

        safe_cast(AST as int64) as Assists,
        safe_cast(STL as int64) as Steals,
        safe_cast(BLK as int64) as Blocks,
        safe_cast(`TO` as int64) as Turnover,
        safe_cast(PF as int64) as Player_fault,

        safe_cast(PLUS_MINUS as float64) as Plus_minus

    from donnees_source
    where COMMENT = '' -- Ici je supprime les joueurs non sélectionnés par le coach
),


doublons as ( 
    select * from nettoyage 
    where player_id is not null 
    qualify row_number() over ( partition by player_id, game_id order by player_id desc ) = 1
)

select * from doublons
