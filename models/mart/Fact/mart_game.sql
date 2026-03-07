{{ config(
    materialized='table',

) }}

with stg as (

    select *
    from {{ ref('staging_team_games_dataset') }}

),

cm as (

    select *
    from {{ ref('Calendrier_matchs') }}

),

games as (

    select
        stg.game_id,
        cm.game_date,
        cm.jour,
        cm.mois,
        cm.annee,
        cm.place,
        cm.oppenent,
        stg.win_loss,
        stg.total_points,
        stg.total_points + stg.ecart as Oppenent_points,
        stg.Ecart,
        stg.total_minutes,
        stg.total_field_goal_made,
        stg.total_field_goal_attempt,
        stg.fg_pct,
        stg.total_field_goal_3pts_made,
        stg.total_field_goal_3pts_attempt,
        stg.fg3_pct,
        stg.total_free_throws_made,
        stg.total_free_throws_attempt,
        stg.ft_pct,
        stg.total_offensive_rebounds,
        stg.total_defensive_rebounds,
        stg.total_total_rebounds,
        stg.total_assists,
        stg.total_steals,
        stg.total_blocks,
        stg.total_turnover,
        stg.total_player_fault,
        stg.win,
        stg.loss,
        stg.win_pct
    from stg
    inner join cm
        on stg.game_id = cm.game_id

    order by annee asc, mois asc, jour asc

)

select * from games