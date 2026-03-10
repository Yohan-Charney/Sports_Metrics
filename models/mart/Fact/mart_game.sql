{{ config(
    materialized='table',

) }}

with stg as (

    select *
    from {{ ref('staging_team_games_dataset') }}

),


fi as (

    select *
    from {{ ref('int_fatigue_index_fi') }}

),

cm as (

    select *
    from {{ ref('Calendrier_matchs') }}

),

Fi_equipe as (
    
    select 
        Next_Match_ID,
        avg(Fi_before_match) as Fi_team
    from fi
    group by Next_Match_ID

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
        f.Fi_team as Fi_before_match_team,
        stg.win_loss,
        stg.total_points,
        stg.total_points - stg.ecart as Oppenent_points,
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
    inner join cm on stg.game_id = cm.game_id
    join Fi_equipe f on f.Next_Match_ID = stg.game_id

    order by annee asc, mois asc, jour asc

)

select * from games