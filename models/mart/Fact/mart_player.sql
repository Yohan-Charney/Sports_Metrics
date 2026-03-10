{{ config(
    materialized='table',

) }}

with stg as (
    select *
    from {{ ref('staging_team_games_dataset') }}
),

sts as (
    select *
    from {{ ref('staging_team_training_sessions') }}
),

mg as (
    select *
    from {{ ref('mart_game') }}
),

cm as (
    select *
    from {{ ref('Calendrier_matchs') }}
),

pi as (
    select *
    from {{ ref('Players_info') }}
),

tps as (
    select *
    from {{ ref('staging_team_players_stats') }}
),

player_perfomance as (

select
    tps.game_id,
    tps.player_id,
    cm.game_date,
    cm.annee,
    cm.mois,
    cm.jour,


    pi.player_name,
    pi.Age,
    pi.Height_cm,
    pi.Weight_kg,
    pi.Position,

    sts.Strength_Score as Strength_Score_last_training, 

    cm.Place,
    stg.win_loss,
    stg.Total_points,
    cm.oppenent,
    mg.Oppenent_points,
    mg.Ecart,

    Start_position,
    minutes_played,
    round( minutes_played / 48 ,2) as minutes_ratio,
    Points,
    tps.FG_PCT,
    tps.FG3_PCT,
    tps.FT_PCT,
    tps.Total_rebounds,
    tps.Assists,
    tps.Steals,
    tps.Blocks,
    tps.Turnover,
    tps.Player_fault,
    (Points + tps.Total_rebounds + tps.Assists + tps.Steals + tps.Blocks - tps.Turnover - tps.Player_fault)
    as Performance_score_match,
    round((Points + tps.Total_rebounds + tps.Assists + tps.Steals + tps.Blocks - tps.Turnover - tps.Player_fault)/minutes_played,2)
    as Performance_score_match_min,
    tps.Plus_minus

from tps
join cm using (game_id)
join pi using (player_id)
join stg using (game_id)
join mg using (game_id)
join sts on tps.game_id = sts.Next_Match_ID

)

select * from player_perfomance