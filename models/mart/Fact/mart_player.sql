{{ config(
    materialized='table',

) }}

with stg as (
    select *
    from {{ ref('staging_team_games_dataset') }}
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
    pi.Heigth_cm,
    pi.Weight_kg,
    pi.Position,

    
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
    tps.Plus_minus

from tps
join cm using (game_id)
join pi using (player_id)
join stg using (game_id)
join mg using (game_id)

)

select * from player_perfomance