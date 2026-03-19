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
    -- Calcul de l'état de forme collectif :
    -- On fait la moyenne du niveau de fatigue de tous les joueurs avant chaque match
    select 
        Season,
        Next_Match_ID,
        avg(Fi_before_match) as Fi_team
    from fi
    group by Next_Match_ID,Season

),

games as (
-- Assemblage final : On croise le calendrier, les stats et la fatigue équipe
    select
        cm.Season,
        stg.game_id,
        cm.game_date,
        cm.jour,
        cm.mois,
        cm.annee,
        cm.place,
        cm.oppenent,
        round(f.Fi_team,2) as Fi_before_match_team,
        stg.win_loss,
        stg.total_points,
        -- Calcul du score adverse basé sur l'écart final
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
    -- Jointure sur le calendrier pour avoir le contexte du match (adversaire, date)
    inner join cm on stg.game_id = cm.game_id
    -- Jointure avec la fatigue collective calculée plus haut
    join Fi_equipe f on f.Next_Match_ID = stg.game_id
-- Tri chronologique pour l'analyse de progression sur la saison
    order by annee asc, mois asc, jour asc

)

select * 
from games
qualify row_number() over(partition by game_id order by game_date desc) = 1