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

last_training as (
    -- Isolation de la SEULE dernière session d'entraînement avant chaque match
    -- Le ROW_NUMBER évite de dupliquer les stats de match si le joueur a eu plusieurs entraînements
select *
    from (select sts.*, row_number() over (partition by sts.player_id, sts.Next_Match_ID order by sts.session_date desc, sts.session_id desc
            ) as row_n
        from sts
    )
    where row_n = 1
),

player_perfomance as (
    -- Assemblage de la performance technique, des données physiques et du contexte
select
    cm.Season,
    tps.game_id,
    tps.player_id,
    cm.game_date,
    cm.annee,
    cm.mois,
    cm.jour,

    -- Données biométriques (pour répondre à la problématique Taille/Poids vs Rebonds)
    pi.player_name,
    pi.Age,
    pi.Height_cm,
    pi.Weight_kg,
    pi.Position,

    -- Score de force issu du dernier entraînement
    lt.Strength_Score as Strength_Score_last_training,

    -- Contexte du match et résultat collectif
    cm.Place,
    stg.win_loss,
    stg.Total_points,
    cm.oppenent,
    mg.Oppenent_points,
    mg.Ecart,

    -- Joueur
    Start_position,
    minutes_played,
    round( minutes_played / 48 ,2) as minutes_ratio,

    -- Statistiques techniques classiques
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

    -- CALCUL DU SCORE DE PERFORMANCE (EFFICIENCE) :
    -- Formule : (Pts + Reb + Ast + Stl + Blk) - (Balles perdues + Fautes)
    (Points + tps.Total_rebounds + tps.Assists + tps.Steals + tps.Blocks - tps.Turnover - tps.Player_fault)
    as Performance_score_match,

    -- Score de performance ramené à la minute (très important pour comparer les remplaçants et les titulaires)
    round((Points + tps.Total_rebounds + tps.Assists + tps.Steals + tps.Blocks - tps.Turnover - tps.Player_fault)/NULLIF(minutes_played, 0),2) -- Sécurité pour le score par minute
    as Performance_score_match_min,
    tps.Plus_minus

from tps
join cm using (game_id)
join pi using (player_id)
join stg using (game_id)
join mg using (game_id)

-- Jointure clé avec le dernier entraînement pour corréler Force et Performance
join last_training lt on tps.player_id = lt.player_id and tps.game_id = lt.Next_Match_ID
)

select * from player_perfomance