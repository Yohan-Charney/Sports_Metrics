{{ config(
    materialized='table',

) }}


-- Objectif: voir la charge a l'entraînement avant le dernier match et 
-- les performance du match qui suit cette entrainement

with sts as (
    select *
    from {{ ref('staging_team_training_sessions') }}
),

fi as (
    select *
    from {{ ref('int_fatigue_index_fi') }}
),

cm as (
    select *
    from {{ ref('Calendrier_matchs') }}
),

pi as (
    select *
    from {{ ref('Players_info') }}
),

mp as (
    select *
    from {{ ref('mart_player') }}
),

tps as (
    select *
    from {{ ref('staging_team_players_stats') }}
),


fatigue_stats as (

    select
        fi.player_id,
        fi.session_id,
        fi.session_date,


-- accumulation fatigue sur 7 jour
        round(avg(fatigue_index_score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 7 preceding and current row
        ),2) as fi_avg_7d,

        max(fatigue_index_score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 7 preceding and current row
        ) as fi_max_7d,


        sum(sts.Duration_min * sts.Load_Intensity_Score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 7 preceding and current row
        ) as training_load_7d,

-- accumulation fatigue sur 28 jour

        round(avg(fatigue_index_score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 28 preceding and current row
        ),2) as fi_avg_28d,

        max(fatigue_index_score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 28 preceding and current row
        ) as fi_max_28d,


        sum(sts.Duration_min * sts.Load_Intensity_Score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 28 preceding and current row
        ) as training_load_28d

    from fi
    join sts using (session_id)
)



select
    mp.Season,
    fi.player_id,
    fi.session_id,
    sts.Next_Match_ID,

   
    mp.player_name,

-- charge dernier entrainement avant match 
    fi.fatigue_index_score as fi_last_training,
    fi.Recovery_score as rs_last_training,
    fi.recovery_needed_hours as recovery_needed_last_training,

-- Charge avant match
    fi.Fi_before_match,
    
-- Stats dernier entraînement
    sts.Focus_Level,
    sts.Strength_Score,
    sts.Shooting_Accuracy_pct,
    sts.Passing_Accuracy_pct,
    sts.Performance_Score,
    sts.Load_Intensity_Score,
    sts.Injury_Risk,
    
-- charge sur 7 jours avant le prochain match (fatigue_stats)
    fs.fi_avg_7d,
    fs.fi_max_7d,

-- charge sur 28 jours avant le prochain match
    fs.fi_avg_28d,
    fs.fi_max_28d,


-- Statistiques de performance réelles lors du match qui a suivi l'entrainement
    mp.Place,
    mp.Oppenent,
    mp.win_loss,
    mp.Position,
    mp.Start_position,
    mp.Total_points,
    mp.Oppenent_points,
    mp.Ecart,
    mp.minutes_played,
    mp.Points,
    mp.fg_pct,
    mp.fg3_pct,
    mp.Total_rebounds,
    mp.Assists,
    mp.Steals,
    mp.Blocks,
    mp.Turnover,
    mp.Player_fault,
    mp.Performance_score_match,
    mp.Performance_score_match_min,

    mp.Plus_minus

from sts 
join mp on mp.game_id = sts.Next_Match_ID and mp.player_id = sts.player_id
join fatigue_stats fs on sts.player_id = fs.player_id and sts.session_id = fs.session_id and sts.session_date = fs.session_date
join fi on sts.player_id = fi.player_id and sts.session_id = fi.session_id and sts.session_date = fi.session_date
