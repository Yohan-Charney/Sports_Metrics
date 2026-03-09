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

        sum(sts.Duration_min) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 7 preceding and current row
        ) as training_duration_7d,

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

        sum(sts.Duration_min) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 28 preceding and current row
        ) as training_duration_28d

    from fi
    join sts using (session_id)

),

Ch_interpretation as (

select 

    player_id,
    session_id,
    session_date,

    case 
         when training_duration_7d / (training_duration_28d / 4) < 0.8 then 'Sous-entraînement'
         when training_duration_7d / (training_duration_28d / 4) < 1.3 then 'Charge normale'
         when training_duration_7d / (training_duration_28d / 4) < 1.5 then 'Charge élevée'
         when (training_duration_7d / (training_duration_28d / 4) > 1.5) or (fi_avg_7d > 70) then 'Surentraînement'
         else 'Manque de données'
    end as training_load

from fatigue_stats

)

select
    fi.player_id,
    fi.session_id,
    fi.session_date,

    mp.annee,
    mp.mois,
    mp.jour,
    mp.player_name,

-- charge dernier entrainement avant match 
    Load_Intensity_Score as last_Load_Intensity_Score,
    fi.fatigue_index_score as fi_last_training,
    fi.Recovery_score as rs_last_training,
    fi.recovery_needed_hours as recovery_needed_last_training,
    fi.fi_interpretation as fi_interpretation_last_training,

-- Stats dernier entraînement
    Focus_Level,
    Strength_Score,
    Shooting_Accuracy_pct,
    Passing_Accuracy_pct,
    Performance_Score,
    
-- charge sur 7 avant le prochain match
    fs.fi_avg_7d,
    fs.fi_max_7d,
    fs.training_duration_7d,

-- charge sur 28 jours avant le prochain match
    fs.fi_avg_28d,
    fs.fi_max_28d,
    fs.training_duration_28d,

-- interpretation
    chi.training_load,

-- performance match suivant
    mp.Place,
    mp.Oppenent,
    mp.win_loss,
    mp.Total_points,
    mp.Oppenent_points,
    mp.Ecart,
    mp.Points,
    mp.fg_pct,
    mp.fg3_pct,
    mp.Total_rebounds,
    mp.Assists,
    mp.Steals,
    mp.Blocks,
    mp.Turnover,
    mp.Player_fault,
    mp.Plus_minus

from sts 
join fatigue_stats fs using (player_id, session_id, session_date)
join Ch_interpretation chi using (player_id, session_id, session_date)
join fi using (player_id, session_id, session_date)
join mp on mp.game_id = sts.Next_Match_ID and mp.player_id = sts.player_id




