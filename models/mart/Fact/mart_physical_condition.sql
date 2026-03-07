{{ config(
    materialized='table',

) }}

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
        avg(fatigue_index_score) over(
            partition by fi.player_id
            order by fi.session_date
            range between interval 7 day preceding and current row
        ) as fi_avg_7d,

        max(fatigue_index_score) over(
            partition by fi.player_id
            order by fi.session_date
            range between interval 7 day preceding and current row
        ) as fi_max_7d,

        sum(sts.Duration_min) over(
            partition by fi.player_id
            order by fi.session_date
            range between interval 7 day preceding and current row
        ) as training_duration_7d,

-- accumulation fatigue sur 28 jour

        avg(fatigue_index_score) over(
            partition by fi.player_id
            order by fi.session_date
            range between interval 28 day preceding and current row
        ) as fi_avg_28d,

        max(fatigue_index_score) over(
            partition by fi.player_id
            order by fi.session_date
            range between interval 28 day preceding and current row
        ) as fi_max_28d,

        sum(sts.Duration_min) over(
            partition by fi.player_id
            order by fi.session_date
            range between interval 28 day preceding and current row
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

),


select
    fi.player_id,
    fi.session_id,
    fi.session_date,
    annee
    mois
    jour

    player_name

-- charge dernier entrainement avant match 
    fatigue_index_score as fi_last_training,
    Recovery_score as rs_last_training,
    recovery_needed_hours as recovery_needed_last_training,
    fi_interpretation as fi_interpretation_last_training

-- Stats dernier entraînement
    Focus_Level
    Strength_Score
    Shooting_Accuracy_pct
    Passing_Accuracy_pct
    Performance_Score
    
-- charge sur 7 avant le prochain match
    fs.fi_avg_7d,
    fs.fi_max_7d,
    fs.training_duration_7d,

-- charge sur 28 jours avant le prochain match
    fs.fi_avg_28d,
    fs.fi_max_28d,
    fs.training_duration_28d,

-- interpretation
    chi.training_load

-- performance match suivant
    Place
    Oppenent
    win_loss
    Total_points
    Oppenent_points
    Ecart
    Points
    fg_pct
    fg3_pct
    Total_rebounds
    Assists
    Turnover
    Player_fault

from fi 
join fatigue_stats fs using (player_id, session_id, session_date)
join Ch_interpretation chi using (player_id, session_id, session_date)




