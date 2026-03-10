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


last_session as (

select * from (

    select
        player_id,
        session_id,
        session_date,
        Next_Match_ID as game_id,

        row_number() over (
            partition by player_id, Next_Match_ID
            order by session_date desc
        ) as row_n

    from sts

            )

where row_n = 1

),



fatigue_stats as (

    select
        fi.player_id,
        fi.session_id,
        ls.game_id,
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

        sum(sts.Duration_min) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 28 preceding and current row
        ) as training_duration_28d,

        sum(sts.Duration_min * sts.Load_Intensity_Score) over(
            partition by fi.player_id
            order by unix_date(fi.session_date)
            range between 28 preceding and current row
        ) as training_load_28d

    from fi
    join sts using (session_id)
    join last_session ls using (session_id)

),

Ch_interpretation as (

select 
    training_load_7d,
    training_load_28d,
    player_id,
    session_id,

    case 
         when training_load_7d / (training_load_28d / 4) < 0.8 then 'Sous-entraînement'
         when training_load_7d / (training_load_28d / 4) < 1.3 then 'Charge normale'
         when training_load_7d / (training_load_28d / 4) < 1.5 then 'Charge élevée'
         when (training_load_7d / (training_load_28d / 4) > 1.5) or (fi_avg_7d > 70) then 'Surentraînement'
         else 'Manque de données'
    end as training_load

from fatigue_stats

)



-- assemblage as (
select
    mp.Season,
    fi.player_id,
    fi.session_id,
    mp.game_id,

    mp.annee,
    mp.mois,
    mp.jour,
    mp.player_name,

-- charge dernier entrainement avant match 
    fi.fatigue_index_score as fi_last_training,
    fi.Recovery_score as rs_last_training,
    fi.recovery_needed_hours as recovery_needed_last_training,
    fi.fi_interpretation as fi_interpretation_last_training,


-- Charge avant match
    fi.Fi_before_match,
    case
            when fi.Fi_before_match <= 10 then 'complètement récupéré'
            when fi.Fi_before_match <= 30 then 'légère fatigue avant match'
            when fi.Fi_before_match <= 60 then 'fatigue modérée avant match'
            when fi.Fi_before_match <= 80 then 'Fatigue élevée / Risque'
            else 'Danger blessure / baisse performance'
        end as Fi_interpretation_before_match,

-- Stats dernier entraînement
    sts.Focus_Level,
    sts.Strength_Score,
    sts.Shooting_Accuracy_pct,
    sts.Passing_Accuracy_pct,
    sts.Performance_Score,
    sts.Load_Intensity_Score,
    sts.Injury_Risk,
    
-- charge sur 7 avant le prochain match
    fs.fi_avg_7d,
    fs.fi_max_7d,
    fs.training_duration_7d,

-- charge sur 28 jours avant le prochain match
    fs.fi_avg_28d,
    fs.fi_max_28d,
    fs.training_duration_28d,

-- interpretation
    chi.training_load_7d,
    chi.training_load_28d,
    chi.training_load,

-- performance match suivant
    mp.Place,
    mp.Oppenent,
    mp.win_loss,
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

from mp 
join sts on mp.game_id = sts.Next_Match_ID and mp.player_id = sts.player_id
join fatigue_stats fs on sts.player_id = fs.player_id and sts.session_id = fs.session_id and sts.session_date = fs.session_date
join Ch_interpretation chi on sts.player_id = chi.player_id and sts.session_id = chi.session_id 
join fi on sts.player_id = fi.player_id and sts.session_id = fi.session_id and sts.session_date = fi.session_date


/*doublons as (select *
            from (
                select *, row_number() over (partition by a.player_id, a.game_id order by a.game_id
            ) as row_n
        from assemblage a
    )
    where row_n = 1
)

select * from doublons*/




