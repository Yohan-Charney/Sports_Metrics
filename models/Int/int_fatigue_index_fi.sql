with sts as (
  select *
  from {{ ref('staging_team_training_sessions') }}
),

spi as (
  select *
  from {{ ref('staging_team_players_personal_info') }}
),

normalisation as (

    select
        sts.Season,	
        sts.session_date,
        sts.session_id,
        sts.player_id,

        Recovery_Time_hours,
        Days_Before_Match,

        (Heart_Rate / (220 - spi.Age)) as HR_norm,

        ( case 
            when Fatigue_Level = 'Low' then 1
            when Fatigue_Level = 'Medium' then 2
            else 3
        end
        ) / 3 as Fatigue_Level_norm,

        round((Days_Before_Match * 24)/Recovery_Time_hours,2) as Recovery_score,
        Load_Intensity_Score / 10 as Load_Intensity_norm,
        Weekly_Training_Hours / ((max(Duration_min) over() /60) * 7) as Weekly_Training_norm

    from sts
    join spi 
        on spi.player_id = sts.player_id
),

fatigue_calc as (

    select
        Season,	
        session_date,
        session_id,
        player_id,
        Recovery_score,

        -- composantes FI
          (0.30 * (0.6 * HR_norm + 0.4 * Fatigue_Level_norm ) -- Charge inerne
        + 0.40 * ( 0.7 * Load_Intensity_norm + 0.3 * Weekly_Training_norm ) -- Charge externe
        + 0.30 * (1 - (least(1, Recovery_score)) -- recovery adj
        ) )* 100 as fatigue_index_score

    from normalisation

),

fatigue_index_fi as (

    select
        f.Season,	
        f.session_date,
        f.session_id,
        f.player_id,
        f.Recovery_score,
        round(fatigue_index_score, 2) as fatigue_index_score,

        case
            when fatigue_index_score <= 30 then 'Fraîcheur optimale'
            when fatigue_index_score <= 50 then 'Fatigue légère'
            when fatigue_index_score <= 65 then 'Fatigue modérée'
            when fatigue_index_score <= 80 then 'Fatigue élevée / Risque'
            else 'Danger blessure / baisse performance'
        end as Fi_interpretation,

        cast(case 
            when n.Recovery_Time_hours > n.days_before_match * 24 then (n.Recovery_Time_hours - n.days_before_match * 24)
            else 0
        end as int64) as Recovery_needed_hours -- indique le besoin supplémentaire en récuperation d'ici le prochain match

    from fatigue_calc f
    join normalisation n using(player_id, session_id) 

)

select * from fatigue_index_fi