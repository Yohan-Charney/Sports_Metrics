{{ config(
    materialized='table',

) }}

with sts as (
  select *
  from {{ ref('staging_team_training_sessions') }}
),

tps as (
    select *
    from {{ ref('staging_team_players_stats') }}
),

spi as (
  select *
  from {{ ref('staging_team_players_personal_info') }}
),

normalisation as (
-- ÉTAPE 1 : Normalisation des données pour les rendre comparables (entre 0 et 1)
    select
        sts.Season,	
        sts.session_date,
        sts.session_id,
        sts.player_id,
        sts.Next_Match_ID,
        tps.minutes_played,

        Recovery_Time_hours,
        Days_Before_Match,

        (Heart_Rate / (220 - spi.Age)) as HR_norm,

        ( case 
            when Fatigue_Level = 'Low' then 1
            when Fatigue_Level = 'Medium' then 2
            else 3
        end
        ) / 3 as Fatigue_Level_norm,
-- Score de récupération : Ratio temps disponible vs temps de repos nécessaire
        round((Days_Before_Match * 24)/Recovery_Time_hours,2) as Recovery_score,
        Load_Intensity_Score / 10 as Load_Intensity_norm,

-- Volume d'entraînement hebdo normalisé par rapport au max de l'équipe
        Weekly_Training_Hours / ((max(Duration_min) over() /60) * 7) as Weekly_Training_norm

    from sts
    join spi 
        on spi.player_id = sts.player_id
    join tps on sts.player_id = tps.player_id and sts.Next_Match_ID = tps.game_id -- On lie la session au match spécifique
    where tps.minutes_played >= 5 -- j'enlève les joueurs du garbage time
),

fatigue_calc as (
-- ÉTAPE 2 : Calcul du Fatigue Index (FI) avec pondération métier
    select
        Season,	
        session_date,
        session_id,
        player_id,
        Next_Match_ID,
        Recovery_score,


        -- FORMULE SPORTMETRICS :
        -- 30% Charge Interne (Physiologie)
        -- 40% Charge Externe (Intensité de la séance)
        -- 30% Récupération (Sommeil/Repos)
          (0.30 * (0.6 * HR_norm + 0.4 * Fatigue_Level_norm ) -- Charge interne
        + 0.40 * ( 0.7 * Load_Intensity_norm + 0.3 * Weekly_Training_norm ) -- Charge externe
        + 0.30 * (1 - (least(1, Recovery_score)) -- recovery adj
        ) )* 100 as fatigue_index_score

    from normalisation

),

fatigue_index_fi as (
-- ÉTAPE 3 : Interprétation et calcul du besoin de repos
    select
        f.Season,	
        f.session_date,
        f.session_id,
        f.Next_Match_ID,
        f.player_id,
        f.Recovery_score,
        round(fatigue_index_score, 2) as fatigue_index_score,
    -- Traduction du score en conseils pour le staff médical
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
-- ÉTAPE FINALE : Projection de la fatigue à l'instant T du match
select 
    f.Season,	
    f.session_date,
    f.session_id,
    f.player_id,
    n.Next_Match_ID,

    f.Recovery_score,
    f.fatigue_index_score,
    f.Fi_interpretation,
    f.Recovery_needed_hours,

    -- Fi_before_match : Estimation de la fatigue résiduelle juste avant le coup d'envoi
    round(least (100, ( f.fatigue_index_score * f.recovery_needed_hours / (n.days_before_match * 24) )),2) as Fi_before_match
    


from fatigue_index_fi f
join normalisation n using(player_id,session_id)