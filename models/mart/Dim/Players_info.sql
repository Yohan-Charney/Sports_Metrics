{{ config(
    materialized='table'
) }}

with donnees_source as (

    select *
    from {{ ref('staging_team_players_personal_info') }}

),

players_info as (

    select
        player_id,
        player_name,
        First_name,
        Last_name,
        Birthdate,
        Age,
        Heigth_cm,
        Weight_kg,
        Position,
        School,
        Country,
        Season_exp

    from donnees_source

)

select *
from players_info