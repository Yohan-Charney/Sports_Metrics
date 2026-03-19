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
        Height_cm,
        Weight_kg,
        Position,
        School,
        Country,
        Season_exp

    from donnees_source

),


doublons as ( 
    select * from players_info 
    where player_id is not null 
    qualify row_number() over ( partition by player_id order by player_id desc ) = 1
)

select * from doublons

