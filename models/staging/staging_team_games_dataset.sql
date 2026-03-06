{{ config(
    materialized='table'
) }}

with sources as (

select *
from {{ source ('Sports_Metrics','team_games_dataset') }}

),

donnees as (

select *
from {{ source ('Sports_Metrics','team_boxscores') }}

),

game_data as (

select
																			
    s.Game_id as game_id,
    Game_date as game_date,
    Matchup,
    WL as Win_Loss,
    W as Win_Loss,
    L as Loss,
    W_PCT Win_pct,
    s.MIN as Total_minutes,
    s.PTS as Total_points,
    s.FGM as Total_Field_goal_made,
    s.FGA as Total_Field_goal_attempt,
    s.FG_PCT,
    s.FG3M as Total_Field_goal_3pts_made,
    s.FG3A as Total_Field_goal_3pts_attempt,
    s.FG3_PCT,
    s.FTM as Total_Free_throws_made,
    s.FTA as Total_Free_throws_attempt,
    s.FT_PCT,
    s.OREB as Total_Offensive_rebounds,
    s.DREB as Total_Defensive_rebounds,
    s.REB as Total_Total_rebounds,
    s.AST as Total_Assists,
    s.STL	as Total_Steals,
    s.BLK	as Total_Blocks,
    s.TOV	as Total_Turnover,
    s.PF as Total_Player_fault,
    d.PLUS_MINUS as Ecart
    

from sources s
join donnees d on d.GAME_ID = s.Game_id
)

select * from game_data