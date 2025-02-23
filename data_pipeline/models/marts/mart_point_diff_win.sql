{{ config(
    materialized='table',
    alias='historical_timed_data'
) }}

with unique_actions as (
    select *
    from {{ ref('stg_unique_actions') }}
)

select
    GAME_ID,
    ACTIONNUMBER,
    SCOREHOME,
    SCOREAWAY,
    Home_win_pct,
    Away_win_pct,
    (SCOREHOME - SCOREAWAY) as score_diff,
    case
        when HOME_RESULT is null then null
        when HOME_RESULT = 'W' then 1 
        else 0 
    end as win
from unique_actions
order by time_left desc, GAME_ID, ACTIONNUMBER
