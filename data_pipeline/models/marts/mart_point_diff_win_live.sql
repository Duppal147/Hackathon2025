{{ config(
    materialized='incremental',
    unique_key='GAME_ID, ACTIONNUMBER',
    alias='live_timed_data'
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
{% if is_incremental() %}
  where (GAME_ID, ACTIONNUMBER) not in (select GAME_ID, ACTIONNUMBER from {{ this }})
{% endif %}
order by time_left desc, GAME_ID, ACTIONNUMBER
