
with raw as (
    select 
        GAME_ID,
        ACTIONNUMBER,
        CLOCK,
        PERIOD,
        SCOREHOME,
        SCOREAWAY,
        HOME_RESULT
    from {{ ref('stg_nba_raw') }}
),

clock_transformed as (
    select
        GAME_ID,
        ACTIONNUMBER,
        REGEXP_REPLACE(CLOCK, 'PT([0-9]{2})M([0-9]{2}\\.[0-9]{2})S', '\\1:\\2') as CLOCK_TRANSFORMED,
        PERIOD,
        SCOREHOME,
        SCOREAWAY,
        HOME_RESULT
    from raw
),

parsed as (
    select
        GAME_ID,
        ACTIONNUMBER,
        CLOCK_TRANSFORMED,
        PERIOD,
        TRY_TO_DOUBLE(SPLIT_PART(CLOCK_TRANSFORMED, ':', 1)) as minutes,
        TRY_TO_DOUBLE(SPLIT_PART(CLOCK_TRANSFORMED, ':', 2)) as seconds,
        SCOREHOME,
        SCOREAWAY,
        HOME_RESULT
    from clock_transformed
),

calculated as (
    select
        GAME_ID,
        ACTIONNUMBER,
        CLOCK_TRANSFORMED,
        PERIOD,
        minutes,
        seconds,
        (minutes + (seconds / 60)) as minutes_left,
        ((minutes + (seconds / 60)) + ((4 - TRY_TO_DOUBLE(PERIOD)) * 12)) as time_left,
        SCOREHOME,
        SCOREAWAY,
        HOME_RESULT
    from parsed
)

select *
from calculated
