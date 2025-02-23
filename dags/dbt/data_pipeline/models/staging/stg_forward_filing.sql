with clock_data as (
    select *
    from {{ ref('stg_nba_clock_transform') }}
),

filled as (
    select
        GAME_ID,
        ACTIONNUMBER,
        time_left,
        HOME_RESULT,
        last_value(SCOREHOME ignore nulls) over (
            partition by GAME_ID
            order by TRY_TO_DOUBLE(ACTIONNUMBER)
            rows between unbounded preceding and current row
        ) as scoreHome,
        last_value(SCOREAWAY ignore nulls) over (
            partition by GAME_ID
            order by TRY_TO_DOUBLE(ACTIONNUMBER)
            rows between unbounded preceding and current row
        ) as scoreAway
    from clock_data
),

unique_game as (
    select 
        GAME_ID,
        max(HOME) as HOME,
        max(AWAY) as AWAY,
        max(Home_win_pct) as Home_win_pct,
        max(Away_win_pct) as Away_win_pct
    from {{ ref('stg_nba_raw') }}
    group by GAME_ID
),


final as (
    select 
        f.GAME_ID,
        u.HOME,
        u.AWAY,        
        u.Home_win_pct,
        u.Away_win_pct,
        f.scoreHome,
        f.ACTIONNUMBER,
        f.scoreAway,
        f.time_left,
        f.HOME_RESULT
    from filled f
    join unique_game u
      on f.GAME_ID = u.GAME_ID
)

select *
from final
order by GAME_ID, ACTIONNUMBER
