with ranked_actions as (
    select *,
        row_number() over (
            partition by GAME_ID, ACTIONNUMBER 
            order by TIME_LEFT desc 
        ) as rownumber
    from {{ ref('stg_forward_filing') }}
)

select
       *
from ranked_actions
where rownumber = 1
