with duplicates as (
  select
    GAME_ID,
    ACTIONNUMBER,
    count(*) as cnt
  from {{ ref('stg_unique_actions') }}
  group by GAME_ID, ACTIONNUMBER
  having count(*) > 1
)
select *
from duplicates
