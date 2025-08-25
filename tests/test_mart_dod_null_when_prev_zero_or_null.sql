-- Fails if DOD_CHANGE_PCT is NOT NULL when previous is NULL or 0
with m as (
  select
    ORGANIZATION_ID,
    DATE,
    DAILY_PAYMENT_AMOUNT,
    PREV_DAILY_PAYMENT_AMOUNT,
    DOD_CHANGE_PCT
  from {{ ref('mart_daily_org_balance') }}
)
select *
from m
where (PREV_DAILY_PAYMENT_AMOUNT is null or PREV_DAILY_PAYMENT_AMOUNT = 0)
  and DOD_CHANGE_PCT is not null
