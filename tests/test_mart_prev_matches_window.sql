-- Fails if PREV_DAILY_PAYMENT_AMOUNT differs from a fresh LAG() recomputation
with m as (
  select
    ORGANIZATION_ID,
    DATE,
    DAILY_PAYMENT_AMOUNT,
    PREV_DAILY_PAYMENT_AMOUNT
  from {{ ref('mart_daily_org_balance') }}
),
w as (
  select
    ORGANIZATION_ID,
    DATE,
    DAILY_PAYMENT_AMOUNT,
    PREV_DAILY_PAYMENT_AMOUNT,
    lag(DAILY_PAYMENT_AMOUNT) over (
      partition by ORGANIZATION_ID
      order by DATE
    ) as prev_calc
  from m
)
select *
from w
where coalesce(PREV_DAILY_PAYMENT_AMOUNT, 0) <> coalesce(prev_calc, 0)
