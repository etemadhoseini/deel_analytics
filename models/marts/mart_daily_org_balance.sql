{{ config(materialized='table') }}

with f as (
  -- New source table at org+date grain (USD already computed)
  select
    ORGANIZATION_ID,
    DATE,
    PAYMENT_AMOUNT_USD as DAILY_PAYMENT_AMOUNT
  from {{ ref('fact_organizations_payment') }}
),

with_prev as (
  select
    ORGANIZATION_ID,
    DATE,
    DAILY_PAYMENT_AMOUNT,
    lag(DAILY_PAYMENT_AMOUNT) over (
      partition by ORGANIZATION_ID
      order by DATE
    ) as PREV_DAILY_PAYMENT_AMOUNT
  from f
)

select
  ORGANIZATION_ID,
  DATE,
  coalesce(DAILY_PAYMENT_AMOUNT, 0)      as DAILY_PAYMENT_AMOUNT,      -- USD for current day
  PREV_DAILY_PAYMENT_AMOUNT,                                            -- last available day
  case
    when PREV_DAILY_PAYMENT_AMOUNT is null or PREV_DAILY_PAYMENT_AMOUNT = 0
      then null
    else (DAILY_PAYMENT_AMOUNT - PREV_DAILY_PAYMENT_AMOUNT)
         / abs(PREV_DAILY_PAYMENT_AMOUNT)
  end as DOD_CHANGE_PCT
from with_prev
