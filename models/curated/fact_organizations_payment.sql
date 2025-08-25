{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ORGANIZATION_ID','DATE'],
    on_schema_change='sync',
    cluster_by=['DATE','ORGANIZATION_ID'],
    alias='FACT_ORGANIZATIONS_PAYMENT',
    tags=['marts','payments']
) }}

with src as (
  select
    ORGANIZATION_ID,
    cast(CREATED_DATE as date) as DATE,
    STATUS,
    AMOUNT,
    PAYMENT_AMOUNT,
    FX_RATE,
    FX_RATE_PAYMENT
  from {{ ref('stg_invoices') }}
  where upper(STATUS) in ('CREDITED','REFUNDED','PAID')
  {% if is_incremental() %}
    -- only reprocess recent days to keep runs fast
    and CREATED_DATE >= (
      select coalesce(dateadd(day, -7, max(DATE)), to_date('1900-01-01'))
      from {{ this }}
    )
  {% endif %}
),

converted as (
  select
    ORGANIZATION_ID,
    DATE,
    /* Convert to USD, avoid divide-by-zero with NULLIF */
    coalesce(
      PAYMENT_AMOUNT / nullif(FX_RATE_PAYMENT, 0),
      AMOUNT         / nullif(FX_RATE,          0)
    ) as PAYMENT_AMOUNT_USD
  from src
),

filtered as (
  -- Keep only rows where the USD calc is available
  select *
  from converted
  where PAYMENT_AMOUNT_USD is not null
),

agg as (
  select
    ORGANIZATION_ID,
    DATE,
    sum(PAYMENT_AMOUNT_USD) as PAYMENT_AMOUNT_USD
  from filtered
  group by 1,2
)

select
  ORGANIZATION_ID,
  DATE,
  PAYMENT_AMOUNT_USD
from agg
