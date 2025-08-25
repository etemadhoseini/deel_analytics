{{ config(materialized='table') }}

with src as (
  select * from {{ ref('stg_organizations') }}
),
ranked as (
  select
    ORGANIZATION_ID,
    FIRST_PAYMENT_DATE,
    LAST_PAYMENT_DATE,
    LEGAL_ENTITY_COUNTRY_CODE,
    COUNT_TOTAL_CONTRACTS_ACTIVE,
    CREATED_DATE,
    row_number() over (
      partition by ORGANIZATION_ID
      order by CREATED_DATE desc nulls last
    ) as rn
  from src
)

select
  ORGANIZATION_ID,
  FIRST_PAYMENT_DATE,
  LAST_PAYMENT_DATE,
  LEGAL_ENTITY_COUNTRY_CODE,
  COUNT_TOTAL_CONTRACTS_ACTIVE,
  CREATED_DATE
from ranked
where rn = 1
