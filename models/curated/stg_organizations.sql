{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select
    cast(ORGANIZATION_ID as number(38,0))                      as ORGANIZATION_ID,
    to_date(try_to_timestamp_ntz(FIRST_PAYMENT_DATE))          as FIRST_PAYMENT_DATE,
    to_date(try_to_timestamp_ntz(LAST_PAYMENT_DATE))           as LAST_PAYMENT_DATE,
    upper(nullif(LEGAL_ENTITY_COUNTRY_CODE, ''))               as LEGAL_ENTITY_COUNTRY_CODE,
    cast(COUNT_TOTAL_CONTRACTS_ACTIVE as number(38,0))         as COUNT_TOTAL_CONTRACTS_ACTIVE,
    to_date(try_to_timestamp_ntz(CREATED_DATE))                as CREATED_DATE
  from {{ source('raw', 'organizations') }}
)

select * from src
