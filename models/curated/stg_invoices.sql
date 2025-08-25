{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select
    cast(INVOICE_ID as varchar)                                as INVOICE_ID,
    cast(PARENT_INVOICE_ID as varchar)                         as PARENT_INVOICE_ID,
    cast(TRANSACTION_ID as varchar)                            as TRANSACTION_ID,
    cast(ORGANIZATION_ID as number(38,0))                      as ORGANIZATION_ID,
    upper(nullif(TYPE, ''))                                    as TYPE,
    upper(nullif(STATUS, ''))                                  as STATUS,
    upper(nullif(CURRENCY, ''))                                as CURRENCY,
    upper(nullif(PAYMENT_CURRENCY, ''))                        as PAYMENT_CURRENCY,
    upper(nullif(PAYMENT_METHOD, ''))                          as PAYMENT_METHOD,
    cast(AMOUNT as number(18,2))                               as AMOUNT,
    cast(PAYMENT_AMOUNT as number(18,2))                       as PAYMENT_AMOUNT,
    cast(FX_RATE as number(18,6))                              as FX_RATE,
    cast(FX_RATE_PAYMENT as number(18,6))                      as FX_RATE_PAYMENT,
    coalesce(
      try_to_timestamp_ntz(CREATED_AT),
      try_to_timestamp_ntz(replace(CREATED_AT, 'T', ' '))
    )                                                          as CREATED_AT,
    to_date(
      coalesce(
        try_to_timestamp_ntz(CREATED_AT),
        try_to_timestamp_ntz(replace(CREATED_AT, 'T', ' '))
      )
    )                                                          as CREATED_DATE
  from {{ source('raw', 'invoices') }}
)

select * from src
