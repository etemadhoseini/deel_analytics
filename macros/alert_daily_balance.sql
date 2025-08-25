{% macro alert_daily_balance_query(alert_date=None, threshold_pct=0.5, min_abs=0, allowlist='', denylist='') %}
{% set date_filter %}
  {% if alert_date %}
    date = to_date('{{ alert_date }}')
  {% else %}
    date = (select max(date) from {{ ref('mart_daily_org_balance') }})
  {% endif %}
{% endset %}

with base as (
  select * from {{ ref('mart_daily_org_balance') }}
  where {{ date_filter }}
), filtered as (
  select * from base
  where abs(coalesce(dod_change_pct, 0)) > {{ threshold_pct }}
    and ( {{ min_abs }} = 0 or abs(coalesce(daily_payment_amount - coalesce(prev_daily_payment_amount, 0), 0)) > {{ min_abs }} )
    and ( '{{ allowlist }}' = '' or position(','||cast(organization_id as string)||',' in ','||'{{ allowlist }}'||',' ) > 0 )
    and ( '{{ denylist }}' = '' or position(','||cast(organization_id as string)||',' in ','||'{{ denylist }}'||',' ) = 0 )
)
select * from filtered order by organization_id
{% endmacro %}