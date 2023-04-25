{{
    config(
        materialized = 'incremental',
        unique_key = 'key_1'
    )
}}

with table_b as (
  select
  {% if is_incremental() %}
    *
  {% else %}
    key_1,
    value_b
  {% endif %}
  from {{ ref('table_b') }}
),

{% if is_incremental() %}
table_b_updates as (
  select key_1
  from table_b
  where updated_at > (select max(updated_at) from {{ this }})
),
{% endif %}

table_a as (
  select
    key_1,
    value_a
  from {{ ref('table_a') }}
  {% if is_incremental() %}
  where
    updated_at > (select max(updated_at) from {{ this }})
    or key_1 in (select key_1 from table_b_updates)
  {% endif %}
),

final as (
  select
    table_a.key_1,
    table_a.value_a,
    table_b.value_b,
    current_timestamp() as updated_at
  from
    table_a
    left join table_b using (key_1)
)

select * from final
