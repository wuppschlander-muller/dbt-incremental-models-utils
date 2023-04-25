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
    key_2,
    key_3,
    value_b
  {% endif %}
  from {{ ref('table_b') }}
),

table_c as (
  select
  {% if is_incremental() %}
    *
  {% else %}
    key_2,
    value_c
  {% endif %}
  from {{ ref('table_c') }}
),

table_d as (
  select
  {% if is_incremental() %}
    *
  {% else %}
    key_3,
    value_d
  {% endif %}
  from {{ ref('table_d') }}
),

{% if is_incremental() %}
table_d_updates as (
  select key_3
  from table_d
  where updated_at > (select max(updated_at) from {{ this }})
),

table_c_updates as (
  select key_2
  from table_c
  where updated_at > (select max(updated_at) from {{ this }})
),

table_b_updates as (
  select key_1
  from table_b
  where
    updated_at > (select max(updated_at) from {{ this }})
    or key_2 in (select key_2 from table_c_updates)
    or key_3 in (select key_3 from table_d_updates)
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
    table_b.key_2,
    table_b.key_3,
    table_a.value_a,
    table_b.value_b,
    table_c.value_c,
    table_d.value_d,
    current_timestamp() as updated_at
  from
    table_a
    left join table_b using (key_1)
    left join table_c using (key_2)
    left join table_d using (key_3)
)

select * from final
