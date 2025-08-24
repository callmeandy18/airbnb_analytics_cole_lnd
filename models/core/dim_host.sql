{{ 
    config(materialized='incremental') 
}}

with src as (
  select
    id::bigint                      as host_id,
    nullif(trim(name),'')           as host_name,
    coalesce(is_superhost,false)    as is_superhost,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
  from {{ source('airbnb','hosts') }}
  where id is not null
)

{% if is_incremental() %}
, watermark as (
  select coalesce(max(updated_at), '1900-01-01'::timestamp) as mx
  from {{ this }}
)
select s.*
from src s
cross join watermark w
where s.updated_at is not null
  and s.updated_at > w.mx
{% else %}
select *
from src
{% endif %}
