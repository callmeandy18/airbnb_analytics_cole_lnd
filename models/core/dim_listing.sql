{{ config(materialized='incremental', unique_key='listing_id') }}

with src as (
  select
    id::bigint                      as listing_id,
    nullif(trim(name),'')           as listing_name,
    nullif(trim(room_type),'')      as room_type,
    minimum_nights::int             as minimum_nights,
    price::numeric                  as price,
    host_id::bigint                 as host_id,
    listing_url,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at,
    substring(listing_url from '^(?:https?://)?([^/]+)') as listing_domain
  from {{ source('airbnb','listings') }}
  where id is not null
    and price is not null and price > 0
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
select * from src
{% endif %}