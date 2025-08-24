{{ 
    config(materialized='incremental') 
}}

WITH src AS (
    SELECT
        id::bigint                      AS listing_id,
        nullif(trim(name),'')           AS listing_name,
        nullif(trim(room_type),'')      AS room_type,
        minimum_nights::int             AS minimum_nights,
        price::numeric                  AS price,
        host_id::bigint                 AS host_id, 
        listing_url,
        created_at::timestamp           AS created_at,
        updated_at::timestamp           AS updated_at,
        substring(listing_url from '^(?:https?://)?([^/]+)') as listing_domain
    FROM {{ source("airbnb","listings") }}
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
select *
from src
{% endif %}