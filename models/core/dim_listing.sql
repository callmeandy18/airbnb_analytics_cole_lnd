{{ 
    config(materialized='incremental') 
}}

SELECT
    listing_id,
    listing_name,
    room_type,
    minimun_nights,
    price,
    host_id
FROM {{ source("airbnb","listings") }}

{% if is_incremental() %}
  where (updated_at is not null)
  and updated_at > (select coalesce(max(updated_at),'1900-01-01') from {{ this }})
{% endif %}
