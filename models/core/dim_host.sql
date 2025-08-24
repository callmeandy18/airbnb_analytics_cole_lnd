{{ 
    config(materialized='incremental') 
}}

SELECT
    host_id,
    host_name,
    is_superhost,
    created_at,
    updated_at
FROM {{ source("airbnb","hosts") }}

{% if is_incremental() %}
  where (updated_at is not null)
  and updated_at > (select coalesce(max(updated_at),'1900-01-01') from {{ this }})
{% endif %}
