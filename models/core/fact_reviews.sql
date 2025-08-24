{{ 
    config(materialized='incremental') 
}}

SELECT
    review_id,
    listing_id,
    host_id,
    review_date,
    sentiment,
    reviewer_name,
    date_key
FROM {{ source("airbnb","reviews") }} f
JOIN {{ ref('dim_listing') }} l on l.listing_id = f.listing_id
JOIN {{ ref('dim_host') }} h on h.host_id = f.host_id
JOIN {{ source('airbnb','dim_date') }} d on d.date = f.date_key


{% if is_incremental() %}
  where (updated_at is not null)
  and updated_at > (select coalesce(max(updated_at),'1900-01-01') from {{ this }})
{% endif %}
