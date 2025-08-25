{{ config(materialized='table') }}
select
  dl.room_type,
  count(*) as total_listings,
  avg(dl.price) as avg_price,
  count(fr.review_id) as review_count
from {{ ref('dim_listing') }} dl
left join {{ ref('fact_reviews') }} fr on fr.listing_id = dl.listing_id
group by dl.room_type