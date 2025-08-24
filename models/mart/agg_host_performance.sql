{{ config(materialized='table') }}

with listing_per_host as (
  select host_id, count(*) as listing_count, avg(price) as avg_price_per_listing
  from {{ ref('dim_listing') }}
  group by host_id
),
review_per_host as (
  select host_id,
         count(review_id) as review_count,
         {{ avg_sentiment('sentiment') }} as avg_sentiment
  from {{ ref('fact_reviews') }}
  group by host_id
)
select
  h.host_id,
  coalesce(l.listing_count,0) as listing_count,
  coalesce(r.review_count,0) as review_count,
  coalesce(l.avg_price_per_listing,0) as avg_price_per_listing,
  coalesce(r.avg_sentiment,0) as avg_sentiment
from {{ ref('dim_host') }} h
left join listing_per_host l using (host_id)
left join review_per_host r using (host_id)