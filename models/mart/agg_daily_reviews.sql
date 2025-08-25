{{ config(materialized='table') }}
select
  fr.date_key,
  count(fr.review_id) as review_count,
  count(distinct fr.listing_id) as unique_listing_count,
  count(distinct fr.host_id) as unique_host_count,
  {{ avg_sentiment('fr.sentiment') }} as avg_sentiment
from {{ ref('fact_reviews') }} fr
group by fr.date_key
