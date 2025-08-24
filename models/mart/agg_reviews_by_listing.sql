{{ config(materialized='table') }}

select
  fr.listing_id,
  count(fr.review_id) as review_count,
  {{ avg_sentiment('fr.sentiment') }} as avg_sentiment,
  max(fr.review_date) as latest_review_date
from {{ ref('fact_reviews') }} fr
group by fr.listing_id