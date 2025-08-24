{{ config(materialized='incremental', unique_key='review_id') }}

with src as (
  select
    review_id::bigint        as review_id,
    listing_id::bigint       as listing_id,
    review_date::date        as review_date,
    nullif(trim(reviewer_name),'') as reviewer_name,
    lower(nullif(trim(sentiment),'')) as sentiment
  from {{ source('airbnb','reviews') }}
  where review_id is not null
),

flt as (
  select *
  from src
  where review_date is not null
    and review_date >= cast('{{ var("min_review_date","2000-01-01") }}' as date)
    and review_date <= current_date
    and reviewer_name is not null
    and length(reviewer_name) between 2 and 100
    and sentiment in ('positive','neutral','negative')
),

with_dims as (
  select
    f.review_id,
    l.listing_id,
    l.host_id,
    f.review_date,
    f.sentiment,
    f.reviewer_name,
    d.date_id as date_key
  from flt f
  join {{ ref('dim_listing') }} l on l.listing_id = f.listing_id
  join {{ source('airbnb','dim_date') }} d on d.date = f.review_date
)

select *
from with_dims

{% if is_incremental() %}
  where review_id not in (select review_id from {{ this }})
{% endif %}