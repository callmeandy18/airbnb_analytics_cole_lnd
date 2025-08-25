{# with base as (
  select * from {{ ref('fact_reviews') }}
),
joined as (
  {{ join_listings_to_hosts('base') }}
),
... #}