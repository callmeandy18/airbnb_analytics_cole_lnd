{% macro join_listings_to_hosts(fact_relation) %}
  -- Macro to perform consistent joins between fact_reviews, dim_listing, and dim_host
  {{ fact_relation }}
  LEFT JOIN {{ ref('dim_listing') }} AS listing
    ON {{ fact_relation }}.listing_id = listing.listing_id
  LEFT JOIN {{ ref('dim_host') }} AS host
    ON {{ fact_relation }}.host_id = host.host_id
{% endmacro %}