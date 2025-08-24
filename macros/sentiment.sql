{% macro sentiment_score(col) %}
case lower({{ col }})
  when 'positive' then 1
  when 'neutral'  then 0
  when 'negative' then -1
  else null
end
{% endmacro %}

{% macro avg_sentiment(col) %}
avg({{ sentiment_score(col) }})
{% endmacro %}