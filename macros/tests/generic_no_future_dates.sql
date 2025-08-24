{% test no_future_dates(model, column_name) %}
select {{ column_name }} as dt
from {{ model }}
where {{ column_name }} > current_date
{% endtest %}