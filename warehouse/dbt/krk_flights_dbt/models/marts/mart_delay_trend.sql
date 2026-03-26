select
    scheduled_arrival_utc,
    scheduled_arrival_date,
    scheduled_arrival_hour,
    count(*) as total_flights,
    countif(status_bucket = 'delayed') as delayed_flights,
    round(avg(delay_minutes), 2) as avg_delay_minutes,
    round(avg(case when status_bucket = 'delayed' then delay_minutes end), 2) as avg_delayed_flight_minutes
from {{ ref('int_latest_arrivals') }}
group by 1, 2, 3
