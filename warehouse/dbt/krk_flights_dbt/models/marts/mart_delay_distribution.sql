select
    scheduled_arrival_utc,
    origin_icao,
    origin_name,
    count(*) as total_flights,
    countif(status_bucket = 'delayed') as delayed_flights,
    round(avg(delay_minutes), 2) as avg_delay_minutes,
    round(max(delay_minutes), 2) as max_delay_minutes
from {{ ref('int_latest_arrivals') }}
group by 1, 2, 3
