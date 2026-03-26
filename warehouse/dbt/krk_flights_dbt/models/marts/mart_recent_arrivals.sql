select
    flight_number,
    status,
    status_bucket,
    airline_name,
    airline_icao,
    origin_name,
    origin_icao,
    scheduled_arrival_utc,
    revised_arrival_utc,
    delay_minutes,
    ingested_at
from {{ ref('int_latest_arrivals') }}
