select
    trim(flight_number) as flight_number,
    lower(status) as status,
    lower(status_bucket) as status_bucket,
    airline_name,
    airline_icao,
    origin_name,
    origin_icao,
    scheduled_arrival_utc,
    revised_arrival_utc,
    cast(delay_minutes as float64) as delay_minutes,
    ingested_at,
    date(ingested_at) as ingested_date,
    date(scheduled_arrival_utc) as scheduled_arrival_date,
    extract(hour from scheduled_arrival_utc) as scheduled_arrival_hour
from {{ source('krk_flights', 'krk_arrivals_raw') }}
