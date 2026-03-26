with ranked_arrivals as (
    select
        *,
        row_number() over (
            partition by flight_number, scheduled_arrival_utc
            order by ingested_at desc
        ) as row_num
    from {{ ref('stg_raw_arrivals_events') }}
)

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
    ingested_at,
    ingested_date,
    scheduled_arrival_date,
    scheduled_arrival_hour
from ranked_arrivals
where row_num = 1
