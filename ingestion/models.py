from dataclasses import dataclass
import json
import pandas as pd

@dataclass
class FlightArrivalEvent:
    flight_number: str
    status: str
    airline_name: str
    origin_name: str
    scheduled_arrival_utc: str
    revised_arrival_utc: str
    origin_icao: str
    airline_icao: str
    delay_minutes: float
    status_bucket: str


def flight_from_row(row):
    return FlightArrivalEvent(
        flight_number=row['flight_number'],
        status=row['status'],
        airline_name=row['airline_name'],
        origin_name=row['origin_name'],
        scheduled_arrival_utc=row['scheduled_arrival_utc'].isoformat() if pd.notnull(row['scheduled_arrival_utc']) else None,
        revised_arrival_utc=row['revised_arrival_utc'].isoformat() if pd.notnull(row['revised_arrival_utc']) else None,
        origin_icao=row['origin_icao'],
        airline_icao=row['airline_icao'],
        delay_minutes=float(row['delay_minutes']) if pd.notnull(row['delay_minutes']) else 0.0,
        status_bucket=row['status_bucket']
    )


def flight_deserializer(data):
    json_str = data.decode('utf-8')
    flight_dict = json.loads(json_str)
    return FlightArrivalEvent(**flight_dict)
