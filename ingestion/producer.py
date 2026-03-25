import json
import requests
import os
import pandas as pd


def get_flight_data():
    url = "https://aerodatabox.p.rapidapi.com/flights/airports/icao/EPKK"

    querystring = {"offsetMinutes":"-5",
                "durationMinutes":"100",
                "withLeg":"true",
                "direction":"Arrival",
                "withCancelled":"true",
                "withCodeshared":"false",
                "withCargo":"true",
                "withPrivate":"true",
                "withLocation":"false"}

    headers = {
        "x-rapidapi-key": os.getenv("AERODATABOX_API_KEY"),
        "x-rapidapi-host": "aerodatabox.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers, params=querystring, timeout=30)
    response.raise_for_status()  # Check if the request was successful
    return response.json()
# print(json.dumps(data, indent=4))


def process_flight_data(data):
    flights = []
    for flight in data.get('arrivals', []):
        flight_info = {
            'flight_number': flight.get('number', None),
            'status': flight.get('status', None),
            'airline_name': flight.get('airline', {}).get('name', None),
            'origin_name': flight.get('departure', {}).get('airport', {}).get('name', None),
            'scheduled_arrival_utc': flight.get('arrival', {}).get('scheduledTime', {}).get('utc', None),
            'revised_arrival_utc': flight.get('arrival', {}).get('revisedTime', {}).get('utc', None),
            'origin_icao': flight.get('departure', {}).get('airport', {}).get('icao', None),
            'airline_icao': flight.get('airline', {}).get('icao', None),
        }
        flights.append(flight_info)

    df = pd.DataFrame(flights)
    df['scheduled_arrival_utc'] = pd.to_datetime(df['scheduled_arrival_utc'], utc=True, errors='coerce')
    df['revised_arrival_utc'] = pd.to_datetime(df['revised_arrival_utc'], utc=True, errors='coerce')
    df['delay_minutes'] = (df['revised_arrival_utc'] - df['scheduled_arrival_utc']).dt.total_seconds() / 60
    df['ingested_at'] = pd.Timestamp.now(tz='UTC')
    mapping = {
        'Arrived': 'arrived',
        'Expected': 'expected',  
        'Cancelled': 'cancelled',
        'Delayed': 'delayed'
    }
    df['status'] = df['status'].map(mapping).fillna("unknown")
    df['status_bucket'] = df['delay_minutes'].apply(lambda x: 'on_time' if x <= 0 else ('delayed' if x > 0 else 'unknown'))
    for col in ['scheduled_arrival_utc', 'revised_arrival_utc', 'ingested_at']:
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)

    return df.to_dict(orient='records')