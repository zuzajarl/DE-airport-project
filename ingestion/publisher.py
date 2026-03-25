import json
from kafka import KafkaProducer # type: ignore
# from ..common.models import FlightArrivalEvent, flight_from_row

from ingestion.producer import get_flight_data, process_flight_data


# def flight_serializer(flight: FlightArrivalEvent) -> bytes:
#     flight_dict = dataclasses.asdict(flight)
#     json_str = json.dumps(flight_dict)
#     return json_str.encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
)

topic_name = 'krk_arrivals'

data = get_flight_data()
processed_data = process_flight_data(data)

flights_count = 0
for flight in processed_data:
    producer.send(topic_name, value=flight)
    flights_count += 1

producer.flush()

print(f'sent {flights_count} rows')
