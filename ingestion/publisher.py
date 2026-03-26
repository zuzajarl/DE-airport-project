import json
import os
import time

from kafka import KafkaProducer  # type: ignore

from ingestion.producer import get_flight_data, process_flight_data

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "krk_arrivals")
POLL_INTERVAL_SECONDS = int(os.getenv("PUBLISH_INTERVAL_SECONDS", "300"))


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
    )


def publish_batch(producer: KafkaProducer) -> int:
    data = get_flight_data()
    processed_data = process_flight_data(data)

    flights_count = 0
    for flight in processed_data:
        producer.send(TOPIC_NAME, value=flight)
        flights_count += 1

    producer.flush()
    return flights_count


def main() -> None:
    producer = build_producer()
    print(
        f"Starting publisher for topic={TOPIC_NAME} "
        f"bootstrap_servers={KAFKA_SERVER} interval={POLL_INTERVAL_SECONDS}s"
    )

    while True:
        try:
            flights_count = publish_batch(producer)
            print(f"sent {flights_count} rows")
        except Exception as exc:
            print(f"publisher iteration failed: {exc}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
