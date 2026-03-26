# Ingestion

This module contains the source-side ingestion logic for Krakow Airport arrivals.

## Files

- `producer.py`: calls AeroDataBox and normalizes the arrivals payload into one record per flight
- `publisher.py`: long-running Kafka publisher that polls every `PUBLISH_INTERVAL_SECONDS`

## Current flow

1. Fetch arrivals from AeroDataBox for `EPKK`
2. Normalize each flight into the canonical event schema
3. Publish normalized JSON messages to the `krk_arrivals` Kafka topic

## Important environment variables

- `AERODATABOX_API_KEY`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `PUBLISH_INTERVAL_SECONDS`
