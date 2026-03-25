from kafka import KafkaConsumer
import json


server = 'localhost:9092'
topic_name = 'krk_arrivals'
output_path = 'docs/raw_arrivals_events.jsonl'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='flights-console',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to {topic_name}...")

count = 0
with open(output_path, 'a', encoding='utf-8') as f:
    for message in consumer:
        flight_event = message.value
        print(f"Received flight: {flight_event}")
        f.write(json.dumps(flight_event) + "\n")
        f.flush() 
        count += 1
        if count >= 10:
            print(f"\n... received {count} messages so far (stopping after 10 for demo)")
            break
consumer.close()
