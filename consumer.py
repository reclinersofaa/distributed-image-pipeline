from confluent_kafka import Consumer, KafkaException
import json
import sys

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = "image_tasks"

consumer.subscribe([topic])

print("Consumer started. Waiting for messages... Press Ctrl+C to exit")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        data = json.loads(msg.value().decode('utf-8'))
        print(f"Consumed message: {data}")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
