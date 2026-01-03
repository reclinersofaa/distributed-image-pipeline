from confluent_kafka import Producer
import json
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

topic = "image_tasks"

print("Producing messages... Press Ctrl+C to stop")

try:
    for i in range(5):
        message = {
            "id": i,
            "message": f"Hello Kafka {i}"
        }

        producer.produce(
            topic=topic,
            value=json.dumps(message),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(1)

except KeyboardInterrupt:
    pass

finally:
    producer.flush()
