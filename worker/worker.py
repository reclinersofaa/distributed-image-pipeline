from confluent_kafka import Consumer, Producer
import json
import uuid

from image_utils.serializer import base64_to_image, image_to_base64
from worker.processors import grayscale, blur, edge_detect


CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "image-workers",
    "auto.offset.reset": "earliest"
}

PRODUCER_CONF = {
    "bootstrap.servers": "localhost:9092"
}

TASK_TOPIC = "image_tasks"
RESULT_TOPIC = "image_results"


def process_tile(operation, image):
    if operation == "grayscale":
        return grayscale(image)
    elif operation == "blur":
        return blur(image)
    elif operation == "edge":
        return edge_detect(image)
    else:
        raise ValueError(f"Unknown operation: {operation}")


def main():
    consumer = Consumer(CONSUMER_CONF)
    producer = Producer(PRODUCER_CONF)

    consumer.subscribe([TASK_TOPIC])

    print("Worker started. Waiting for tasks...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            task = json.loads(msg.value().decode("utf-8"))

            required = {"image_id", "tile_id", "position", "operation", "tile_data"}
            if not required.issubset(task):
                print("Skipping invalid task:", task)
                continue            

            image = base64_to_image(task["tile_data"])
            processed = process_tile(task["operation"], image)

            result = {
                "image_id": task["image_id"],
                "tile_id": task["tile_id"],
                "position": task["position"],
                "tile_data": image_to_base64(processed)
            }

            producer.produce(
                RESULT_TOPIC,
                value=json.dumps(result)
            )

            producer.poll(0)

            print(
                f"Processed tile {task['tile_id']} "
                f"({task['operation']})"
            )

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
