import json
import time
import uuid
import threading

from confluent_kafka import Consumer, Producer

from image_utils.serializer import base64_to_image, image_to_base64
from worker.processors import process_image


# -----------------------------
# Worker identity & state
# -----------------------------

WORKER_ID = str(uuid.uuid4())
HEARTBEAT_INTERVAL = 2  # seconds

tiles_processed_total = 0
worker_state = "idle"


# -----------------------------
# Kafka config
# -----------------------------

KAFKA_BROKER = "localhost:9092"
TASK_TOPIC = "image_tasks"
RESULT_TOPIC = "image_results"
HEARTBEAT_TOPIC = "worker_heartbeats"


# -----------------------------
# Heartbeat Producer
# -----------------------------

heartbeat_producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})


def heartbeat_loop():
    """
    Periodically send worker heartbeat messages.
    Runs independently of task processing.
    """
    while True:
        payload = {
            "worker_id": WORKER_ID,
            "timestamp": time.time(),
            "state": worker_state,
            "tiles_processed_total": tiles_processed_total
        }

        heartbeat_producer.produce(
            HEARTBEAT_TOPIC,
            value=json.dumps(payload).encode("utf-8")
        )
        heartbeat_producer.flush()

        time.sleep(HEARTBEAT_INTERVAL)


# -----------------------------
# Kafka Consumer (tasks)
# -----------------------------

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "image-workers",
    "auto.offset.reset": "latest"
})

consumer.subscribe([TASK_TOPIC])


# -----------------------------
# Kafka Producer (results)
# -----------------------------

result_producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})


# -----------------------------
# Main Worker Loop
# -----------------------------

def main():
    global tiles_processed_total, worker_state

    print(f"[WORKER {WORKER_ID}] Started")

    # Start heartbeat thread
    threading.Thread(
        target=heartbeat_loop,
        daemon=True
    ).start()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            worker_state = "idle"
            continue

        if msg.error():
            print(f"[WORKER {WORKER_ID}] Kafka error:", msg.error())
            continue

        try:
            task = json.loads(msg.value().decode("utf-8"))
            required_keys = {"job_id", "tile_id", "tile_data", "position", "operation"}
            if not required_keys.issubset(task.keys()):
                continue

            worker_state = "busy"

            image = base64_to_image(task["tile_data"])
            start = time.time()
            processed_image = process_image(
                image,
                task["operation"]
            )
            elapsed_ms = (time.time() - start) * 1000

            result = {
                "job_id": task["job_id"],
                "tile_id": task["tile_id"],
                "position": task["position"],
                "tile_data": image_to_base64(processed_image),
                "worker_id": WORKER_ID,
                "tile_time_ms": elapsed_ms
            }

            result_producer.produce(
                RESULT_TOPIC,
                value=json.dumps(result).encode("utf-8")
            )
            result_producer.flush()

            tiles_processed_total += 1

            print(
                f"Processed tile {task['tile_id']} for job {task['job_id']}"
            )

        except Exception as e:
            print(f"[WORKER {WORKER_ID}] Error:", e)


if __name__ == "__main__":
    main()
