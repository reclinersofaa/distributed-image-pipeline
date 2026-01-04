import json
import time
import threading
import uuid

from confluent_kafka import Consumer


KAFKA_BROKER = "localhost:9092"
HEARTBEAT_TOPIC = "worker_heartbeats"
HEARTBEAT_TIMEOUT = 5  # seconds

WORKERS = {}
LOCK = threading.Lock()


def _consume_loop():
    """
    Runs in a dedicated daemon thread.
    Owns the Kafka Consumer for its entire lifetime.
    """
    print("[MASTER METRICS] Consumer thread starting")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"master-metrics-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    })

    consumer.subscribe([HEARTBEAT_TOPIC])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("[MASTER METRICS] Kafka error:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
            worker_id = data["worker_id"]

            with LOCK:
                WORKERS[worker_id] = {
                    "last_seen": time.time(),
                    "state": data["state"],
                    "tiles_processed": data["tiles_processed_total"]
                }

            print(f"[MASTER METRICS] heartbeat from {worker_id}")

        except Exception as e:
            print("[MASTER METRICS] Parse error:", e)


def _cleanup_loop():
    while True:
        time.sleep(2)
        now = time.time()

        with LOCK:
            dead = [
                wid for wid, info in WORKERS.items()
                if now - info["last_seen"] > HEARTBEAT_TIMEOUT
            ]
            for wid in dead:
                del WORKERS[wid]


def start_worker_metrics():
    threading.Thread(target=_consume_loop, daemon=True).start()
    threading.Thread(target=_cleanup_loop, daemon=True).start()


def get_worker_snapshot():
    with LOCK:
        snapshot = []
        for wid, info in WORKERS.items():
            snapshot.append({
                "worker_id": wid,
                "state": info["state"],
                "tiles_processed": info["tiles_processed"],
                "last_seen_sec": round(time.time() - info["last_seen"], 2)
            })
        return snapshot
