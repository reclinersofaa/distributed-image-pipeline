from confluent_kafka import Consumer
import json
import threading
JOB_WORKERS = {}
JOB_TILE_TIMES = {}
from collections import deque
import time
TILE_COMPLETION_TIMES = deque(maxlen=5000)


class ResultConsumer:
    def __init__(self, broker="localhost:9092"):
        self.consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": "master-result-consumer",
            "auto.offset.reset": "latest"
        })

        self.consumer.subscribe(["image_results"])

        self.results = {}
        self.lock = threading.Lock()

        threading.Thread(
            target=self._consume_loop,
            daemon=True
        ).start()

    def _consume_loop(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            data = json.loads(msg.value().decode("utf-8"))
            TILE_COMPLETION_TIMES.append(time.time())

            job_id = data["job_id"]   

            worker_id = data.get("worker_id")
            tile_time = data.get("tile_time_ms")

            with self.lock:
                if job_id not in self.results:
                    self.results[job_id] = []
                    JOB_WORKERS[job_id] = set()
                    JOB_TILE_TIMES[job_id] = []

                self.results[job_id].append(data)

                if worker_id:
                    JOB_WORKERS[job_id].add(worker_id)

                if tile_time is not None:
                    JOB_TILE_TIMES[job_id].append(tile_time)

    def get_results(self, job_id):
        with self.lock:
            return list(self.results.get(job_id, []))
