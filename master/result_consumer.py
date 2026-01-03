from confluent_kafka import Consumer
import json
import threading


class ResultConsumer:
    def __init__(self, broker="localhost:9092"):
        self.consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": "master-result-consumer",
            "auto.offset.reset": "earliest"
        })

        self.consumer.subscribe(["image_results"])

        self.results = {}
        self.lock = threading.Lock()

        self.thread = threading.Thread(
            target=self._consume_loop,
            daemon=True
        )
        self.thread.start()

    def _consume_loop(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            data = json.loads(msg.value().decode("utf-8"))
            image_id = data["image_id"]

            with self.lock:
                if image_id not in self.results:
                    self.results[image_id] = []
                self.results[image_id].append(data)

    def get_results(self, image_id):
        with self.lock:
            return list(self.results.get(image_id, []))
