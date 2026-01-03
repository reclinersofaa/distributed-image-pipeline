from confluent_kafka import Producer
import json
import uuid

from image_utils.serializer import image_to_base64


class TaskProducer:
    def __init__(self, broker="localhost:9092"):
        self.producer = Producer({
            "bootstrap.servers": broker
        })

    def send_tile(self, image_id, tile, operation):
        message = {
            "image_id": image_id,
            "tile_id": tile["tile_id"],
            "position": tile["position"],
            "operation": operation,
            "tile_data": image_to_base64(tile["image"])
        }

        self.producer.produce(
            "image_tasks",
            value=json.dumps(message)
        )

        self.producer.poll(0)

    def flush(self):
        self.producer.flush()