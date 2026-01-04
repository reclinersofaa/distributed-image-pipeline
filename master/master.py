import uuid
import time
import threading

from image_utils.splitter import split_image
from image_utils.assembler import assemble_image
from image_utils.serializer import base64_to_image

from master.task_producer import TaskProducer
from master.result_consumer import ResultConsumer


# -----------------------------
# Global state (lives with app)
# -----------------------------

JOB_STATUS = {}
JOB_START_TIME = {}
JOB_END_TIME = {}
JOB_WORKERS = {}
JOB_TILE_TIMES = {}

# ONE long-lived Kafka result consumer
RESULT_CONSUMER = ResultConsumer()


def run_master(image_path, operation, job_id):
    """
    Core orchestration logic.
    Runs in background thread.
    """

    try:
        tiles, grid_size, original_size = split_image(image_path)
        producer = TaskProducer()

        # Send tiles to Kafka
        for tile in tiles:
            producer.send_tile(job_id, tile, operation)

        producer.flush()
        print(f"[MASTER] Sent {len(tiles)} tiles for job {job_id}")

        processed_tiles = []

        # Wait for all tiles
        while len(processed_tiles) < len(tiles):
            time.sleep(1)

            results = RESULT_CONSUMER.get_results(job_id)

            processed_tiles = [
                {
                    "tile_id": r["tile_id"],
                    "position": r["position"],
                    "image": base64_to_image(r["tile_data"])
                }
                for r in results
            ]

            print(
                f"[MASTER] Job {job_id}: "
                f"{len(processed_tiles)}/{len(tiles)} tiles received"
            )

        final_image = assemble_image(
            processed_tiles,
            grid_size,
            original_size
        )

        output_path = f"data/output/{job_id}.png"
        final_image.save(output_path)

        JOB_STATUS[job_id] = "completed"
        JOB_END_TIME[job_id] = time.time()
        print(f"[MASTER] Job {job_id} completed")

    except Exception as e:
        JOB_STATUS[job_id] = f"failed: {e}"
        print(f"[MASTER] Job {job_id} failed:", e)


def start_job(image_path, operation, job_id):
    """
    Starts a job using a PRE-CREATED job_id.
    """
    print(f"[MASTER] start_job called for {job_id}")

    JOB_STATUS[job_id] = "processing"
    JOB_START_TIME[job_id] = time.time()
    JOB_WORKERS[job_id] = set()
    JOB_TILE_TIMES[job_id] = []

    thread = threading.Thread(
        target=run_master,
        args=(image_path, operation, job_id),
        daemon=True
    )
    thread.start()

    return job_id
