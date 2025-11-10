import json
from confluent_kafka import Consumer, Producer, KafkaError
import sys
import socket
import base64
import io
from PIL import Image
import os
import uuid
import threading
import time
import logging

# ----------------- CONFIG -----------------
BOOTSTRAP_SERVERS = '172.27.247.209:9092'
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'
GROUP_ID = 'image-processor-group'

# ----------------- LOGGING -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("worker")

# ----------------- UTILS -----------------
def get_worker_id():
    """Generate a stable-ish worker id."""
    env_worker_id = os.getenv("WORKER_ID")
    if env_worker_id:
        return env_worker_id
    try:
        hostname = socket.gethostname()
        unique_id = uuid.uuid4().hex[:6]
        return f"{hostname}-{unique_id}"
    except Exception as e:
        log.error(f"Error generating worker ID: {e}")
        return f"worker-{uuid.uuid4().hex[:6]}"

def create_consumer(bootstrap_servers, group_id):
    """Create Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'latest'
    })

def create_producer(bootstrap_servers):
    """Create Kafka Producer."""
    return Producer({'bootstrap.servers': bootstrap_servers})

def process_image(image_bytes):
    """
    Apply a grayscale filter.
    Return processed image bytes or None on error.
    """
    try:
        image = Image.open(io.BytesIO(image_bytes))
        processed_image = image.convert('L')
        buf = io.BytesIO()
        processed_image.save(buf, format="JPEG")
        return buf.getvalue()
    except Exception as e:
        log.error(f"Image processing error: {e}")
        return None

WORKER_ID = get_worker_id()

# ----------------- HEARTBEAT -----------------
def heartbeat_loop():
    """Send heartbeats every 5 seconds."""
    log.info(f"[{WORKER_ID}] Heartbeat thread started")
    heartbeat_producer = create_producer(BOOTSTRAP_SERVERS)
    data = {"worker_id": WORKER_ID, "status": "alive"}

    while True:
        try:
            data["timestamp"] = time.time()
            heartbeat_producer.produce(
                HEARTBEAT_TOPIC,
                value=json.dumps(data).encode('utf-8')
            )
            heartbeat_producer.poll(0)
        except Exception as e:
            log.warning(f"[{WORKER_ID}] Heartbeat failed: {e}")
        time.sleep(5)

# ----------------- MAIN -----------------
def main():
    log.info(f"[{WORKER_ID}] Worker starting...")
    try:
        consumer = create_consumer(BOOTSTRAP_SERVERS, GROUP_ID)
        producer = create_producer(BOOTSTRAP_SERVERS)
    except Exception as e:
        log.critical(f"[{WORKER_ID}] Kafka connection error: {e}")
        sys.exit(1)

    log.info(f"[{WORKER_ID}] Connected to Kafka at {BOOTSTRAP_SERVERS}")
    consumer.subscribe([TASK_TOPIC])
    log.info(f"[{WORKER_ID}] Subscribed to topic: {TASK_TOPIC} (group={GROUP_ID})")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning(f"[{WORKER_ID}] Consumer error: {msg.error()}")
                continue

            try:
                task = json.loads(msg.value().decode('utf-8'))
                job_id = task.get('job_id', 'unknown_job')
                tile_id = task.get('tile_id', 'unknown_tile')
                tile_b64 = task.get('tile_data')
                if not tile_b64:
                    log.warning(f"[{WORKER_ID}] Skip job={job_id}: missing tile_data")
                    continue

                # timing before base64 decode
                t0 = time.perf_counter()
                image_bytes = base64.b64decode(tile_b64)
                decode_ms = (time.perf_counter() - t0) * 1000

                log.info(f"[{WORKER_ID}] Received job={job_id} tile={tile_id} in_bytes={len(image_bytes)}B decode={decode_ms:.1f}ms")
            except Exception as e:
                log.error(f"[{WORKER_ID}] Decode task error: {e}")
                continue

            # timing before processing
            p0 = time.perf_counter()
            processed = process_image(image_bytes)
            proc_ms = (time.perf_counter() - p0) * 1000

            if processed is None:
                log.error(f"[{WORKER_ID}] Process failed job={job_id} tile={tile_id} proc={proc_ms:.1f}ms")
                continue

            processed_b64 = base64.b64encode(processed).decode('utf-8')
            log.info(f"[{WORKER_ID}] Processed tile={tile_id} out_bytes={len(processed)}B proc={proc_ms:.1f}ms")

            # timing before produce
            s0 = time.perf_counter()
            try:
                result = {
                    'job_id': job_id,
                    'tile_id': tile_id,
                    'processed_data': processed_b64,
                    'worker_id': WORKER_ID
                }
                producer.produce(
                    RESULT_TOPIC,
                    key=str(job_id),
                    value=json.dumps(result).encode('utf-8')
                )
                producer.flush()
                send_ms = (time.perf_counter() - s0) * 1000
                log.info(f"[{WORKER_ID}] Result sent topic={RESULT_TOPIC} send={send_ms:.1f}ms")
            except Exception as e:
                log.error(f"[{WORKER_ID}] Result produce error: {e}")

    except KeyboardInterrupt:
        log.info(f"[{WORKER_ID}] Stopping worker (keyboard interrupt)")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        log.info(f"[{WORKER_ID}] Worker stopped")

if __name__ == '__main__':
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    main()
