import json
from confluent_kafka import Consumer, KafkaError
import sys
import os
import base64
import redis
from PIL import Image
import time
import logging
import threading

# ----------------- CONFIG -----------------
BOOTSTRAP_SERVERS = '172.27.247.209:9092'
RESULT_TOPIC = 'results'
GROUP_ID = 'master-results-group'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

PROCESSED_DIR = 'processed'
FINAL_DIR = 'final'

# ----------------- LOGGING -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ----------------- KAFKA & REDIS -----------------
def create_consumer(bootstrap_servers, group_id):
    """Create Kafka Consumer."""
    return Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })


def connect_to_redis():
    """Connect to Redis."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        log.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return r
    except redis.exceptions.ConnectionError as e:
        log.critical(f"Redis connection failed: {e}")
        sys.exit(1)


# ----------------- STITCHING -----------------
def stitch_tiles(r, job_id):
    """Stitch processed tiles into final image."""
    log.info(f"[{job_id}] Stitching thread started...")
    s0 = time.perf_counter()

    job_data = r.hgetall(job_id)
    if not job_data:
        log.error(f"[{job_id}] No metadata found in Redis")
        return

    try:
        total_tiles = int(job_data.get('total_tiles', 0))
        grid_width = int(job_data.get('grid_width', 0))
        tile_w = int(job_data.get('tile_width', 0))
        tile_h = int(job_data.get('tile_height', 0))
        final_w = int(job_data.get('original_width', 0))
        final_h = int(job_data.get('original_height', 0))
    except Exception as e:
        log.error(f"[{job_id}] Metadata decode error: {e}")
        return

    if not all([total_tiles, grid_width, tile_w, tile_h, final_w, final_h]):
        log.error(f"[{job_id}] Missing grid/tile/final dimensions")
        return

    final_img = Image.new('L', (final_w, final_h))
    missing = 0

    for i in range(total_tiles):
        path = os.path.join(PROCESSED_DIR, job_id, f"tile_{i}.jpg")
        if not os.path.exists(path):
            missing += 1
            continue

        with Image.open(path) as tile:
            x = (i % grid_width) * tile_w
            y = (i // grid_width) * tile_h
            final_img.paste(tile, (x, y))

    os.makedirs(FINAL_DIR, exist_ok=True)
    final_path = os.path.join(FINAL_DIR, f"{job_id}_complete.jpg")

    stitch_ms = (time.perf_counter() - s0) * 1000
    log.info(f"[{job_id}] Stitching completed in {stitch_ms:.1f} ms. Final image saved: {final_path} (missing_tiles={missing})")

    try:
        r.hset(job_id, mapping={"status": "complete", "final_filename": os.path.basename(final_path)})
        log.info(f"[{job_id}] Redis updated: status=complete")
    except Exception as e:
        log.error(f"[{job_id}] Failed to update Redis: {e}")


# ----------------- MAIN LOOP -----------------
def main():
    log.info("Result service starting...")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FINAL_DIR, exist_ok=True)

    try:
        consumer = create_consumer(BOOTSTRAP_SERVERS, GROUP_ID)
        r = connect_to_redis()
    except Exception as e:
        log.critical(f"Initialization failed: {e}")
        sys.exit(1)

    log.info(f"Connected to Kafka broker {BOOTSTRAP_SERVERS}")
    consumer.subscribe([RESULT_TOPIC])
    log.info(f"Subscribed to topic: {RESULT_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
            except Exception:
                log.warning("Received invalid JSON message")
                continue

            job_id = data.get('job_id')
            tile_id = data.get('tile_id')
            tile_b64 = data.get('processed_data')

            if not all([job_id, tile_id is not None, tile_b64]):
                log.warning("Received incomplete tile data")
                continue

            # timing before decode
            t0 = time.perf_counter()
            try:
                tile_bytes = base64.b64decode(tile_b64)
            except Exception as e:
                log.error(f"[{job_id}] Base64 decode failed: {e}")
                continue
            decode_ms = (time.perf_counter() - t0) * 1000

            job_dir = os.path.join(PROCESSED_DIR, job_id)
            os.makedirs(job_dir, exist_ok=True)

            tile_path = os.path.join(job_dir, f"tile_{tile_id}.jpg")

            # timing before writing file
            w0 = time.perf_counter()
            try:
                with open(tile_path, 'wb') as f:
                    f.write(tile_bytes)
            except Exception as e:
                log.error(f"[{job_id}] Error writing tile: {e}")
                continue
            write_ms = (time.perf_counter() - w0) * 1000

            # timing before redis update
            r0 = time.perf_counter()
            received = r.hincrby(job_id, "received_tiles", 1)
            total = int(r.hget(job_id, "total_tiles") or 0)
            redis_ms = (time.perf_counter() - r0) * 1000

            log.info(
                f"[{job_id}] Tile {tile_id} saved ({len(tile_bytes)}B) "
                f"decode={decode_ms:.1f}ms write={write_ms:.1f}ms redis={redis_ms:.1f}ms "
                f"received={received}/{total}"
            )

            if total > 0 and received == total:
                log.info(f"[{job_id}] All tiles received. Starting stitch thread.")

                stitching_thread = threading.Thread(
                    target=stitch_tiles,
                    args=(r, job_id,),
                    daemon=True
                )
                stitching_thread.start()

    except KeyboardInterrupt:
        log.info("Stopping results service...")
    finally:
        consumer.close()
        log.info("Service stopped.")


if __name__ == '__main__':
    main()
