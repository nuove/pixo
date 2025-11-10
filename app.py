import json
import uuid
import io
import base64
import os
import time
import logging
from flask import Flask, request, render_template, jsonify, send_from_directory
from confluent_kafka import Producer
import redis
from PIL import Image

# ----------------- CONFIG -----------------
BOOTSTRAP_SERVERS = '172.27.247.209:9092'
TASK_TOPIC = 'tasks'
FINAL_IMAGE_DIR = 'final'

# ----------------- LOGGING -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("app")

# ----------------- APP / GLOBALS -----------------
app = Flask(__name__)
producer = None

# Try Redis once at startup (optional)
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()
    redis_available = True
    log.info("Redis connected successfully.")
except redis.ConnectionError:
    log.warning("Redis is NOT available. Running without Redis functionality.")
    log.warning("Make sure Redis is running on this machine.")
    r = None
    redis_available = False


def get_kafka_producer():
    """Initializes and returns a Kafka Producer."""
    global producer
    if producer is None:
        try:
            conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
            producer = Producer(conf)
            log.info("Kafka Producer initialized successfully.")
        except Exception as e:
            log.critical(f"Error initializing Kafka Producer: {e}")
            log.info("Check BOOTSTRAP_SERVERS and whether Kafka is running.")
            return None
    return producer


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    if 'image' not in request.files:
        log.warning("Upload rejected: no image file part")
        return "No image file found.", 400

    image_file = request.files['image']
    try:
        image = Image.open(image_file.stream)
    except Exception as e:
        log.error(f"Invalid image: {e}")
        return "Invalid image file.", 400

    job_id = f"job-{uuid.uuid4().hex}"

    tile_size = 512
    width, height = image.size
    num_tiles_x = (width + tile_size - 1) // tile_size  # Ceiling division
    num_tiles_y = (height + tile_size - 1) // tile_size
    total_tiles = num_tiles_x * num_tiles_y

    if redis_available and r:
        try:
            pipe = r.pipeline()
            pipe.hset(job_id, 'total_tiles', total_tiles)
            pipe.hset(job_id, 'received_tiles', 0)
            pipe.hset(job_id, 'status', 'processing')
            pipe.hset(job_id, 'tile_width', tile_size)
            pipe.hset(job_id, 'tile_height', tile_size)
            pipe.hset(job_id, 'grid_width', num_tiles_x)
            pipe.hset(job_id, 'original_width', width)
            pipe.hset(job_id, 'original_height', height)
            pipe.execute()
            log.info(f"[{job_id}] Redis job initialized tiles={total_tiles} size={width}x{height}")
        except redis.RedisError as e:
            log.error(f"[{job_id}] Redis init error: {e}")
            return "Redis error.", 500

    kafka_producer = get_kafka_producer()
    if kafka_producer is None:
        return jsonify({"error": "Kafka Producer is not connected"}), 500

    tile_id = 0
    log.info(f"[{job_id}] Queueing {total_tiles} tiles to topic={TASK_TOPIC}")

    # timing: take snapshot before enqueueing tiles
    t_enqueue0 = time.perf_counter()
    try:
        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                box = (x, y, min(x + tile_size, width), min(y + tile_size, height))
                tile = image.crop(box)

                tile_bytes_io = io.BytesIO()
                tile.save(tile_bytes_io, format='PNG')
                tile_bytes = tile_bytes_io.getvalue()
                tile_data_b64 = base64.b64encode(tile_bytes).decode('utf-8')

                message = {
                    "job_id": job_id,
                    "tile_id": tile_id,
                    "total_tiles": total_tiles,
                    "tile_data": tile_data_b64
                }

                kafka_producer.produce(TASK_TOPIC, key=None, value=json.dumps(message))
                tile_id += 1
    except Exception as e:
        log.error(f"[{job_id}] Error while queuing tile {tile_id}: {e}")
        return jsonify({"error": f"Error sending tile {tile_id}: {e}", "job_id": job_id}), 500

    enqueue_ms = (time.perf_counter() - t_enqueue0) * 1000
    log.info(f"[{job_id}] Enqueued {total_tiles} tiles in {enqueue_ms:.1f} ms; flushing...")

    # timing: take snapshot before flush
    t_flush0 = time.perf_counter()
    try:
        kafka_producer.flush()
    except Exception as e:
        log.error(f"[{job_id}] Kafka flush error: {e}")
        return jsonify({"error": f"Kafka flush failed: {e}", "job_id": job_id}), 500
    flush_ms = (time.perf_counter() - t_flush0) * 1000

    log.info(f"[{job_id}] Successfully sent all tiles (flush {flush_ms:.1f} ms)")

    return jsonify({
        "message": "Successfully queued all tiles!",
        "job_id": job_id,
        "total_tiles": total_tiles
    })


@app.route('/status/<job_id>', methods=['GET'])
def job_status(job_id):
    if not redis_available or r is None:
        log.warning(f"[{job_id}] Status requested but Redis unavailable")
        return jsonify({"error": "Redis is not available", "job_id": job_id}), 503

    try:
        job_data = r.hgetall(job_id)

        # dynamic worker discovery
        live_workers = {}
        for key in r.scan_iter("worker_status:*"):
            worker_id = key.split(":", 1)[1]
            live_workers[worker_id] = "alive"
    except redis.RedisError as exc:
        log.error(f"[{job_id}] Unable to read status: {exc}")
        return jsonify({"error": f"Unable to read job status: {exc}", "job_id": job_id}), 500

    if not job_data:
        log.warning(f"[{job_id}] Job not found")
        return jsonify({"error": "Job not found", "job_id": job_id}), 404

    total_tiles = int(job_data.get('total_tiles', 0))
    received_tiles = int(job_data.get('received_tiles', 0))
    status = job_data.get("status", "processing")

    resp = {
        "status": status,
        "total_tiles": total_tiles,
        "received_tiles": received_tiles,
        "workers": live_workers
    }

    if status == "complete":
        resp["url"] = f"/final/{job_data.get('final_filename')}"

    log.info(f"[{job_id}] status={status} received={received_tiles}/{total_tiles} workers={len(live_workers)}")
    return jsonify(resp)


@app.route('/final/<filename>')
def serve_final_img(filename):
    """Serve the final image from the 'final' directory."""
    log.info(f"Serving /{FINAL_IMAGE_DIR}/{filename}")
    try:
        os.makedirs(FINAL_IMAGE_DIR, exist_ok=True)
        return send_from_directory(FINAL_IMAGE_DIR, filename)
    except FileNotFoundError:
        log.warning(f"Final image not found: {filename}")
        return "File not found.", 404


if __name__ == '__main__':
    get_kafka_producer()
    app.run(host='0.0.0.0', port=5001, debug=True)
