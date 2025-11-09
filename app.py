import json
import uuid
import io
import base64
import os
from flask import Flask, request, redirect, render_template, jsonify, send_from_directory
from confluent_kafka import Producer
import redis
from PIL import Image

BOOTSTRAP_SERVERS = '172.27.247.209:9092'

TASK_TOPIC = 'tasks'
FINAL_IMAGE_DIR = 'final' 

app = Flask(__name__)
producer = None

try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()
    redis_available = True
    print("Redis connected successfully.")
except redis.ConnectionError:
    print("!!! Warning: Redis is not available. Running without Redis functionality. !!!")
    print("!!! Make sure Redis is running on this machine. !!!")
    r = None
    redis_available = False

def get_kafka_producer():
    """Initializes and returns a Kafka Producer."""
    global producer
    if producer is None:
        try:
            conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
            producer = Producer(conf)
            print("Kafka Producer initialized successfully.")
        except Exception as e:
            print(f"!!! Error initializing Kafka Producer: {e}")
            print(f"!!! Check BOOTSTRAP_SERVERS IP and if Kafka is running. !!!")
            return None
    return producer

@app.route('/')
def index():
     return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'image' not in request.files:
        return "No image file found.", 400
    image_file = request.files['image']

    try:
        image = Image.open(image_file.stream)
    except Exception as e:
        print(f"Error opening image: {e}")
        return "Invalid image file.", 400

    job_id = f"job-{uuid.uuid4().hex}"

    tile_size = 512
    width, height = image.size
    num_tiles_x = (width + tile_size - 1) // tile_size  # Ceiling division
    num_tiles_y = (height + tile_size - 1) // tile_size  # Ceiling division
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
        except redis.RedisError as e:
            print(f"Error initializing job in Redis: {e}")
            return "Redis error.", 500

    # Get Kafka producer
    kafka_producer = get_kafka_producer()
    if kafka_producer is None:
        return jsonify({
            "error": "Kafka Producer is not connected"
        }), 500

    # Loop & Produce: For each tile
    tile_id = 0
    print(f"Starting to queue {total_tiles} tiles for job {job_id}")
    try:
        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                # Crop the tile
                box = (x, y, min(x + tile_size, width), min(y + tile_size, height))
                tile = image.crop(box)

                # Convert tile to bytes (use PNG for lossless quality)
                tile_bytes_io = io.BytesIO()
                tile.save(tile_bytes_io, format='PNG')
                tile_bytes = tile_bytes_io.getvalue()

                tile_data_b64 = base64.b64encode(tile_bytes).decode('utf-8')

                # Create JSON: Create a new JSON message
                message = {
                    "job_id": job_id,
                    "tile_id": tile_id,
                    "total_tiles": total_tiles,
                    "tile_data": tile_data_b64
                }

                # Produce message
                kafka_producer.produce(TASK_TOPIC, key=None, value=json.dumps(message))
                tile_id += 1

        # flushing once after the loop(cause this is much faster)
        print(f"Flushing all {total_tiles} tiles to Kafka...")
        kafka_producer.flush() 
        print(f"Successfully sent all {total_tiles} tiles for job {job_id}")

    except Exception as e:
        print(f"Error sending tile {tile_id}: {e}")
        return jsonify({
            "error": f"Error sending tile {tile_id}: {e}",
            "job_id": job_id
        }), 500

    # Return the job ID to the frontend
    return jsonify({
        "message": "Successfully queued all tiles!",
        "job_id": job_id,
        "total_tiles": total_tiles
    })

@app.route('/status/<job_id>', methods=['GET'])
def job_status(job_id):
    if not redis_available or r is None:
        return jsonify({
            "error": "Redis is not available",
            "job_id": job_id
        }), 503

    try:
        job_data = r.hgetall(job_id)

        # dynamic worker discovery
        live_workers = {}
        # scan for all keys that match the pattern "worker_status:*"
        for key in r.scan_iter("worker_status:*"):
            worker_id = key.split(":", 1)[1]
            live_workers[worker_id] = "alive"

    except redis.RedisError as exc:
        return jsonify({
            "error": f"Unable to read job status: {exc}",
            "job_id": job_id
        }), 500

    if not job_data:
        return jsonify({
            "error": "Job not found",
            "job_id": job_id
        }), 404

    total_tiles = int(job_data.get('total_tiles', 0))
    received_tiles = int(job_data.get('received_tiles', 0))
    status = job_data.get("status", "processing")

    response_data = {
        "status": status,
        "total_tiles": total_tiles,
        "received_tiles": received_tiles,
        "workers": live_workers # dynamic list
    }

    if status == "complete":
        response_data["url"] = f"/final/{job_data.get('final_filename')}"

    return jsonify(response_data)


@app.route('/final/<filename>')
def serve_final_img(filename):
    """
    Catches the URL from the JS (e.g., /final/job-123.jpg)
    and serves the physical file from the 'final' directory.
    """
    print(f"Serving file: {filename} from /{FINAL_IMAGE_DIR}")
    try:
        os.makedirs(FINAL_IMAGE_DIR, exist_ok=True)
        return send_from_directory(FINAL_IMAGE_DIR, filename)
    except FileNotFoundError:
        return "File not found.", 404

if __name__ == '__main__':
    get_kafka_producer()
    app.run(host='0.0.0.0', port=5001, debug=True)