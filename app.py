import json
import uuid
import io
import base64
from flask import Flask, request, redirect, render_template, jsonify, send_from_directory
from confluent_kafka import Producer
import redis
from PIL import Image

BOOTSTRAP_SERVERS = '172.27.247.209:9092'
TASK_TOPIC = 'tasks'

app = Flask(__name__)
producer = None

# Redis connection with error handling
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()
    redis_available = True
    print("Redis connected successfully.")
except redis.ConnectionError:
    print("Warning: Redis is not available. Running without Redis functionality.")
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
            print(f"Error initializing Kafka Producer: {e}")
            return None
    return producer

@app.route('/')
def index():
     return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    # Receive the uploaded image file
    image_file = request.files['image']

    # Open it with the Pillow library
    image = Image.open(image_file)

    # Generate job_id: Create a unique ID for this job
    job_id = f"job-{uuid.uuid4().hex}"

    # Image Tiling: Write the logic to split the image into 512x512 tiles
    tile_size = 512
    width, height = image.size
    num_tiles_x = (width + tile_size - 1) // tile_size  # Ceiling division
    num_tiles_y = (height + tile_size - 1) // tile_size  # Ceiling division
    total_tiles = num_tiles_x * num_tiles_y

    # Initialize Job in Redis: Connect to Redis and set the total tile count
    if redis_available and r:
        r.hset(job_id, 'total_tiles', total_tiles)
        r.hset(job_id, 'received_tiles', 0)

        r.hset(job_id, 'tile_width', tile_size)
        r.hset(job_id, 'tile_height', tile_size)
        r.hset(job_id, 'grid_width', num_tiles_x)
        r.hset(job_id, 'original_width', width)
        r.hset(job_id, 'original_height', height)
        

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

                # Convert tile to bytes
                tile_bytes = io.BytesIO()
                tile.save(tile_bytes, format='PNG')
                tile_bytes = tile_bytes.getvalue()

                tile_data_b64 = base64.b64encode(tile_bytes).decode('utf-8')

                # Create JSON: Create a new JSON message
                message = {
                    "job_id": job_id,
                    "tile_id": tile_id,
                    "total_tiles": total_tiles,
                    "tile_data": tile_data_b64
                }

                kafka_producer.produce(TASK_TOPIC, key=None, value=json.dumps(message))

                tile_id += 1

        # ---flushing once after the loop---
        print(f"Flushing all {total_tiles} tiles to Kafka...")
        kafka_producer.flush() # <-- Much faster!
        print(f"Successfully sent all {total_tiles} tiles for job {job_id}")
        # ----------------------------------

    except Exception as e:
        print(f"Error sending tile {tile_id}: {e}")
        return jsonify({
            "error": f"Error sending tile {tile_id}: {e}",
            "job_id": job_id
        }), 500

    print(f"Successfully sent all {total_tiles} tiles for job {job_id}")

    return jsonify({
        "message": "Successfully processed image!",
        "job_id": job_id,
        "total_tiles": total_tiles,
        "tile_size": tile_size
    })

@app.route('/status/<job_id>', methods=['GET'])
@app.route('/api/status/<job_id>', methods=['GET'])
def job_status(job_id):
    if not redis_available or r is None:
        return jsonify({
            "error": "Redis is not available",
            "job_id": job_id
        }), 503

    try:
        total_tiles = r.hget(job_id, 'total_tiles')
        received_tiles = r.hget(job_id, 'received_tiles')
        job_data = r.hgetall(job_id)
        status = job_data.get("status", "processing")
    except redis.RedisError as exc:
        return jsonify({
            "error": f"Unable to read job status: {exc}",
            "job_id": job_id
        }), 500

    if total_tiles is None or received_tiles is None:
        return jsonify({
            "error": "Job not found",
            "job_id": job_id
        }), 404

    total_tiles = int(total_tiles)
    received_tiles = int(received_tiles)

    if status == "complete":
        final_filename = job_data.get('final_filename')
        return jsonify({
            "status": "complete",
            "url": f"/final/{final_filename}"
        })
    else:
        return jsonify({
            "status": "processing",
            "total_tiles": total_tiles,
            "received_tiles": received_tiles
        })

@app.route('/final/<filename>')
def serve_final_img(filename):
    """
    Catches the URL from the JS (e.g., /final/job-123.jpg)
    and serves the physical file from the 'final' directory.
    """
    print(f"Serving file: {filename} from /final")
    try:
        return send_from_directory("final", filename)
    except FileNotFoundError:
        return "File not found.", 404

if __name__ == '__main__':
    get_kafka_producer()
    app.run(host='0.0.0.0', port=5001, debug=True)