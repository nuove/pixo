import json
import uuid
import io
import base64
from flask import Flask, request, redirect, render_template
from confluent_kafka import Producer
import redis
from PIL import Image

BOOTSTRAP_SERVERS = '172.27.247.209:9092'
TASK_TOPIC = 'tasks'

app = Flask(__name__)
producer = None

# Redis connection with error handling
try:
    r = redis.Redis(host='172.27.247.209', port=6379, decode_responses=True)
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

    # Get Kafka producer
    kafka_producer = get_kafka_producer()
    if kafka_producer is None:
        return "Error: Kafka Producer is not connected.", 500

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

                # Base64 Encode: Convert the tile's image data into a Base64 string
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
                    return f"Error sending tile {tile_id}: {e}", 500

    print(f"Successfully sent all {total_tiles} tiles for job {job_id}")
    return f"Successfully processed image! Job ID: {job_id} with {total_tiles} tiles sent to workers."

if __name__ == '__main__':
    get_kafka_producer()
    app.run(host='0.0.0.0', port=5001, debug=True)
