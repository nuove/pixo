import json
from confluent_kafka import Consumer, KafkaError
import sys
import os
import base64
import io
import redis
from PIL import Image

# --- Configuration ---
BOOTSTRAP_SERVERS = '172.27.247.209:9092' # Your Kafka Broker
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'
GROUP_ID = 'master-results-group'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

PROCESSED_DIR = 'processed'
FINAL_DIR = 'final'
# --- -----------------

def create_consumer(bootstrap_servers, group_id):
    """Creates and new_configs a Kafka Consumer."""
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def connect_to_redis():
    """Connects to Redis server."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        print("!!! Make sure Redis server is running. !!!")
        sys.exit(1)

def stitch_tiles(r, job_id):
    """
    Stitches processed tiles back into a final image.
    Reads job metadata from Redis.
    """
    try:
        print(f"Job {job_id} complete! Starting image stitching...")
        
        # 1. Get stitching metadata from Redis (set by Aman's app.py)
        job_data = r.hgetall(job_id)
        
        if not job_data:
            print(f"Stitching Error: No metadata found in Redis for {job_id}")
            return

        # Decode values from Redis (which returns strings)
        total_tiles = int(job_data.get('total_tiles', 0))
        grid_width = int(job_data.get('grid_width', 0))
        tile_width = int(job_data.get('tile_width', 0))
        tile_height = int(job_data.get('tile_height', 0))
        
        if grid_width == 0 or tile_width == 0 or tile_height == 0:
            print(f"Stitching Error: Incomplete metadata for {job_id}. Missing grid/tile info.")
            return

        grid_height = total_tiles // grid_width
        
        # Create the new blank canvas (L for Grayscale, as per worker)
        final_image = Image.new('L', (grid_width * tile_width, grid_height * tile_height))
        print(f"Created new canvas for {job_id}: {final_image.width}x{final_image.height}")

        # 2. Loop, open each tile, and paste it
        for i in range(total_tiles):
            tile_path = os.path.join(PROCESSED_DIR, job_id, f"tile_{i}.jpg")
            
            if not os.path.exists(tile_path):
                print(f"Stitching Error: Missing tile {tile_path}")
                continue
                
            with Image.open(tile_path) as tile_img:
                # Calculate x, y position from tile index
                x = (i % grid_width) * tile_width
                y = (i // grid_width) * tile_height
                final_image.paste(tile_img, (x, y))

        # 3. Save the final image
        os.makedirs(FINAL_DIR, exist_ok=True)
        final_path = os.path.join(FINAL_DIR, f"{job_id}_complete.jpg")
        final_image.save(final_path)
        print(f"=== Stitching complete! Final image saved to: {final_path} ===")
        
        # 4. (Optional) Clean up job in Redis
        # r.delete(job_id)

    except Exception as e:
        print(f"Error during stitching for {job_id}: {e}")

def main():
    print("Starting Full-Featured Results Service...")
    
    # Create output directories
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FINAL_DIR, exist_ok=True)

    try:
        consumer = create_consumer(BOOTSTRAP_SERVERS, GROUP_ID)
        r = connect_to_redis()
    except Exception as e:
        print(f"Error during initialization: {e}")
        sys.exit(1)
        
    print(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
    
    try:
        # Subscribe to both results and heartbeats
        consumer.subscribe([RESULT_TOPIC])
        print(f"Subscribed to topics: {RESULT_TOPIC}")
        print("Waiting for results...")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                continue
            
            # --- Message Received! ---
            topic = msg.topic()
            msg_value_str = msg.value().decode('utf-8')
            
            try:
                data = json.loads(msg_value_str)
            except json.JSONDecodeError:
                print(f"[ResultsSvc] Error: Could not decode JSON: {msg_value_str}")
                continue

            # --- RESULT LOGIC ---
            if topic == RESULT_TOPIC:
                try:
                    job_id = data.get('job_id')
                    tile_id = data.get('tile_id')
                    processed_data_b64 = data.get('processed_data')

                    if not all([job_id, tile_id is not None, processed_data_b64]):
                        print(f"[ResultsSvc] Error: Incomplete data in message: {data}")
                        continue
                    
                    # 1. Save tile to disk
                    tile_bytes = base64.b64decode(processed_data_b64)
                    job_dir = os.path.join(PROCESSED_DIR, job_id)
                    os.makedirs(job_dir, exist_ok=True)
                    # Assume worker sends tiles as 0.jpg, 1.jpg, etc.
                    tile_filename = f"tile_{tile_id}.jpg" 
                    tile_path = os.path.join(job_dir, tile_filename)
                    
                    with open(tile_path, 'wb') as f:
                        f.write(tile_bytes)
                    
                    # 2. Update Redis atomically
                    # This increments the 'received_tiles' field by 1
                    received_count = r.hincrby(job_id, "received_tiles", 1)
                    print(f"[ResultsSvc] Saved tile {tile_id} for {job_id}. Total received: {received_count}")
                    
                    # 3. Check for job completion
                    job_metadata = r.hgetall(job_id)
                    total_count = int(job_metadata.get('total_tiles', 0))

                    if total_count > 0 and received_count == total_count:
                        # We have all the tiles! Time to stitch.
                        stitch_tiles(r, job_id)
                    
                except Exception as e:
                    print(f"[ResultsSvc] Error processing result: {e}")

    except KeyboardInterrupt:
        print("\nStopping results service...")
    finally:
        consumer.close()
        print("Service stopped.")

if __name__ == '__main__':
    main()