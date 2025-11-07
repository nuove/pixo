import json
from confluent_kafka import Consumer, Producer, KafkaError
import sys
import socket
import base64 
import io 
from PIL import Image
import os 
import uuid

BOOTSTRAP_SERVERS = '172.27.247.209:9092' 
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
GROUP_ID = 'image-processor-group'

def get_worker_id():
    """
    Generates a unique worker ID based on:
    1) Environment variable WORKER_ID
    2) if not found, then hostname-UUID
    """
    env_worker_id = os.getenv("WORKER_ID")
    if env_worker_id:
        return env_worker_id
    try:
        hostname = socket.gethostname()
        unique_id = uuid.uuid4().hex[:6]
        return f"{hostname}-{unique_id}"
    except Exception as e:
        print(f"Error generating worker ID: {e}")
        return f"worker-{uuid.uuid4().hex[:6]}"

def create_consumer(bootstrap_servers, group_id):
    """Creates and configures a Kafka Consumer."""
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def create_producer(bootstrap_servers):
    """Creates and configures a Kafka Producer."""
    conf = {'bootstrap.servers': bootstrap_servers}
    return Producer(conf)

def process_image(image_bytes):
    """
    Applies a grayscale filter to the image.
    Takes image bytes, returns processed image bytes.
    """
    try:
        image = Image.open(io.BytesIO(image_bytes))
        
        processed_image = image.convert('L')
        
        byte_buffer = io.BytesIO()
        processed_image.save(byte_buffer, format="JPEG")
        return byte_buffer.getvalue()
        
    except Exception as e:
        print(f"Error during image processing: {e}")
        return None
    
WORKER_ID = get_worker_id()

def main():
    print(f"Starting worker: {WORKER_ID}...")
    
    try:
        consumer = create_consumer(BOOTSTRAP_SERVERS, GROUP_ID)
        producer = create_producer(BOOTSTRAP_SERVERS)
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        sys.exit(1)
        
    print(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
    
    try:
        consumer.subscribe([TASK_TOPIC])
        print(f"Subscribed to topic: {TASK_TOPIC}")
        print(f"Waiting for tasks as part of group: {GROUP_ID}...")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                task = json.loads(msg.value().decode('utf-8'))
                job_id = task.get('job_id', 'unknown_job')
                tile_id = task.get('tile_id', 'unknown_tile')
                tile_data_base64 = task.get('tile_data')
                
                if not tile_data_base64:
                    print(f"Skipping message for {job_id}: missing 'tile_data'")
                    continue

                print(f"\n[{WORKER_ID}] Received task for Job ID: {job_id}, Tile: {tile_id}")
                
                image_bytes = base64.b64decode(tile_data_base64)
                
            except Exception as e:
                print(f"Error decoding message: {e}")
                continue

            processed_image_bytes = process_image(image_bytes)
            
            if processed_image_bytes is None:
                print(f"Failed to process image for {job_id}, Tile: {tile_id}")
                continue
                
            processed_data_base64 = base64.b64encode(processed_image_bytes).decode('utf-8')
            print(f"Successfully processed Tile: {tile_id}")

            try:
                result_data = {
                    'job_id': job_id,
                    'tile_id': tile_id,
                    'processed_data': processed_data_base64,
                    'worker_id': WORKER_ID
                }
                
                producer.produce(
                    RESULT_TOPIC, 
                    key=str(job_id), 
                    value=json.dumps(result_data).encode('utf-8')
                )
                producer.flush() 
                print(f"[{WORKER_ID}] Sent processed tile to topic: {RESULT_TOPIC}")
                
            except Exception as e:
                print(f"Error producing result: {e}")

    except KeyboardInterrupt:
        print(f"Stopping worker: {WORKER_ID}...")
    finally:
        consumer.close()
        print("Worker stopped.")

if __name__ == '__main__':
    main()