import json
import redis
import sys
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = '172.27.247.209:9092'
REDIS_HOST = '172.27.111.128' 

REDIS_PORT = 6379
HEARTBEAT_TOPIC = 'heartbeats'
GROUP_ID = 'monitoring-group-v2'

#A worker is "dead" if it doesn't send a heartbeat for 15s
WORKER_TTL_SECONDS = 15

def main():
    print("Starting Dynamic Monitoring Service..")

# --- connect to redis ---
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"Connected to Redis at {REDIS_HOST}")
    except Exception as e:
        print(f"Error connecting to Redis at {REDIS_HOST}: {e}!!")
        print("Make sure Redis is running on server and accessible.")
        sys.exit(1)

    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest'
    }

    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([HEARTBEAT_TOPIC])
        print(f"Connected to Kafka. Subscribed to '{HEARTBEAT_TOPIC}' topic.")
        print("Waiting for heartbeats...")
    except Exception as e:
        print(f"Error connecting to Kafka at {BOOTSTRAP_SERVERS}: {e}")
        print("Make sure broker's IP is correct and Kafka is running.")
        sys.exit(1)


    try:
        # the main consumer loop
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # --- process a received heartbeat ---
            try:
                data = json.loads(msg.value().decode('utf-8'))
                worker_id = data.get('worker_id')

                if worker_id:
                    redis_key = f"worker_status:{worker_id}"

                    # This is the core logic:
                    # 1. SET the worker's key to "alive"
                    # 2. Set that key to automatically expire (ex) after 15 seconds.
                    # This is the "dead worker" detection.
                    r.set(redis_key, "alive", ex=WORKER_TTL_SECONDS)
                    print(f"Refreshed heartbeat for: {worker_id}")

            except json.JSONDecodeError:
                print("Could not decode JSON from heartbeat message.")
            except Exception as e:
                print(f"Error processing heartbeat: {e}")

    except KeyboardInterrupt:
        print("\nStopping monitoring service...")
    finally:
        consumer.close()
        print("Service stopped.")

if __name__ == "__main__":
    main()
