import json
import redis
import sys
from confluent_kafka import Consumer, KafkaError
import logging

# ----------------- CONFIG -----------------
BOOTSTRAP_SERVERS = '172.27.247.209:9092'
REDIS_HOST = '172.27.111.128'
REDIS_PORT = 6379

HEARTBEAT_TOPIC = 'heartbeats'
GROUP_ID = 'monitoring-group-v2'

# A worker is "dead" if it doesn't send a heartbeat for 15s
WORKER_TTL_SECONDS = 15

# ----------------- LOGGING -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("monitoring")


def main():
    log.info("Starting Dynamic Monitoring Service...")

    # --- connect to redis ---
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        log.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        log.critical(f"Error connecting to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
        log.info("Make sure Redis is running and accessible.")
        sys.exit(1)

    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest'
    }

    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([HEARTBEAT_TOPIC])
        log.info(f"Connected to Kafka {BOOTSTRAP_SERVERS}")
        log.info(f"Subscribed to topic: {HEARTBEAT_TOPIC} (group={GROUP_ID})")
        log.info("Waiting for heartbeats...")
    except Exception as e:
        log.critical(f"Error connecting to Kafka at {BOOTSTRAP_SERVERS}: {e}")
        log.info("Ensure the broker IP is correct and Kafka is running.")
        sys.exit(1)

    try:
        # --- main consumer loop ---
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning(f"Consumer error: {msg.error()}")
                continue

            # --- process a received heartbeat ---
            try:
                payload = msg.value().decode('utf-8')
                data = json.loads(payload)
                worker_id = data.get('worker_id')

                if not worker_id:
                    log.warning("Heartbeat missing worker_id; skipping")
                    continue

                redis_key = f"worker_status:{worker_id}"
                # Core logic:
                # 1) mark worker as "alive"
                # 2) key expires after WORKER_TTL_SECONDS (dead-worker detection)
                r.set(redis_key, "alive", ex=WORKER_TTL_SECONDS)
                log.info(f"Refreshed heartbeat for worker={worker_id} ttl={WORKER_TTL_SECONDS}s")

            except json.JSONDecodeError:
                log.warning("Invalid heartbeat JSON; skipping")
            except UnicodeDecodeError:
                log.warning("Heartbeat not UTF-8; skipping")
            except Exception as e:
                log.error(f"Error processing heartbeat: {e}")

    except KeyboardInterrupt:
        log.info("Stopping monitoring service (keyboard interrupt)")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        log.info("Service stopped.")


if __name__ == "__main__":
    main()
