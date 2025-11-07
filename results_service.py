import json
from confluent_kafka import Consumer, KafkaError
import sys

# --- Configuration ---
BOOTSTRAP_SERVERS = '172.27.247.209:9092' 
RESULT_TOPIC = 'results'
GROUP_ID = 'master-results-group'
# --- -----------------

def create_consumer(bootstrap_servers, group_id):
    """Creates and new_configs a Kafka Consumer."""
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def main():
    print("Starting Results Service...")
    
    try:
        consumer = create_consumer(BOOTSTRAP_SERVERS, GROUP_ID)
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        sys.exit(1)
        
    print(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
    
    try:
        consumer.subscribe([RESULT_TOPIC])
        print(f"Subscribed to topic: {RESULT_TOPIC}")
        print("Waiting for results...")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                break
            
            # --- Message Received! ---
            topic = msg.topic()
            msg_value_str = msg.value().decode('utf-8')
            
            if topic == RESULT_TOPIC:
                try:
                    # 1. Parse the JSON string into a Python dictionary
                    data = json.loads(msg_value_str)
                    
                    # 2. Extract the data received
                    job_id = data.get('job_id', 'Unknown Job')
                    status = data.get('status', 'Unknown Status')
                    worker_id = data.get('worker_id', 'Unknown Worker')
                    
                    # 3. Print the message
                    print(f"[ResultsSvc] Received '{status}' for Job ID: {job_id} from worker: {worker_id}")

                except json.JSONDecodeError:
                    print(f"[ResultsSvc] Error: Could not decode JSON: {msg_value_str}")

    except KeyboardInterrupt:
        print("\nStopping results service...")
    finally:
        consumer.close()
        print("Service stopped.")

if __name__ == '__main__':
    main()