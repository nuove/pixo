import json
from confluent_kafka import Consumer, Producer, KafkaError
import sys
import socket

BOOTSTRAP_SERVERS = '172.27.247.209:9092' 
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
GROUP_ID = 'image-processor-group'

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

def main():
    print("Starting worker...")
    
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
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            
            try:
                # Decode the message from bytes to a string
                msg_value_str = msg.value().decode('utf-8')
                # Parse the string as JSON
                task = json.loads(msg_value_str)
                job_id = task.get('job_id', 'unknown_job')
                
                print(f"\n[Worker-1] Received task for Job ID: {job_id}")
                print(f"Task data: {task}")
                
            except Exception as e:
                print(f"Error processing message: {e}")
                print(f"Raw message data: {msg.value()}")
                continue

            try:
                result_data = {
                    'job_id': job_id,
                    'status': 'pong',
                    'worker_id': 'worker-1'
                }
                
                producer.produce(
                    RESULT_TOPIC, 
                    key=str(job_id), 
                    value=json.dumps(result_data).encode('utf-8')
                )
                
                producer.flush() 
                print(f"Sent receipt to topic: {RESULT_TOPIC}")
                
            except Exception as e:
                print(f"Error producing result: {e}")

    except KeyboardInterrupt:
        print("Stopping worker...")
    finally:
        consumer.close()
        print("Worker stopped.")

if __name__ == '__main__':
    main()