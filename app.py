import json
import uuid
from flask import Flask
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = '172.27.247.209:9092' 
TASK_TOPIC = 'tasks'

app = Flask(__name__)
producer = None

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
    return 'Master UI is running. Go to /test-upload to send a test message.'

@app.route('/test-upload')
def test_upload():
    """
    Sends a single 'Hello World' test message to the 'tasks' topic.
    """
    kafka_producer = get_kafka_producer()
    if kafka_producer is None:
        return "Error: Kafka Producer is not connected.", 500

    job_id = f"job-{uuid.uuid4().hex[:6]}" 
    
    test_task = {
        'job_id': job_id,
        'message': 'ping'
    }
    
    try:
        payload = json.dumps(test_task).encode('utf-8')
        
        kafka_producer.produce(TASK_TOPIC, key=str(job_id), value=payload)
        kafka_producer.flush()
        
        print(f"Sent test task for Job ID: {job_id}")
        return f"Test task sent for Job ID: {job_id}"
        
    except Exception as e:
        print(f"Error sending message: {e}")
        return f"Error sending message: {e}", 500

if __name__ == '__main__':
    get_kafka_producer()
    app.run(host='0.0.0.0', port=5001, debug=True)
