# Pixo

A scalable distributed image processing system that uses Apache Kafka for message queuing, Redis for state management, and Flask for the web interface. The system splits images into tiles, processes them in parallel using distributed workers, and stitches them back together.

## 🏗️ Architecture

The system consists of four main components:

1. **Flask Web Application** (`app.py`) - Handles image uploads and job management
2. **Worker Nodes** (`worker.py`) - Process image tiles and apply grayscale filters
3. **Results Service** (`results_service.py`) - Collects processed tiles and stitches final images
4. **Monitoring Service** (`monitoring_service.py`) - Tracks worker health via heartbeats

![Pixo Architecture](/assets/architecture.png "Optional title")

## 📋 Prerequisites

- **Apache Kafka** (installed at `/opt/kafka`)
- **Apache ZooKeeper** (comes with Kafka)
- **Redis Server**
- **Python 3.8+**
- **pip** (Python package manager)

## 🚀 Setup Instructions

### Step 1: Install Python Dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 2: Start Kafka Infrastructure

#### Terminal 1: Start ZooKeeper

```bash
cd /opt/kafka
bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

#### Terminal 2: Start Kafka Broker

```bash
cd /opt/kafka
sudo bin/kafka-server-start.sh config/server.properties
```

#### Terminal 3: Create Kafka Topics

```bash
cd /opt/kafka

# Create tasks topic (for distributing image tiles to workers)
bin/kafka-topics.sh --create --topic tasks --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

# Create results topic (for collecting processed tiles)
bin/kafka-topics.sh --create --topic results --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create heartbeats topic (for worker health monitoring)
bin/kafka-topics.sh --create --topic heartbeats --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**Topic Explanation:**
- `tasks` - 2 partitions for parallel distribution of image processing tasks
- `results` - Single partition to maintain tile order during collection
- `heartbeats` - Worker status updates every 5 seconds

### Step 3: Start Redis Server

```bash
redis-server
```

Or if Redis is already running as a service:
```bash
brew services start redis  # macOS with Homebrew
# or
sudo systemctl start redis  # Linux with systemd
```

### Step 4: Configure IP Addresses

Update the following IP addresses in the Python files to match your setup:

**In `app.py`:**
```python
BOOTSTRAP_SERVERS = '172.27.247.209:9092'  # Update to your Kafka broker IP
```

**In `worker.py`:**
```python
BOOTSTRAP_SERVERS = '172.27.247.209:9092'  # Update to your Kafka broker IP
```

**In `results_service.py`:**
```python
BOOTSTRAP_SERVERS = '172.27.247.209:9092'  # Update to your Kafka broker IP
REDIS_HOST = 'localhost'  # Update if Redis is on a different host
```

**In `monitoring_service.py`:**
```python
BOOTSTRAP_SERVERS = '172.27.247.209:9092'  # Update to your Kafka broker IP
REDIS_HOST = '172.27.111.128'  # Update to your Redis server IP
```

## 🎮 Running the Application

### Start Services (in separate terminals)

#### Terminal 4: Start Results Service
```bash
python results_service.py
```
This service listens for processed tiles and stitches them into final images.

#### Terminal 5: Start Monitoring Service
```bash
python monitoring_service.py
```
This service tracks worker health and updates Redis with worker status.

#### Terminal 6+: Start Worker(s)
```bash
python worker.py
```
Start multiple workers for parallel processing. Each worker will automatically generate a unique ID.

To manually set a worker ID:
```bash
WORKER_ID=worker-1 python worker.py
```

#### Terminal 7: Start Flask Web Application
```bash
python app.py
```
The web interface will be available at `http://localhost:5001`

## 🖥️ Using the Web Interface

1. Open your browser and navigate to `http://localhost:5001`
2. Click "Select Image File" and choose a PNG or JPG image
3. Click "Process Image" to start the distributed processing
4. Monitor the progress in real-time:
   - **Worker Status** - Shows all active workers and their health
   - **Job Progress** - Displays tile processing progress
   - **Final Result** - Shows the processed grayscale image when complete

## 🔧 Configuration Options

### Image Processing Settings

**Tile Size** (in `app.py`):
```python
tile_size = 512  # Adjust tile size for different performance characteristics
```
- Larger tiles = fewer messages, less overhead
- Smaller tiles = more parallelism, better load distribution

### Worker Heartbeat Interval

**In `worker.py`:**
```python
time.sleep(5)  # Worker sends heartbeat every 5 seconds
```

### Worker Timeout

**In `monitoring_service.py`:**
```python
WORKER_TTL_SECONDS = 15  # Worker considered dead after 15 seconds without heartbeat
```

## 📁 Directory Structure

```
.
├── app.py                  # Flask web application
├── worker.py               # Distributed worker nodes
├── results_service.py      # Results collection and image stitching
├── monitoring_service.py   # Worker health monitoring
├── requirements.txt        # Python dependencies
├── LICENSE                 # Project license
├── README.md               # This file (which links to assets/architecture.png)
├── assets/
│   └── architecture.png
├── templates/
│   └── index.html          # Web interface
├── processed/              # Temporary processed tiles (auto-created)
│   └── <job-id>/
│       └── tile_*.jpg
└── final/                  # Final stitched images (auto-created)
    └── <job-id>_complete.jpg
```

# 🚀 Deployment & Team Setup
This project is designed to run on a distributed, 4-node (PC) cluster. Here is the recommended mapping of services to systems:

### System 1 (Web & State):

`redis-server (The central Redis database)`
`python app.py (The Flask web application)`

### System 2 (Broker & Results):

```
zookeeper-server-start.sh (Kafka's ZooKeeper)
kafka-server-start.sh (The Kafka Broker)
python results_service.py (The results collector & image stitcher)
```
### System 3 (Worker 1):

`python worker.py (An instance of the processing worker)`

### System 4 (Worker 2 & Monitoring):

```
python worker.py (A second instance of the processing worker)
python monitoring_service.py (The worker heartbeat monitor)
```

>**Note:** All systems must be on the same network (e.g., connected via ZeroTier or on the same LAN) and all IP addresses in the scripts must be updated to point to the correct system's IP.

# 📈 A Note on Scalability
This 4-node setup is just an example. The architecture is horizontally scalable.

You can add more `worker.py` instances on new machines at any time. The Kafka consumer group `(image-processor-group)` will automatically discover and load-balance tasks to them.

>**Important:** To scale beyond 2 workers, you must increase the partition count on the tasks topic. The number of partitions is the maximum number of parallel consumers you can have. If you want 10 workers, you must re-create the tasks topic with at least 10 partitions.
