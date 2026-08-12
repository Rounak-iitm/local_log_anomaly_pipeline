# Local Log Anomaly Pipeline

A local end-to-end **log anomaly detection pipeline** built using **Python, Apache Kafka, PyFlink, and PySpark**.

The project generates simulated server logs, streams them through Kafka, detects anomalies in real time using PyFlink, and performs historical analysis using PySpark.

---

## Architecture

```text
                    ┌─────────────────────┐
                    │    Python Producer  │
                    │                     │
                    │  Generate Server    │
                    │       Logs         │
                    └──────────┬──────────┘
                               │
                               │ JSON Events
                               ▼
                    ┌─────────────────────┐
                    │       Apache       │
                    │       Kafka        │
                    │                     │
                    │    server-logs     │
                    └──────────┬──────────┘
                               │
                               │ Streaming Data
                               ▼
                    ┌─────────────────────┐
                    │      PyFlink       │
                    │                     │
                    │  Parse Logs        │
                    │  Detect Anomalies  │
                    └──────────┬──────────┘
                               │
                               │ Anomalous Events
                               ▼
                    ┌─────────────────────┐
                    │   Anomaly Output   │
                    └─────────────────────┘


       ┌───────────────────────────────────────┐
       │        Historical Server Logs        │
       │          server_logs.csv             │
       └──────────────────┬────────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │     PySpark     │
                 │                 │
                 │  Aggregations   │
                 │  Statistics     │
                 │  Investigation  │
                 └─────────────────┘
```

---

## Features

* Simulated server log generation
* Kafka-based event streaming
* Real-time anomaly detection with PyFlink
* Historical log analysis with PySpark
* Configurable anomaly detection rules
* Docker-based Kafka setup
* Local CSV storage for historical analysis
* Sample execution outputs for verification

---

## Technologies

| Technology   | Purpose                                           |
| ------------ | ------------------------------------------------- |
| Python       | Log generation and application logic              |
| Apache Kafka | Real-time event streaming                         |
| PyFlink      | Real-time stream processing and anomaly detection |
| PySpark      | Batch analytics and historical investigation      |
| Docker       | Running Kafka locally                             |
| Git/GitHub   | Version control                                   |

---

## Project Structure

```text
log-anomaly-project/
│
├── producer.py
│       └── Generates simulated server logs
│
├── flink_processor.py
│       └── Reads Kafka logs and detects anomalies
│
├── spark_analysis.py
│       └── Performs historical analysis using PySpark
│
├── requirements.txt
│       └── Python dependencies
│
├── docker-compose.yml
│       └── Local Kafka configuration
│
├── lib/
│   └── flink-connector-kafka-3.4.0-1.20.jar
│       └── Kafka connector for Flink
│
├── data/
│       └── Local generated data
│
├── outputs/
│       └── Sample execution outputs
│
└── terminal_commands/
    └── commands.txt
        └── Useful project commands
```

---

# Prerequisites

Install the following before running the project:

* Python 3.x
* Docker Desktop
* Git
* Java/JDK compatible with your installed PySpark and Flink versions

Verify Python:

```bash
python --version
```

Verify Docker:

```bash
docker --version
```

Verify Git:

```bash
git --version
```

---

# Installation

## 1. Clone the repository

```bash
git clone https://github.com/Rounak-iitm/local_log_anomaly_pipeline.git
cd local_log_anomaly_pipeline
```

---

## 2. Create a virtual environment

### Windows

```powershell
python -m venv venv
venv\Scripts\activate
```

### Linux/macOS

```bash
python -m venv venv
source venv/bin/activate
```

---

## 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

The main dependencies include:

```text
PySpark
PyFlink
kafka-python
```

---

# Start Kafka

The project uses Docker Compose to run Kafka locally.

Start the Kafka container:

```bash
docker compose up -d
```

Check that the container is running:

```bash
docker ps
```

You should see the Kafka container running.

---

# Create Kafka Topics

Create the input topic:

```bash
docker exec log-anomaly-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic server-logs \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Create the anomaly topic:

```bash
docker exec log-anomaly-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic anomalies \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Verify the topics:

```bash
docker exec log-anomaly-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected:

```text
anomalies
server-logs
```

---

# Run the Log Producer

Start the Python log generator:

```bash
python producer.py
```

The producer generates events similar to:

```text
2026-08-12 10:35:01 | 192.168.1.12 | GET | /api/products | 200 | 173ms
2026-08-12 10:35:02 | 192.168.1.17 | POST | /api/order | 201 | 229ms
2026-08-12 10:35:03 | 10.0.0.45 | GET | /api/login | 200 | 182ms
```

Each event is:

```text
timestamp
ip
method
endpoint
status_code
response_time
```

The producer sends the events to:

```text
Kafka → server-logs
```

It also stores a local copy for Spark analysis:

```text
data/logs/server_logs.csv
```

---

# Verify Kafka Messages

In another terminal, consume messages from Kafka:

```bash
docker exec -it log-anomaly-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic server-logs \
  --from-beginning
```

Example:

```json
{
  "timestamp": "2026-08-12 10:35:01",
  "ip": "192.168.1.12",
  "method": "GET",
  "endpoint": "/api/products",
  "status_code": 200,
  "response_time": 173
}
```

---

# Run PyFlink

The PyFlink processor reads the streaming logs from Kafka.

Run:

```bash
python flink_processor.py
```

The processor evaluates each event against anomaly detection rules.

---

# Anomaly Detection Rules

Currently, an event is classified as anomalous when one or more of the following conditions are met.

### 1. High response time

```text
response_time > 1000 ms
```

Example:

```text
response_time = 3200 ms
```

Reason:

```text
high_response_time
```

---

### 2. Server error

```text
status_code >= 500
```

Examples:

```text
500
502
503
```

Reason:

```text
server_error
```

---

### 3. Suspicious IP

IPs beginning with:

```text
10.
```

are considered suspicious by the demo rules.

Example:

```text
10.0.0.72
```

Reason:

```text
suspicious_ip
```

---

## Example Anomaly

```json
{
  "timestamp": "2026-08-12 10:42:19",
  "ip": "10.0.0.72",
  "method": "GET",
  "endpoint": "/api/products",
  "status_code": 200,
  "response_time": 182,
  "anomaly": true,
  "reasons": [
    "suspicious_ip"
  ]
}
```

Multiple rules can also trigger for the same event.

---

# Run PySpark Analysis

After generating some logs, run:

```bash
python spark_analysis.py
```

Spark loads:

```text
data/logs/server_logs.csv
```

and performs several analyses.

### Status Code Distribution

```text
200
201
400
404
500
503
```

### Endpoint Statistics

For each endpoint, Spark calculates:

* Number of requests
* Average response time
* Maximum response time
* Minimum response time

### Slow Requests

Spark identifies:

```text
response_time > 1000 ms
```

### Server Errors

Spark identifies:

```text
status_code >= 500
```

---

# Complete Execution Flow

For a complete demonstration, use separate terminals.

### Terminal 1 — Kafka

```bash
docker compose up -d
```

### Terminal 2 — Producer

```bash
venv\Scripts\activate
python producer.py
```

### Terminal 3 — PyFlink

```bash
venv\Scripts\activate
python flink_processor.py
```

### Terminal 4 — PySpark

After collecting enough logs:

```bash
venv\Scripts\activate
python spark_analysis.py
```

---

# Data Flow

```text
Python
  │
  │ Generate logs
  ▼
Kafka
  │
  │ server-logs topic
  ▼
PyFlink
  │
  ├── Parse events
  ├── Check response time
  ├── Check status codes
  └── Check IP addresses
  │
  ▼
Anomaly Detection


Historical CSV
  │
  ▼
PySpark
  │
  ├── Status statistics
  ├── Endpoint statistics
  ├── Response-time analysis
  └── Server-error analysis
```

---

# Example Server Log

```text
2026-08-12 10:20:01,192.168.1.10,GET,/api/products,200,120
2026-08-12 10:20:02,192.168.1.15,GET,/api/login,500,850
2026-08-12 10:20:03,192.168.1.20,POST,/api/order,201,230
```

Format:

```text
timestamp,ip,method,endpoint,status_code,response_time
```

---

# Why Kafka + Flink + Spark?

### Kafka

Kafka acts as the event streaming layer.

It allows log events to be produced independently from the applications processing them.

### PyFlink

Flink is used for real-time processing.

It can inspect incoming events immediately and identify potentially anomalous requests.

### PySpark

Spark is used for historical analysis.

It is useful for investigating accumulated logs and finding patterns such as:

* Frequently failing endpoints
* High-latency APIs
* HTTP error distributions
* Response-time patterns

---

# Future Improvements

The current project is intentionally a local demonstration pipeline. Possible improvements include:

* Write detected anomalies to a Kafka `anomalies` topic
* Add a Kafka consumer for anomaly notifications
* Store anomalies in PostgreSQL or Elasticsearch
* Add a dashboard using Grafana
* Add real server/application log ingestion
* Add window-based anomaly detection in Flink
* Add statistical anomaly detection
* Add machine-learning-based anomaly detection
* Add configurable thresholds
* Add structured logging
* Add unit and integration tests
* Containerize the complete pipeline
* Add CI/CD using GitHub Actions
* Add monitoring and metrics
* Add alerting for critical anomalies

---

# Current Limitations

This project is intended for learning and local experimentation.

The anomaly detection rules are currently deterministic and threshold-based rather than machine-learning-based.

The generated logs are simulated rather than collected from a production server.

The current PyFlink implementation prints detected anomalies rather than publishing them to the `anomalies` Kafka topic.

---

# Learning Objectives

This project demonstrates how multiple data-engineering technologies can work together:

```text
Event Generation
       ↓
Message Streaming
       ↓
Real-Time Processing
       ↓
Anomaly Detection
       ↓
Historical Analytics
```

It provides practical experience with:

* Kafka producers and topics
* Stream processing
* PyFlink
* PySpark
* Docker
* Event-driven architectures
* Log analytics
* Anomaly detection
* Local data-engineering pipelines

---

# Author

**Rounak**

GitHub:

https://github.com/Rounak-iitm

Repository:

https://github.com/Rounak-iitm/local_log_anomaly_pipeline

