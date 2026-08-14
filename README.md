# Real-Time Telecom Traffic & Network Error Analytics Pipeline

An end-to-end event-driven data streaming pipeline designed to simulate, process, and analyze call detail records (CDRs) and network status logs from base stations in real time. 

This project simulates real-world telecommunication traffic (e.g., dropped calls, network errors) and streams the data through a distributed message broker (**Apache Kafka**) to a distributed stream processing engine (**Apache Spark**).

---

## Architecture Overview

    [ Data Generator ]  --->  [ Apache Kafka ]  --->  [ Apache Spark ]  --->  [ Console Output ]
     (Python Script)          (Docker Container)     (Structured Streaming)     (Real-Time Analytics)

1. **Data Generator (Python):** Simulates live telecommunication activity (user connections, usage types, base station metrics, and call statuses) as JSON records.
2. **Event Broker (Apache Kafka KRaft):** Ingests and buffers streaming event records asynchronously into the `telecom_traffic` topic.
3. **Stream Processing (PySpark):** Consumes event logs, parses binary JSON payloads, filters network errors/dropped calls, and performs windowed aggregation per base station.
4. **Environment Isolation (Docker):** Runs Apache Kafka natively using KRaft (Kafka Raft Metadata) mode without requiring Zookeeper.

---

## Tech Stack

* **Programming Language:** Python 3.12
* **Stream Processing:** Apache Spark (PySpark v3.5.1)
* **Message Broker:** Apache Kafka (Official Apache Docker Image with KRaft)
* **Containerization:** Docker / Docker Compose
* **Version Control:** Git

---

## Project Structure

    telekom-data-analysis/
    ├── docker-compose.yml     # Kafka service configuration (KRaft mode)
    ├── data_generator.py      # Live streaming telecom log generator
    ├── spark_analyzer.py      # PySpark Structured Streaming consumer & analytics
    ├── .gitignore             # Ignored files (.venv, caches)
    └── README.md              # Project documentation

---

## Setup & Local Deployment Guide

Follow these steps to run the pipeline on your local environment.

### Prerequisites

Ensure you have the following installed on your machine:
* **Docker & Docker Compose**
* **Python 3.10+**
* **Java OpenJDK 17** (Required for Apache Spark)

---

### Step 1: Clone the Repository

    git clone https://github.com/YOUR_USERNAME/telekom-data-analysis.git
    cd telekom-data-analysis

---

### Step 2: Set Up Python Virtual Environment

Create and activate an isolated Python virtual environment:

    # Create virtual environment
    python3 -m venv .venv
    
    # Activate environment (Linux/macOS)
    source .venv/bin/activate
    
    # Install required dependencies
    pip install confluent-kafka pyspark==3.5.1

---

### Step 3: Start Apache Kafka Infrastructure

Spin up the official Apache Kafka container using Docker Compose:

    docker-compose up -d

Verify that the Kafka container is running:

    docker ps

---

### Step 4: Run the Streaming Pipeline

To see the system in action, open **two separate terminal windows** (ensure the virtual environment is activated in both):

**Terminal 1 — Start the Data Generator:**

    python data_generator.py

*Outputs JSON events and streams them to the `telecom_traffic` Kafka topic.*

**Terminal 2 — Start the Spark Stream Analyzer:**

    python spark_analyzer.py

*Listens to the Kafka topic, processes incoming streams, and prints real-time error counts grouped by base station.*

---

## Sample Output

    -------------------------------------------
    Batch: 12
    -------------------------------------------
    +-----------------+-------------------+-----+
    |     base_station|             status|count|
    +-----------------+-------------------+-----+
    |  Eryaman_Optimum| Connection_Dropped|    2|
    |     Tunali_Hilmi|              Error|    1|
    +-----------------+-------------------+-----+

---

## License

This project is open-source and available under the [MIT License](LICENSE).