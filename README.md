# Real-Time Stock Analytics Platform
### by The Data Alchemist

## Overview
**The Data Alchemist** team built a robust, real-time data engineering pipeline designed to ingest, process, and visualize stock market data with low latency. Leveraging industry-standard technologies like Apache Kafka and Apache Spark, it simulates a production-grade streaming architecture capable of handling high-velocity financial data.

## Architecture
The pipeline follows a modern decoupled streaming architecture:

1.  **Ingestion Layer (Apache Kafka)**:
    *   **Producer**: Simulates real-time stock ticks by streaming data from a dataset. Scalable to multiple instances.
    *   **Broker**: Apache Kafka acts as the central message bus, ensuring durable and reliable data transport.

2.  **Processing Layer (Apache Spark)**:
    *   **Consumer**: A PySpark Structured Streaming application that consumes data from Kafka.
    *   **Analytics**: Performs real-time transformations and detects price anomalies using threshold-based logic (simulating Z-Score analysis).

3.  **Visualization Layer (Streamlit)**:
    *   **Dashboard**: A professional, real-time UI that displays metrics, price trends, and alerts.
    *   **Features**: Interactive Plotly charts (Dark Mode), dynamic watchlists, and auto-refresh capabilities.

## Prerequisites
Ensure the following are installed on your system:
*   **Docker & Docker Compose**: For running the Kafka and Zookeeper infrastructure.
*   **Python 3.8+**: For running the producer and dashboard scripts.
*   **Java 11 (OpenJDK)**: Required for PySpark execution.

## Installation & Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd Data_Eng_Project
```

### 2. Set Up Virtual Environment
Create and activate a Python virtual environment to manage dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
Install the required Python libraries:
```bash
pip install -r requirements.txt
```
*Key libraries: `pyspark`, `kafka-python`, `streamlit`, `plotly`, `pandas`.*

## Usage Guide (End-to-End Flow)

Follow these steps to run the entire platform locally.

### Step 1: Start Infrastructure & Producers
Launch the Kafka broker, Zookeeper, and the scalable Producer service using Docker Compose.

```bash
docker compose up -d --build --scale producer=3
```

*   **What happens**:
    *   Kafka and Zookeeper containers start.
    *   3 instances of the `producer` service start, streaming stock data to the `test_topic` topic.
    *   **Anomaly Injection**: By default, producers inject a fake anomaly (5x price spike) every 30 minutes.

### Step 2: Start the Analytics Engine
Run the Spark consumer to process the stream. This script reads from Kafka and writes processed data to the file system.

```bash
# Ensure you are in the virtual environment
source venv/bin/activate

# Set environment variables (if not already set globally)
export JAVA_HOME=/opt/homebrew/opt/openjdk@11  # Adjust path to your Java 11 installation
export SPARK_LOCAL_IP=127.0.0.1

# Run the consumer
python consumer.py
```

*   **Output**:
    *   Processed data is written to `./outputs/streaming_data/raw_ticks`.
    *   Detected anomalies are written to `./outputs/anomalies`.

### Step 3: Launch the Dashboard
Open the real-time visualization interface to monitor the data.

```bash
streamlit run dashboard.py
```

*   **Access**: Open your browser to `http://localhost:8501`.
*   **Features**:
    *   **Dashboard Tab**: View real-time metrics, a watchlist, and price trend charts.
    *   **Anomalies Tab**: View a historical list of detected anomalies.
    *   **Analytics Tab**: View detailed statistics.

## Configuration

### Producer Configuration (`producer.py`)
*   `SPEED`: Controls the speed of data emission (default: 0.2s).
*   `ANOMALY_EVERY_SEC`: Frequency of fake anomalies (default: 1800s / 30 mins).
*   `ENABLE_FAKE_ANOMALIES`: Toggle to enable/disable anomaly injection.

### Dashboard Configuration (`dashboard.py`)
*   **Refresh Rate**: The dashboard auto-refreshes every 1 second (when enabled).
*   **Time Window**: Adjustable via the sidebar (default: 3 minutes).

## Troubleshooting

*   **Kafka Connection Issues**: Ensure Docker containers are running (`docker ps`). If running locally on Mac, ensure `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` points to `127.0.0.1`.
*   **Java Errors**: PySpark requires Java 8 or 11. Ensure `JAVA_HOME` is correctly set.
*   **Missing Data**: Check if the `producer` containers are running and if `consumer.py` is successfully writing to the `outputs/` directory.

---
*Built by The Data Alchemist Engineering Team*
