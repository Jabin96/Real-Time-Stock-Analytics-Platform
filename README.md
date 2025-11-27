# The Data Alchemists - Real-Time Stock Analytics Platform

## Overview
**The Data Alchemists** built a robust, real-time data engineering pipeline designed to ingest, process, and visualize stock market data with low latency. Built using industry-standard technologies, it simulates a production-grade streaming architecture capable of handling high-velocity financial data.

## Architecture
The pipeline follows a modern streaming architecture:

1.  **Ingestion Layer (Kafka)**:
    *   **Producer**: Simulates real-time stock ticks by streaming data from a CSV dataset or generating synthetic events.
    *   **Broker**: Apache Kafka acts as the central message bus, ensuring durable and decoupled data transport.

2.  **Processing Layer (Apache Spark)**:
    *   **Consumer**: A PySpark Structured Streaming application that consumes data from Kafka.
    *   **Analytics**: Performs real-time transformations, calculates 5-minute moving averages, and detects price anomalies using Z-Score statistical analysis.

3.  **Visualization Layer (Streamlit)**:
    *   **Dashboard**: A professional, flicker-free UI that displays real-time metrics, price trends, and detected anomalies.
    *   **Features**: Interactive Plotly charts, dynamic watchlists, and auto-refresh capabilities.

## Prerequisites
Ensure you have the following installed:
*   **Docker & Docker Compose**: For running Kafka and Zookeeper.
*   **Python 3.8+**: For running the scripts.
*   **Java 11 (OpenJDK)**: Required for PySpark.

## Installation

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd Data_Eng_Project
    ```

2.  **Set Up Virtual Environment**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: Ensure `pyspark`, `kafka-python`, `streamlit`, `plotly`, `pandas` are installed)*

## Usage Guide

### 1. Start Infrastructure
Launch the Kafka and Zookeeper containers:
```bash
docker-compose up -d
```

### 2. Start the Data Producer
Stream stock data to the Kafka topic:
```bash
python producer.py
```
*   *Alternative*: Use `python dummy_producer.py` for synthetic random data.

### 3. Start the Analytics Engine
Run the Spark consumer to process the stream:
```bash
python consumer.py
```
*   This script writes processed data to `./outputs/streaming_data` and anomalies to `./outputs/anomalies`.

### 4. Launch the Dashboard
Open the real-time visualization interface:
```bash
streamlit run dashboard.py
```
*   Access the dashboard at `http://localhost:8501`.

## Project Structure

| File | Description |
|------|-------------|
| `producer.py` | Reads stock data from CSV and streams it to Kafka, simulating real-time events. |
| `consumer.py` | PySpark application that consumes Kafka streams, calculates moving averages, and detects anomalies. |
| `dashboard.py` | Streamlit application for visualizing real-time data, trends, and alerts. |
| `dummy_producer.py` | Generates random stock data for testing purposes. |
| `docker-compose.yml` | Configuration for deploying Kafka and Zookeeper services. |
| `data/` | Contains the source dataset (`stock_data.csv`). |
| `outputs/` | Directory where processed data and anomalies are stored. |

## Configuration
*   **Kafka Port**: Default is `9093` (configured in `docker-compose.yml` and scripts).
*   **Topic Name**: `test_topic`.
*   **Dashboard Refresh**: Default is 2 seconds (toggleable in UI).
*   **Topic Name**: `test_topic`.

## Scaling & Cloud Deployment

This project is designed to be cloud-agnostic. Here is how to scale it for production:

### 1. Scaling Kafka
*   **Increase Throughput**: Add more brokers to the Kafka cluster in `docker-compose.yml`.
*   **Parallel Processing**: Increase the number of partitions for `test_topic`. This allows multiple Spark consumers to read from the same topic simultaneously.

### 2. Scaling Spark
*   **Distributed Processing**: Submit the `consumer.py` job to a cluster manager like **YARN** or **Kubernetes**.
    *   Command: `spark-submit --master yarn --deploy-mode cluster consumer.py`
*   **Resource Allocation**: Tune `spark.executor.cores` and `spark.executor.memory` to handle larger workloads.

### 3. Cloud Storage (S3/HDFS)
*   To switch from local storage to cloud storage, update the `OUTPUT_DIR` in `consumer.py`:
    ```python
    # AWS S3 Example
    OUTPUT_DIR = "s3a://my-bucket/stock-data"
    ```

---
*Built by The Data Alchemists Engineering Team*
