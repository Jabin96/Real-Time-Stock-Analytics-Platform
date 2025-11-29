# Real-Time Stock Analytics Platform
### by The Data Alchemist

## Overview
**The Data Alchemist** team built a robust, real-time data engineering pipeline designed to ingest, process, and visualize stock market data with low latency. Leveraging industry-standard technologies like Apache Kafka and Apache Spark, it simulates a production-grade streaming architecture capable of handling high-velocity financial data.

## Architecture
The pipeline follows a modern decoupled streaming architecture:

1.  **Ingestion Layer (Apache Kafka)**:
    *   **Producer**: Simulates real-time stock ticks by streaming data from a dataset. Scalable to multiple instances using **modulo-based sharding**.
    *   **Broker**: Apache Kafka acts as the central message bus, ensuring durable and reliable data transport.

2.  **Processing Layer (Apache Spark)**:
    *   **Consumer**: A PySpark Structured Streaming application that consumes data from Kafka.
    *   **Analytics**: Performs real-time transformations and detects price anomalies using threshold-based logic.
        *   **Logic**: Calculates Z-Score ($Z = \frac{Price - Mean}{StdDev}$).
        *   **Threshold**: Flags anomaly if $|Z| > 3.0$ AND $StdDev > 0.05$.

3.  **Visualization Layer (Streamlit)**:
    *   **Dashboard**: A professional, real-time UI that displays metrics, price trends, and alerts.
    *   **Features**: Interactive Plotly charts (Dark Mode), dynamic watchlists, and auto-refresh capabilities.

## Scalability & Performance

### 1. Producer Scaling (Data Sharding) ✅ WORKING
The producer utilizes **Modulo-Based Data Sharding** to distribute ingestion load.

* **How to Scale**:
    ```bash
    # Scales to 3 instances. The script automatically calculates its index (1, 2, 3) 
    # and handles only its assigned subset of stocks.
    TOTAL_PRODUCERS=3 docker compose up -d --build --scale producer=3
    ```
* **Mechanism**:
    1.  The container detects its identity (e.g., `producer-1`, `producer-2`).
    2.  It filters the dataset: `my_stocks = all_stocks % total_producers`.
    3.  **Result**: 3 producers run in parallel with **Zero Overlap**, effectively tripling the ingestion throughput without data duplication.

### 2. Anomaly Injection Strategy
To guarantee demo alerts without manual intervention, the producer is deterministic:
* **Trigger**: Every **1,000 records** (~3.5 minutes), the producer artificially spikes the price by **500%**.
* **Detection**: The Consumer's Z-Score model catches this instantly ($|Z| > 3.0$).

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

### Step 1: Start the Entire Stack
Launch the Kafka broker, Zookeeper, Producer, and Consumer services using Docker Compose.

```bash
docker compose up -d --build
```

*   **What happens**:
    *   **Infrastructure**: Kafka and Zookeeper start.
    *   **Producers**: The `producer` service starts streaming data.
    *   **Consumer**: The `consumer` service starts processing data automatically.
    *   **Scaling**: You can scale producers dynamically (e.g., `TOTAL_PRODUCERS=3 docker compose up -d --scale producer=3`).

### Step 2: Monitor the Analytics Engine
Since the consumer is running inside Docker, you can monitor its progress via logs:

```bash
docker logs -f consumer-driver
```

*   **Output**:
    *   Processed data is written to `./outputs/streaming_data/raw_ticks`.
    *   Detected anomalies are written to `./outputs/anomalies`.

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
*   `ANOMALY_EVERY_SEC`: Frequency of fake anomalies (default: 180s / 3 mins).
*   `ENABLE_FAKE_ANOMALIES`: Toggle to enable/disable anomaly injection.

### Dashboard Configuration (`dashboard.py`)
*   **Refresh Rate**: The dashboard auto-refreshes every 1 second (when enabled).
*   **Time Window**: Adjustable via the sidebar (default: 3 minutes).

## Troubleshooting

*   **Kafka Connection Issues**: Ensure Docker containers are running (`docker ps`). If running locally on Mac, ensure `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` points to `127.0.0.1`.
*   **Java Errors**: PySpark requires Java 8 or 11. Ensure `JAVA_HOME` is correctly set.
*   **Missing Data**: Check if the `producer` containers are running and if `consumer.py` is successfully writing to the `outputs/` directory.
*   **Consumer Crashing**: If you see `java.lang.UnsupportedOperationException`, check your Java version. Java 11 is required.
*   **High CPU Usage**: Spark can be CPU intensive during the initial catch-up phase. This is normal.

---
*Built by The Data Alchemist Engineering Team*
