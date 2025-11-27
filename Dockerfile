FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install kafka-python pandas

# Copy source code and data
COPY producer.py .
COPY data/ data/

# Run the producer
CMD ["python", "producer.py"]
