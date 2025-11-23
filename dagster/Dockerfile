# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install Java (required for PySpark) and other system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk-headless \
    procps \
    curl \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create data directories if they don't exist
RUN mkdir -p data/raw data/transformed

# # Set Python path to include src directory
# ENV PYTHONPATH=/app:${PYTHONPATH}

# Expose Dagster webserver port
EXPOSE 3000

# Set environment variable for Dagster home
ENV DAGSTER_HOME=/app

# Default command to run Dagster webserver
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "src.definitions"]