# Use Python 3.8 as base image
FROM python:3.8-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

# Install OpenJDK and other necessary dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
		procps \
        wget \
        openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# download postgres jdbc jar
RUN mkdir -p /opt/postgres/ && cd /opt/postgres && wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Create and set working directory
WORKDIR /app

# Copy requirements file (if you have one)
#COPY requirements.txt .

# Install Python dependencies
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install pyspark

# Copy your Python scripts
COPY script.py .

# Command to run when container starts
CMD ["python3", "script.py"]

