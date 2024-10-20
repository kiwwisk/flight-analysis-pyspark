FROM python:3.8-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
		procps \
        wget \
        openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# download postgres jdbc jar
RUN mkdir -p /opt/postgres/ && cd /opt/postgres && wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
COPY script.py .
COPY *.json .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run when container starts
CMD ["python3", "script.py"]
