FROM python:3.12-slim

# Install tzdata for timezone support
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*

# Default timezone — can be overridden via TZ env var
ENV TZ=America/Mexico_City
RUN ln -sf /usr/share/zoneinfo/${TZ} /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Create data directory for SQLite
RUN mkdir -p /data

# Run the poller
CMD ["python", "src/poller.py"]