FROM python:3.12-slim

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