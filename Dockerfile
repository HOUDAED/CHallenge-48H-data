FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl gcc gfortran libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY config/ ./config/

RUN chmod +x scripts/*.sh

RUN mkdir -p data/raw data/processed/outbox

CMD ["python", "scripts/hourly_pipeline_worker.py"]
