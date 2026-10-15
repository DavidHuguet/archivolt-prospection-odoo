FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "mistralai-workflows>=3.10" "mistralai>=1.9"

COPY src ./src
COPY worker.py .

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

CMD ["python", "worker.py"]
