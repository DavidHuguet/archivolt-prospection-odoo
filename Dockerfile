FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "mistralai-workflows>=3.10"

COPY src ./src
COPY worker.py .

ENV PYTHONPATH=/app/src

CMD ["python", "worker.py"]
