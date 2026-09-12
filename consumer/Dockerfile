FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY consumer/requirements.txt /app/consumer/requirements.txt
RUN pip install --no-cache-dir -r /app/consumer/requirements.txt

COPY shared /app/shared
COPY consumer /app/consumer

CMD ["python", "-m", "consumer.worker"]

