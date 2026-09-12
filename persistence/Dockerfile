FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY persistence/requirements.txt /app/persistence/requirements.txt
RUN pip install --no-cache-dir -r /app/persistence/requirements.txt

COPY shared /app/shared
COPY persistence /app/persistence

CMD ["python", "-m", "persistence.worker"]

