FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY simulator/requirements.txt /app/simulator/requirements.txt
RUN pip install --no-cache-dir -r /app/simulator/requirements.txt

COPY shared /app/shared
COPY simulator /app/simulator

ENTRYPOINT ["python", "-m", "simulator.generate_events"]

