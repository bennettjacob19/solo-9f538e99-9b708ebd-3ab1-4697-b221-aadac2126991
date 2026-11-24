FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DRILL_DB=/data/pool_drill.db

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app
COPY docker/entrypoint-web.sh /usr/local/bin/entrypoint-web.sh
COPY docker/entrypoint-gateway.sh /usr/local/bin/entrypoint-gateway.sh
RUN chmod +x /usr/local/bin/entrypoint-web.sh /usr/local/bin/entrypoint-gateway.sh \
    && mkdir -p /data

EXPOSE 8000 8001
CMD ["/usr/local/bin/entrypoint-web.sh"]
