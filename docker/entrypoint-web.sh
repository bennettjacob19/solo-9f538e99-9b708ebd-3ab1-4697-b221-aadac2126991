#!/bin/sh
set -e
# Single writer process: one gunicorn worker, threaded; WAL allows the
# read-only gateway process to query concurrently.
exec gunicorn --workers 1 --threads 8 --bind 0.0.0.0:8000 \
    --access-logfile - --error-logfile - \
    "app.web:create_app()"
