#!/bin/sh
set -e
# Independent read-only gateway process. It never opens the DB for writing.
exec gunicorn --workers 1 --threads 4 --bind 0.0.0.0:8001 \
    --access-logfile - --error-logfile - \
    "app.gateway:create_app()"
