FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data

WORKDIR /srv/app

# Install dependencies first to leverage layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY app ./app
COPY wsgi.py ./

# Persistent SQLite databases live in a mounted volume.
RUN mkdir -p /data && chown -R 10001:10001 /data /srv/app
USER 10001

EXPOSE 8000

# One worker (the migrator is an in-process background thread) with several
# threads so long-running migration steps never block web requests.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", \
     "--threads", "8", "--timeout", "120", "wsgi:app"]
