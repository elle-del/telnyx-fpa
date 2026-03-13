# FP&A Dashboard - Python API + Static frontend
FROM python:3.11-alpine

# Install nginx and dependencies
RUN apk update && apk upgrade --no-cache && \
    apk add --no-cache nginx supervisor

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2-binary

# Copy static files
COPY *.html /usr/share/nginx/html/
COPY data/ /usr/share/nginx/html/data/

# Copy API server
COPY api_server.py /app/api_server.py

# Nginx config
COPY nginx.conf /etc/nginx/http.d/default.conf

# Supervisor config to run both nginx and API
RUN mkdir -p /etc/supervisor.d
COPY supervisord.conf /etc/supervisor.d/fpa.ini

# Health check
RUN echo "OK" > /usr/share/nginx/html/health

EXPOSE 8080

# Run supervisor (manages nginx + API server)
CMD ["supervisord", "-c", "/etc/supervisord.conf"]
