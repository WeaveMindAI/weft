# Minimal infra sidecar for the e2e rig: a tiny HTTP server serving /health and
# /outputs. python:3-alpine has a stdlib http server, so no build step and no
# dependencies. Runs as a non-root user to satisfy a restricted pod baseline.
FROM python:3.12-alpine

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 8080
USER 1000
CMD ["python", "/app/server.py"]
