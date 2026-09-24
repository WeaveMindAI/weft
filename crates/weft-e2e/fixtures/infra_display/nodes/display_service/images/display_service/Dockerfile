# Infra sidecar with a display for the e2e rig: serves /health, a /live
# panel with one button, and the /action that button presses. Stdlib only,
# run as a non-root user to satisfy a restricted pod baseline.
FROM python:3.12-alpine

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 8080
USER 1000
CMD ["python", "/app/server.py"]
