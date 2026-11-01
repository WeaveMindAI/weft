# The e2e rig's fetching container: a stdlib HTTP server that downloads the
# link it is posted and answers how many bytes it read. No dependencies, so
# no build step. Runs as a non-root user to satisfy a restricted container
# baseline.
FROM python:3.15-alpine

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 8080
USER 1000
CMD ["python", "/app/server.py"]
