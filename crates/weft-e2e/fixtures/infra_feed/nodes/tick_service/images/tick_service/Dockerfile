# The tick stream: Python's standard library only, no build step.
FROM python:3.12-alpine

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 8080
USER 1000
CMD ["python", "/app/server.py"]
