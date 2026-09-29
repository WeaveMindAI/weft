# The GPU probe's container. Nothing CUDA in the image: the NVIDIA runtime
# mounts the driver and `nvidia-smi` in when the container is given a GPU.
# A glibc base, because what it mounts is built for glibc (on alpine the
# binary is there and does not run).
FROM python:3.12-slim

WORKDIR /app
COPY server.py /app/server.py

EXPOSE 8080
USER 1000
CMD ["python", "/app/server.py"]
