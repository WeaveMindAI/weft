# Mints the database's password on first boot and hands it over once.
# Python's standard library only: no dependencies to install, nothing
# to keep patched beyond the base image itself.
FROM python:3.12-alpine

COPY bootstrap.py /bootstrap.py

# Not root, and not Postgres's user either: this container only needs
# the shared group the Pod puts every container in.
USER 999:999
ENTRYPOINT ["python3", "/bootstrap.py"]
