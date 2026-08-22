"""The database's own credential: mint it, then hand it over once.

Two jobs, one script, chosen by argv:

  mint   Run as an init container, before Postgres starts. Writes a
         fresh password to the shared volume the FIRST time only. On
         every later boot the file is already there and nothing
         changes, which is what keeps the database answering to the
         password it was created with.

  serve  Run beside Postgres. Answers the password to whoever asks
         over the cluster-internal network, then stops answering once
         the asker proves it stored it, by sending the password back.
         Proof, not a bare say-so: otherwise anything able to reach
         this port could retire a password nobody holds, and the
         database would be locked away with no way back.

         That same proof doubles as the answer to "is what I hold
         still this database's password", which is how a caller finds
         out its disk was replaced underneath it.

Where the files live and which port to serve on come from the node
that declares this container, so the two sides cannot drift.
"""

import http.server
import json
import os
import secrets
import sys

SECRET_DIR = os.environ.get("WEFT_SECRET_DIR")
PASSWORD_FILE = os.environ.get("WEFT_PASSWORD_FILE")
PORT = os.environ.get("WEFT_CREDENTIAL_PORT")
if not SECRET_DIR or not PASSWORD_FILE or not PORT:
    sys.exit(
        "WEFT_SECRET_DIR, WEFT_PASSWORD_FILE and WEFT_CREDENTIAL_PORT must be set by the node"
    )
PORT = int(PORT)
SEALED_FILE = os.path.join(SECRET_DIR, "sealed")

# SYNC: credential routes <-> catalog/postgres/database/mod.rs
#       CREDENTIAL_PATH / CREDENTIAL_STORED_PATH / HEALTH_PATH
CREDENTIAL_PATH = "/credential"
CREDENTIAL_STORED_PATH = "/credential/stored"
HEALTH_PATH = "/health"


def mint() -> None:
    # Made on EVERY boot, before any other container starts, because
    # the container that serves the password is given this directory
    # alone rather than the whole disk. A directory that already
    # exists is handed over as it is; one the kubelet has to create
    # can land owned by root, locking out the container that must
    # write in it.
    os.makedirs(SECRET_DIR, mode=0o750, exist_ok=True)
    if os.path.exists(PASSWORD_FILE):
        return
    # url-safe: the value travels in a connection string.
    password = secrets.token_urlsafe(32)
    # Written under a temporary name and renamed once complete AND on
    # disk, so neither a crash mid-write nor a node reboot can leave a
    # half password, or lose one Postgres has already adopted.
    tmp = PASSWORD_FILE + ".partial"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(password)
        f.flush()
        os.fsync(f.fileno())
    # Group-readable, because Postgres runs as a different user in the
    # same Pod and reads this file to set its own password. The group
    # is the Pod's fsGroup, which only its own containers are in.
    os.chmod(tmp, 0o640)
    os.replace(tmp, PASSWORD_FILE)
    dir_fd = os.open(SECRET_DIR, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def sealed() -> bool:
    return os.path.exists(SEALED_FILE)


def stored_password() -> str | None:
    try:
        with open(PASSWORD_FILE, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


class Handler(http.server.BaseHTTPRequestHandler):
    # A client that opens a connection and says nothing gives up its
    # thread after this, so a handful of silent clients cannot pile up
    # into every thread being held and the readiness probe failing.
    timeout = 5
    protocol_version = "HTTP/1.0"

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 (the stdlib's spelling)
        if self.path == HEALTH_PATH:
            self._send(200, {"ok": True})
            return
        if self.path != CREDENTIAL_PATH:
            self._send(404, {"error": "no such path"})
            return
        password = stored_password()
        if password is None:
            self._send(503, {"error": "no password on the shared volume yet"})
            return
        # "Already handed over" is an ANSWER, not a failure: the node
        # that asks needs to act on it (find its own connection, or
        # tell the user how to recover), and an HTTP error would reach
        # it as an opaque transport failure instead.
        self._send(200, {"sealed": True} if sealed() else {"password": password})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != CREDENTIAL_STORED_PATH:
            self._send(404, {"error": "no such path"})
            return
        password = stored_password()
        if password is None:
            self._send(503, {"error": "no password on the shared volume yet"})
            return
        try:
            # Clamped at BOTH ends, because the length is the caller's
            # claim: above, because a password is never near this size;
            # below, because a negative length means "read to the end"
            # and a client that trickles forever would take the whole
            # container's memory with it.
            length = max(0, min(int(self.headers.get("Content-Length", "0")), 4096))
            sent = json.loads(self.rfile.read(length) or b"{}").get("password", "")
        except (ValueError, json.JSONDecodeError):
            sent = ""
        # Only a caller that actually holds the password may retire it.
        # Compared as BYTES: comparing str refuses non-ASCII outright,
        # which would escape as a crash rather than a refusal.
        #
        # A mismatch is an ANSWER, not a failure. It is how the caller
        # learns that the password it holds is not this database's,
        # which happens when the disk was replaced under it: a fresh
        # disk means a fresh password, and the caller has to notice
        # rather than go on handing out one nothing accepts.
        if not secrets.compare_digest(str(sent).encode("utf-8"), password.encode("utf-8")):
            self._send(200, {"stored": False})
            return
        # Idempotent: confirming twice is the same as confirming once.
        with open(SEALED_FILE, "w", encoding="utf-8") as f:
            f.write("stored")
        self._send(200, {"stored": True})

    def log_message(self, fmt: str, *args: object) -> None:
        # One line per request on stderr, never the response body.
        sys.stderr.write("credential: %s\n" % (fmt % args))


def serve() -> None:
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("mint", "serve"):
        sys.exit("usage: bootstrap.py mint|serve")
    mint() if sys.argv[1] == "mint" else serve()
