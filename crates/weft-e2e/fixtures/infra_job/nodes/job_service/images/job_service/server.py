"""Job service for the e2e rig.

  - GET  /health     -> 200, the readiness probe target.
  - POST /jobs       -> {"id": ...}: starts a job.
  - GET  /jobs/<id>  -> {"status": "running"} for the first two looks, then
                        {"status": "done", "result": "rendered <id>"}.

Counting looks rather than seconds keeps the test independent of the poll
interval: the run can only finish after the poll came back twice more.
"""

import json
import os
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock

PORT = int(os.environ.get("PORT", "8080"))
RUNNING_LOOKS = 2

jobs = {}
lock = Lock()


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok"})
            return
        if self.path.startswith("/jobs/"):
            job = self.path[len("/jobs/"):]
            with lock:
                if job not in jobs:
                    self._send(404, {"error": f"no job {job}"})
                    return
                jobs[job] += 1
                looks = jobs[job]
            if looks <= RUNNING_LOOKS:
                self._send(200, {"status": "running"})
            else:
                self._send(200, {"status": "done", "result": f"rendered {job}"})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        if self.path == "/jobs":
            job = uuid.uuid4().hex
            with lock:
                jobs[job] = 0
            self._send(200, {"id": job})
            return
        self._send(404, {"error": "not found"})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
