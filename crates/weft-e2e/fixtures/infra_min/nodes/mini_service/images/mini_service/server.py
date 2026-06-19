"""Minimal infra sidecar for the e2e rig.

Serves the two routes the platform needs:
  - GET /health  -> 200, the readiness probe target.
  - GET /outputs -> a flat JSON object whose keys become the node's output ports
                    (here: `status`). The key set matches metadata.json outputs.

No dependencies (stdlib http.server), so the image builds with no build step.
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))


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
        elif self.path == "/outputs":
            # Keys here become the node's output ports (see metadata.json).
            self._send(200, {"status": "ready"})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *args):
        # Quiet: the rig reads behavior through the dispatcher, not pod logs.
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
