"""Infra sidecar with a display, for the e2e rig.

  - GET  /health -> 200, the readiness probe target.
  - GET  /live   -> the node's display: how many times its button was
                    pressed, and the button.
  - POST /action -> {"action": "press"} counts one press and answers
                    {"result": {"presses": n}}; any other action answers
                    {"result": {"error": ...}}, as a real node refuses a
                    button it does not have.
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))
presses = 0
lock = threading.Lock()


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
        elif self.path == "/live":
            with lock:
                count = presses
            self._send(200, {"items": [{
                "type": "text",
                "label": "Presses",
                "data": str(count),
                "action": {"label": "Press", "actionKind": "press"},
            }]})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        global presses
        if self.path != "/action":
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        if body.get("action") != "press":
            self._send(200, {"result": {"error": f"no action {body.get('action')!r}"}})
            return
        with lock:
            presses += 1
            count = presses
        self._send(200, {"result": {"presses": count}})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
