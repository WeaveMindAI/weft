"""The e2e rig's fetching container.

  - GET /health -> 200, the readiness probe target.
  - POST /fetch {"url"} -> downloads the url from inside the container and
    answers {"size": <bytes read>}, or 502 with what the download said.

No dependencies (stdlib), so the image builds with no build step.
"""

import json
import os
import urllib.error
import urllib.request
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
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/fetch":
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        url = json.loads(self.rfile.read(length))["url"]
        try:
            with urllib.request.urlopen(url, timeout=30) as answer:
                self._send(200, {"size": len(answer.read())})
        except (urllib.error.URLError, OSError) as e:
            self._send(502, {"error": f"downloading {url} from inside the container failed: {e}"})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
