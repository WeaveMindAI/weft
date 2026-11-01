"""The e2e rig's fetching container.

  - GET /health -> 200, the readiness probe target.
  - POST /fetch {"url"} -> downloads the url from inside the container and
    answers {"size": <bytes read>}, or 502 with what the download said. Only
    a link to the machine as a container reaches it (`host.docker.internal`,
    where a local install's container links point) is fetched; any other is
    refused with 400, so the server never fetches an address it is handed.

No dependencies (stdlib), so the image builds with no build step.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))
# The machine as a container reaches it, where a local install's container
# links point.
MACHINE = "host.docker.internal"


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
        sent = urllib.parse.urlsplit(json.loads(self.rfile.read(length))["url"])
        if sent.scheme != "http" or sent.hostname != MACHINE or sent.port is None:
            self._send(400, {"error": f"only a link to http://{MACHINE}:<port> is fetched here, not {sent.geturl()}"})
            return
        url = urllib.parse.urlunsplit(("http", f"{MACHINE}:{sent.port}", sent.path, sent.query, ""))
        try:
            with urllib.request.urlopen(url, timeout=30) as answer:
                self._send(200, {"size": len(answer.read())})
        except (urllib.error.URLError, OSError) as e:
            self._send(502, {"error": f"downloading {url} from inside the container failed: {e}"})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
