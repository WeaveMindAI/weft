"""The GPU probe: /health for readiness, /outputs with the GPUs it sees."""

import json
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def gpus():
    try:
        return subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True, timeout=20).stdout.strip()
    except (OSError, subprocess.SubprocessError) as e:
        return f"no nvidia-smi: {e}"


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
            self._send(200, {"gpus": gpus()})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
