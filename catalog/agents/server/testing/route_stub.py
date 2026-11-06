"""A stand-in for the program's tool route (the agent server's MCP server),
for the tests of what an agent's plugin or command sends there: it records
every JSON-RPC message it is sent and answers a `tools/call` with the text
and failure the test sets.

Standard library alone.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class RouteStub:
    def __init__(self, text: str = "42", is_error: bool = False):
        self.text = text
        self.is_error = is_error
        self.calls = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                stub.calls.append(body)
                result = {"content": [{"type": "text", "text": stub.text}], "isError": stub.is_error}
                data = json.dumps({"jsonrpc": "2.0", "id": body.get("id"), "result": result}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.http.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.http.server_address[1]}/mcp"
        threading.Thread(target=self.http.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()

    def called(self) -> list:
        """(name, arguments) of every `tools/call` it got."""
        return [(c["params"]["name"], c["params"]["arguments"]) for c in self.calls if c.get("method") == "tools/call"]

    def close(self) -> None:
        self.http.shutdown()
        self.http.server_close()
