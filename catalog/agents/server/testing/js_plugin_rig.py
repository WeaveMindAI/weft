"""What the tests of weft's JavaScript plugins share (`js_plugin.py`: Pi's
extension, Kilo Code's, OpenCode's, OpenClaw's and Cline's plugins): a
stand-in of the agent server's MCP route, a run of one plugin on Node
through the loader the adapter writes, and an ACP adapter built with the
program's tools without starting it.
"""

import http.server
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest

import adapters

NODE = shutil.which("node")
needs_node = unittest.skipUnless(NODE, "needs Node 22.6 or newer on PATH, which runs the plugin")


class Route(http.server.BaseHTTPRequestHandler):
    """Answers each `tools/call` with the tool's name and arguments as its
    text, and as an error for a tool named `failing`; keeps every call."""

    calls = None

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.calls.append(body)
        params = body["params"]
        result = {"content": [{"type": "text", "text": f"{params['name']} {json.dumps(params['arguments'])}"}],
                  "isError": params["name"] == "failing"}
        data = json.dumps({"jsonrpc": "2.0", "id": body["id"], "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def route(test: unittest.TestCase):
    """Start a route for `test`, stopped with it: (its URL, the calls it got)."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Route)
    server.calls = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    test.addCleanup(server.server_close)
    test.addCleanup(server.shutdown)
    return f"http://127.0.0.1:{server.server_address[1]}/mcp", server.calls


def tool(name: str, description: str = None, required=("q",)) -> dict:
    """A program tool taking a text `q`."""
    return {"name": name, "description": description or f"{name} of weft",
            "parameters": {"type": "object", "properties": {"q": {"type": "string"}}, "required": list(required)}}


def run_node(test: unittest.TestCase, script: str, *args) -> dict:
    """Run `script` (an ES module) on Node with `args`; what it printed, as JSON."""
    folder = tempfile.mkdtemp()
    test.addCleanup(shutil.rmtree, folder, True)
    path = os.path.join(folder, "harness.mjs")
    with open(path, "w", encoding="utf-8") as f:
        f.write(script)
    done = subprocess.run([NODE, "--experimental-strip-types", "--no-warnings", path, *args],
                          capture_output=True, text=True, timeout=60)
    test.assertEqual(done.returncode, 0, done.stderr)
    return json.loads(done.stdout.strip().splitlines()[-1])


def made(loader_text: str) -> dict:
    """What a loader hands its plugin's `make`: the route and the tools."""
    return json.loads(loader_text[loader_text.index("make(") + len("make("):loader_text.rindex(");")])


class Conversation:
    """What an adapter reads of a conversation while it is built, and the
    history it writes."""

    def __init__(self, home):
        self.home = home
        self.history = []

    def remember(self, turn, message):
        self.history.append(message)

    def emit(self, turn, kind, **fields):
        return None


class AdapterCase(unittest.TestCase):
    """An ACP adapter of `kind` built with the program's tools, without starting it."""

    kind = None

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.conv = Conversation(self.root)
        adapters.SERVER_URL = "http://127.0.0.1:4100"

    def adapter(self, tools, permission="ask", credentials=None):
        config = {"agent": self.kind, "folder": self.root, "permission": permission, "options": {}, "credentials": credentials}
        return adapters.KINDS[self.kind](self.conv, config, tools)

    def history_of(self, adapter, *updates) -> list:
        """The calls the adapter itself writes into the history for these tool calls."""
        for call_id, update in enumerate(updates):
            adapter._update(object(), {"sessionUpdate": "tool_call", "toolCallId": str(call_id), "rawInput": {}, **update})
        return [c["name"] for m in self.conv.history for c in m.get("tool_calls") or []]
