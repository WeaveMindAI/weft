"""The rig every test of the agent server runs on: one server in this
process, and the stand-in CLIs (`testing/`) on PATH under each agent's own
command name, so the adapters start them exactly as they start the agents.

Standard library alone.
"""

import functools
import glob
import json
import os
import shutil
import stat
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

TESTING = os.path.dirname(os.path.abspath(__file__))
HERE = os.path.dirname(TESTING)
sys.path.insert(0, HERE)

import adapters  # noqa: E402
import agent_server  # noqa: E402

agent_server.log = lambda message: None
adapters.log = lambda message: None

LOOKUP = [{"name": "lookup", "description": "Look up the magic number", "parameters": {"type": "object", "properties": {}}}]

# Every agent's recorded lines, by kind (`fake_acp_replay.py` plays them).
RECORDED = os.path.join(TESTING, "recorded")


@functools.cache
def recordings() -> dict:
    """Read once: every test puts a stand-in on PATH for each of them."""
    found = {}
    for path in sorted(glob.glob(os.path.join(RECORDED, "*.json"))):
        with open(path, encoding="utf-8") as f:
            found[os.path.splitext(os.path.basename(path))[0]] = dict(json.load(f), path=path)
    return found


def shim(directory: str, name: str, script: str, env: dict = None) -> None:
    path = os.path.join(directory, name)
    exports = "".join(f"{k}={v} " for k, v in (env or {}).items())
    with open(path, "w") as f:
        f.write(f"#!/bin/sh\nexec env {exports}{sys.executable} {os.path.join(TESTING, script)} \"$@\"\n")
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)


def stand_ins(directory: str) -> None:
    """Every stand-in on PATH, under the command its adapter starts."""
    for name, script in (("claude", "fake_claude.py"), ("codex", "fake_codex.py"), ("fakeacp", "fake_acp.py")):
        shim(directory, name, script)
    for recording in recordings().values():
        shim(directory, recording["command"], "fake_acp_replay.py", {"FAKE_RECORDING": recording["path"]})


class Server:
    """One agent server, in this process, on a port of its own."""

    def __init__(self, home: str):
        self.conversation = agent_server.Conversation(home)
        handler = type("H", (agent_server.Handler,), {"conversation": self.conversation})
        # The server's own class, so what it does with a failed request runs here too.
        self.http = agent_server.Server(("127.0.0.1", 0), handler)
        self.http.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.http.server_address[1]}"
        adapters.SERVER_URL = self.url
        # `shutdown` waits for the loop's next look at its flag, which is
        # every half second by default: in every test's cleanup.
        threading.Thread(target=self.http.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()

    def close(self):
        adapter = self.conversation.adapter
        if adapter is not None:
            adapter.stop()
        self.http.shutdown()
        self.http.server_close()

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.url + path, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as answer:
                return answer.status, json.loads(answer.read())
        except urllib.error.HTTPError as e:
            with e:
                return e.code, json.loads(e.read())

    def post(self, path, body):
        status, answer = self.call("POST", path, body)
        assert status == 200, (status, answer)
        return answer

    def until(self, turn: str, *wanted: str, after: int = 0) -> dict:
        """Read the turn's events until its status is one of `wanted`; every event read, and the last answer."""
        seen = []
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            status, answer = self.call("GET", f"/events?turn={turn}&after={after}&wait=5")
            assert status == 200, answer
            seen += answer["events"]
            if answer["events"]:
                after = answer["events"][-1]["n"]
            if answer["status"] in wanted and (answer["pending"] is None or answer["pending"]["n"] <= after):
                answer["seen"] = seen
                answer["after"] = after
                return answer
        raise AssertionError(f"turn {turn} never reached {wanted}; saw {seen}")


class Rig(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        bin_dir = os.path.join(self.root, "bin")
        os.makedirs(bin_dir)
        stand_ins(bin_dir)
        self.folder = os.path.join(self.root, "work")
        os.makedirs(self.folder)
        self.log = os.path.join(self.root, "fake.log")
        self.home = os.path.join(self.root, "home")
        self.env = {"PATH": bin_dir + os.pathsep + os.environ["PATH"], "FAKE_LOG": self.log}
        self.saved_env = dict(os.environ)
        os.environ.update(self.env)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(self.saved_env)))
        self.server = self.start()

    def start(self):
        server = Server(self.home)
        self.addCleanup(server.close)
        return server

    def configure(self, agent, **more):
        config = {"agent": agent, "folder": self.folder, "permission": "ask", "streamBy": "message", "options": {}, "credentials": None}
        config.update(more)
        return self.server.post("/configure", config)

    def turn(self, message, tools=None, **more):
        return self.server.post("/turns", {"message": message, "tools": tools or [], **more})["turn"]

    def logged(self):
        with open(self.log) as f:
            return [json.loads(line) for line in f]
