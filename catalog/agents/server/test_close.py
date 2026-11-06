"""The agent server stopping for good: on a SIGTERM (a stop of its process
unit, or the computer removing its app), it stops its agent first, keeps the
conversation on disk, and exits. Run with `python3 test_agent_server.py`,
which runs every `test_*.py` beside it.
"""

import json
import os
import signal
import subprocess
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import HERE, Rig  # noqa: E402
import events as ev  # noqa: E402


class CloseTest(Rig):
    def test_closing_stops_the_agent_keeps_the_conversation_and_refuses_a_new_turn(self):
        self.configure("claude")
        turn = self.turn("hello")
        self.server.until(turn, ev.DONE)
        proc = self.server.conversation.adapter.proc
        self.assertIsNone(proc.poll(), "the agent stays up between turns")
        self.server.conversation.close()
        self.assertIsNotNone(proc.poll(), "the agent was stopped")
        self.assertIsNone(self.server.conversation.adapter)
        with open(os.path.join(self.home, "state.json"), encoding="utf-8") as f:
            self.assertTrue(json.load(f)["history"], "the conversation is kept for the next start")
        status, answer = self.server.call("POST", "/turns", {"message": "again", "tools": []})
        self.assertEqual(status, 503)
        self.assertIn("the agent server is stopping", answer["error"])

    def test_a_sigterm_stops_the_agent_and_then_the_server(self):
        port_file = os.path.join(self.root, "port")
        server = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "agent_server.py"), "--port", "0", "--home", self.home, "--app-file", port_file],
            env=dict(os.environ), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.addCleanup(lambda: server.poll() is None and server.kill())
        deadline = time.monotonic() + 20
        while not (os.path.exists(port_file) and os.path.getsize(port_file)):
            self.assertLess(time.monotonic(), deadline, "the server never wrote its port down")
            time.sleep(0.05)
        # The rig's requests go to that server from here on.
        with open(port_file, encoding="utf-8") as f:
            self.server.url = f"http://127.0.0.1:{f.read().strip()}"
        self.configure("claude")
        turn = self.turn("hello")
        self.server.until(turn, ev.DONE)
        server.send_signal(signal.SIGTERM)
        _, said = server.communicate(timeout=20)
        said = said.decode()
        self.assertEqual(server.returncode, 0, f"a stop is no crash: {said}")
        self.assertIn("stopping: the agent first", said)
        self.assertIn("stopped", said)
        self.assertTrue(os.path.exists(os.path.join(self.home, "state.json")), "a stop keeps the conversation")


if __name__ == "__main__":
    unittest.main()
