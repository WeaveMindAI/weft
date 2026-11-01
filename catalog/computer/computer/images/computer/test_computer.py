"""The computer container's own parts: what each action asks of the X tools
and how their answer comes back, the shell's sessions, the card, and the
`/action` envelope. The X tools are faked (no screen here); the shell is the
real bash, as in the container. Run with the standard library alone
(`python3 test_computer.py`), so it needs no install."""

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.request
from unittest import mock

# The settings the node and the image hand the container, so the module
# imports here as it does there. The home folder is a scratch one.
HOME = tempfile.mkdtemp()
os.environ.update({"WEFT_COMPUTER_PORT": "8100", "WEFT_COMPUTER_SCREEN": "1280x800", "HOME": HOME})
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Importing makes the shell's output folder; here it goes in a scratch one.
with mock.patch("os.makedirs"):
    import computer  # noqa: E402
computer.log = lambda message: None
computer.OUTPUT_DIR = tempfile.mkdtemp()

PNG = b"\x89PNG\r\n\x1a\nrest"
JPEG = b"\xff\xd8\xffrest"


class FakeX:
    """Stands in for `subprocess.run` on the X tools: keeps every command and
    answers each the way the real tool does, from `answers` by tool name."""

    def __init__(self, answers):
        self.answers = answers
        self.ran = []

    def __call__(self, args, env=None, capture_output=False, **_):
        self.ran.append((args, env.get("DISPLAY") if env else None))
        code, out, err = self.answers[args[0]]
        return subprocess.CompletedProcess(args, code, out, err)


class XTest(unittest.TestCase):
    def fake(self, answers):
        x = FakeX(answers)
        patch = mock.patch.object(computer.subprocess, "run", x)
        patch.start()
        self.addCleanup(patch.stop)
        return x

    def test_a_screenshot_is_the_png_of_the_whole_screen_with_its_size(self):
        x = self.fake({"import": (0, PNG, b"")})
        shot = computer.ACTIONS["screenshot"]({})
        self.assertEqual(base64.b64decode(shot["png"]), PNG)
        self.assertEqual((shot["width"], shot["height"]), (1280, 800))
        self.assertEqual(x.ran, [(["import", "-window", "root", "-depth", "8", "png:-"], ":1")])

    def test_a_screenshot_that_could_not_be_taken_says_why(self):
        self.fake({"import": (1, b"", b"unable to open X server")})
        with self.assertRaises(computer.Refused) as refused:
            computer.ACTIONS["screenshot"]({})
        self.assertEqual(str(refused.exception), "taking the screenshot failed: unable to open X server")

    def test_typing_hands_the_text_to_xdotool_whole(self):
        x = self.fake({"xdotool": (0, b"", b"")})
        self.assertEqual(computer.ACTIONS["type"]({"text": "echo typed > said\n"}), {})
        self.assertEqual(x.ran, [(["xdotool", "type", "--delay", "12", "--", "echo typed > said\n"], ":1")])

    def test_a_click_moves_there_first_and_holds_its_modifiers_around_it(self):
        x = self.fake({"xdotool": (0, b"", b"")})
        computer.ACTIONS["click"]({"x": 300, "y": 200, "modifiers": "ctrl+shift"})
        self.assertEqual(
            x.ran[0][0],
            ["xdotool", "keydown", "ctrl", "keydown", "shift", "mousemove", "300", "200",
             "click", "--repeat", "1", "--delay", "80", "1", "keyup", "shift", "keyup", "ctrl"],
        )

    def test_a_key_xdotool_skips_with_exit_code_0_is_refused_in_its_words(self):
        # xdotool answers 0 to a key name it does not know, after skipping it.
        self.fake({"xdotool": (0, b"", b"(symbol) No such key name 'NOT_A_KEY'. Ignoring it.\n")})
        with self.assertRaises(computer.Refused) as refused:
            computer.ACTIONS["key"]({"keys": "NOT_A_KEY"})
        self.assertIn("No such key name", str(refused.exception))


class ShellTest(unittest.TestCase):
    def setUp(self):
        computer.SHELL.end_all("a new test")

    def run_command(self, command, session=""):
        answer = computer.ACTIONS["exec"]({"command": command, "session": session})
        self.assertEqual(answer["status"], computer.DONE, answer)
        piece = computer.ACTIONS["output"]({"id": answer["id"], "from": 0})
        return answer["exitCode"], base64.b64decode(piece["data"]).decode()

    def test_a_command_answers_its_output_and_exit_code(self):
        self.assertEqual(self.run_command("echo from the shell"), (0, "from the shell\n"))
        self.assertEqual(self.run_command("echo no >&2; exit 3"), (3, "no\n"))

    def test_a_session_keeps_its_folder_and_exports_and_another_does_not_see_them(self):
        os.makedirs(os.path.join(HOME, "work"), exist_ok=True)
        self.run_command("cd work && export GREETING=hi && echo typed > said")
        self.assertEqual(self.run_command("pwd; echo $GREETING; cat said"), (0, f"{HOME}/work\nhi\ntyped\n"))
        self.assertEqual(self.run_command("pwd; echo ${GREETING:-none}", session="other"), (0, f"{HOME}\nnone\n"))

    def test_the_card_shows_the_screen_and_each_session_with_its_buttons(self):
        self.run_command("true")
        with mock.patch.object(computer.subprocess, "run", FakeX({"import": (0, JPEG, b"")})):
            items = {item["label"]: item for item in computer.live_items()}
        self.assertEqual(items["Screen"]["type"], "image")
        self.assertEqual(items["Screen"]["data"], "data:image/jpeg;base64," + base64.b64encode(JPEG).decode())
        self.assertEqual(items["Shell"]["data"], f"the default session: idle in {HOME}")
        self.assertEqual(items["Shell"]["action"]["actionKind"], "restart_shell")
        self.assertEqual(items["Screen size"]["data"], "1280x800")
        self.assertNotIn("SSH", items, "no keys, no SSH line")

    def test_restarting_the_shell_starts_every_session_over_in_the_home_folder(self):
        self.run_command("cd /")
        self.assertEqual(computer.press("restart_shell"), {"restarted": "shell"})
        self.assertEqual(computer.SHELL.states(), "no session yet")
        self.assertEqual(self.run_command("pwd"), (0, f"{HOME}\n"))


class EnvelopeTest(unittest.TestCase):
    """`POST /action` over HTTP: what the nodes and the card's buttons send."""

    def setUp(self):
        self.server = computer.http.server.ThreadingHTTPServer(("127.0.0.1", 0), computer.Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def post(self, body):
        url = f"http://127.0.0.1:{self.server.server_address[1]}{computer.ACTION_PATH}"
        request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
        with urllib.request.urlopen(request, timeout=30) as answer:
            return answer.status, json.loads(answer.read())

    def test_an_action_answers_its_result_and_a_refusal_is_an_error_in_the_result(self):
        x = FakeX({"xdotool": (0, b"", b""), "import": (0, PNG, b"")})
        with mock.patch.object(computer.subprocess, "run", x):
            self.assertEqual(self.post({"action": "move", "payload": {"x": 10, "y": 20}}), (200, {"result": {}}))
            status, answer = self.post({"action": "screenshot", "payload": {}})
            self.assertEqual((status, base64.b64decode(answer["result"]["png"])), (200, PNG))
            status, answer = self.post({"action": "move", "payload": {"x": 5000, "y": 20}})
            self.assertEqual(status, 200)
            self.assertEqual(answer, {"result": {"error": "(5000, 20) is off the 1280x800 screen"}})
            status, answer = self.post({"action": "fly", "payload": {}})
            self.assertTrue(answer["result"]["error"].startswith("no action 'fly'"), answer)


if __name__ == "__main__":
    unittest.main()
