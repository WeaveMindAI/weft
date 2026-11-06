"""Qwen Code's program tools: the discovery and call commands weft writes
(`acp_qwen.qwen_files`, `qwen_tools`), and those commands run as Qwen runs them.

The turns, settings and logins are in `test_acp_agents.py`, played from
`testing/recorded/qwen.json`.

Run with the others: `python3 -m unittest test_qwen`.
"""

import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

import rig  # noqa: E402,F401 (the server's modules on the path, its logs quiet)
import acp_qwen  # noqa: E402
import adapter_acp  # noqa: E402
import adapters  # noqa: E402
from events import Refused  # noqa: E402
from route_stub import RouteStub  # noqa: E402

TOOLS = [{"name": "read_file", "description": "The program's reader", "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}},
         {"name": "lookup", "description": "Look up the magic number", "parameters": {"type": "object", "properties": {}}}]


class Commands(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)
        self.route = RouteStub("found it")
        self.addCleanup(self.route.close)
        adapters.SERVER_URL = self.route.url.removesuffix(adapters.MCP_PATH)
        self.system = os.path.join(self.home, "system.json")
        patch = unittest.mock.patch.dict(os.environ, {"QWEN_CODE_SYSTEM_SETTINGS_PATH": self.system})
        patch.start()
        self.addCleanup(patch.stop)

    def setup(self):
        """The files written before a start on the machine's own login (no
        private home), and the Setup pointing Qwen at them."""
        folder = os.path.join(self.home, "qwen-files")
        adapter = types.SimpleNamespace(conv=types.SimpleNamespace(home=self.home), tools=TOOLS, credentials=None,
                                        files_dir=lambda: folder)
        for path, content in acp_qwen.qwen_files(adapter).items():
            adapters.write_whole(os.path.join(folder, path), content, 0o700 if isinstance(content, adapter_acp.Script) else 0o600)
        return acp_qwen.qwen_tools(adapter)

    def settings(self, setup) -> dict:
        with open(setup.env["QWEN_CODE_SYSTEM_SETTINGS_PATH"], encoding="utf-8") as f:
            return json.load(f)

    def test_the_settings_name_both_commands_and_keep_the_machine_s_own(self):
        with open(self.system, "w", encoding="utf-8") as f:
            json.dump({"general": {"vimMode": True}, "tools": {"useRipgrep": False}}, f)
        setup = self.setup()
        self.assertEqual(setup.env["QWEN_CODE_LEGACY_MCP_BLOCKING"], "1", "without it Qwen never runs the discovery")
        settings = self.settings(setup)
        self.assertEqual(settings["general"], {"vimMode": True})
        self.assertFalse(settings["tools"]["useRipgrep"])
        self.assertTrue(os.access(settings["tools"]["callCommand"], os.X_OK))
        self.assertNotIn("disabled", settings["tools"], "a built-in turned off would take the program's tool of its name too")

    def test_no_system_settings_of_the_machine_is_no_settings_but_weft_s(self):
        self.assertEqual(list(self.settings(self.setup())), ["tools"])

    def test_system_settings_that_cannot_be_read_are_refused(self):
        with open(self.system, "w", encoding="utf-8") as f:
            f.write("{not json")
        with self.assertRaises(Refused) as refused:
            self.setup()
        self.assertIn("cannot be read", str(refused.exception))

    def test_the_discovery_lists_every_program_tool_under_its_name(self):
        settings = self.settings(self.setup())
        found = json.loads(subprocess.run(shlex.split(settings["tools"]["discoveryCommand"]), capture_output=True, text=True,
                                          check=True).stdout)
        self.assertEqual(found, [{"name": t["name"], "description": t["description"], "parameters": t["parameters"]} for t in TOOLS])

    def test_a_call_goes_to_the_route_and_its_answer_is_the_output(self):
        # As Qwen runs it: the command alone, the tool's name its one argument.
        call = self.settings(self.setup())["tools"]["callCommand"]
        done = subprocess.run([call, "read_file"], input=json.dumps({"path": "a.txt"}), capture_output=True, text=True)
        self.assertEqual((done.returncode, done.stdout, done.stderr), (0, "found it", ""))
        self.assertEqual(self.route.called(), [("read_file", {"path": "a.txt"})])

    def test_a_failed_call_is_its_text_on_stderr_and_a_nonzero_exit(self):
        self.route.is_error = True
        call = self.settings(self.setup())["tools"]["callCommand"]
        done = subprocess.run([call, "lookup"], input="{}", capture_output=True, text=True)
        self.assertEqual((done.returncode, done.stdout, done.stderr.strip()), (1, "", "found it"))

    def test_a_route_that_is_gone_fails_the_call_saying_so(self):
        call = self.settings(self.setup())["tools"]["callCommand"]
        self.route.close()
        done = subprocess.run([call, "lookup"], input="{}", capture_output=True, text=True)
        self.assertEqual(done.returncode, 1)
        self.assertIn("did not answer the call of 'lookup'", done.stderr)


class Recognition(unittest.TestCase):
    def test_a_call_is_named_by_its_tool_name_in_meta(self):
        read = adapters.KINDS["qwen"].spec.program_tool.read
        names = {"read_file", "lookup"}
        # A real `tool_call` of 0.26.6 for a discovered tool, and one of its own.
        self.assertEqual(read({"title": "read_file: {}", "_meta": {"toolName": "read_file", "provenance": "builtin"}}, names), "read_file")
        self.assertIsNone(read({"title": "glob: {}", "_meta": {"toolName": "glob"}}, names))
        self.assertIsNone(read({"title": "lookup (weft MCP Server): {}", "_meta": {"toolName": "mcp__weft__lookup"}}, names))


if __name__ == "__main__":
    unittest.main()
