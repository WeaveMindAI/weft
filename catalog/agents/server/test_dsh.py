"""DeepSeek Harness's program tools: the patch weft hands it
(`acp_dsh.dsh_files`, `dsh_tools`) and weft's plugin (`dsh_weft.mjs`) run in Node as
dsh runs it, registering in each top-level agent's own scope (where a tool
shadows the built-in of its name and nothing else).

The turns, settings and logins are in `test_acp_agents.py`, played from
`testing/recorded/dsh.json`.

Run with the others: `python3 -m unittest test_dsh`.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

import rig  # noqa: E402,F401 (the server's modules on the path, its logs quiet)
import acp_dsh  # noqa: E402
import adapter_acp  # noqa: E402
import adapters  # noqa: E402
from route_stub import RouteStub  # noqa: E402

TOOLS = [{"name": "read", "description": "The program's reader", "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}},
         {"name": "web_search", "description": "The program's search", "parameters": {"type": "object", "properties": {}}},
         {"name": "lookup", "description": "Look up the magic number", "parameters": {"type": "object", "properties": {}}}]

# Runs the plugin as dsh does: `apply(ctx, config)` with the patch's config,
# against a stand-in of dsh's context: one top-level agent there at the
# start, a sub-agent and a second top-level agent made after (`agent/created`),
# each with a scope of its own (`agent.ctx.tools`); then calls each tool named
# in argv as the first agent sees it.
HARNESS = """
import { apply } from %s;
const config = JSON.parse(process.argv[2]);
const refuse = new Set(JSON.parse(process.argv[3]));
const scopes = new Map();
const agent = (id, parentAgent) => {
  const registered = new Map();
  scopes.set(id, registered);
  return { id, parentAgent, ctx: { tools: { register(tool) {
    if (refuse.has(tool.name)) throw new Error(`tool name "${tool.name}" is reserved`);
    registered.set(tool.name, tool);
    return () => registered.delete(tool.name);
  } } } };
};
const main = agent("main");
const listeners = {};
apply({ agents: { list: () => [main] }, on: (event, listener) => { listeners[event] = listener; }, effect() {} }, config);
listeners["agent/created"]({ agent: agent("sub", main) });
const second = agent("second");
listeners["agent/created"]({ agent: second });
listeners["agent/disposed"]({ agent: second });
const registered = scopes.get("main");
const out = { registered: [...registered.values()].map((t) => ({ name: t.name, parameters: t.parameters })), calls: [],
              sub: [...scopes.get("sub").keys()], disposed: [...scopes.get("second").keys()] };
for (const name of JSON.parse(process.argv[4])) {
  try {
    out.calls.push({ ok: await registered.get(name).execute({ path: "a.txt" }, { signal: new AbortController().signal }) });
  } catch (error) {
    out.calls.push({ error: error.message });
  }
}
console.log(JSON.stringify(out));
"""


class Patch(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)
        adapters.SERVER_URL = "http://127.0.0.1:4100"

    def test_the_patch_inserts_the_plugin_and_turns_no_row_off(self):
        adapter = types.SimpleNamespace(tools=TOOLS, files_dir=lambda: self.home)
        for path, content in acp_dsh.dsh_files(adapter).items():
            adapters.write_whole(os.path.join(self.home, path), content, 0o600)
        setup = acp_dsh.dsh_tools(adapter)
        self.assertEqual(setup.args[0], "--patch")
        with open(setup.args[1], encoding="utf-8") as f:
            patch = json.load(f)
        plugin = patch[0]["insert"][0]
        self.assertEqual((plugin["id"], plugin["name"]), ("weft-tools", acp_dsh.PLUGIN))
        self.assertEqual(plugin["config"]["url"], "http://127.0.0.1:4100/mcp")
        self.assertEqual([t["name"] for t in plugin["config"]["tools"]], ["read", "web_search", "lookup"])
        self.assertEqual(len(patch), 1, "a program tool named like a built-in shadows it alone: no row is turned off")


class Plugin(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.route = RouteStub("found it")
        self.addCleanup(self.route.close)
        self.harness = os.path.join(self.root, "harness.mjs")
        with open(self.harness, "w", encoding="utf-8") as f:
            f.write(HARNESS % json.dumps("file://" + acp_dsh.PLUGIN))

    def run_plugin(self, refuse=(), call=()):
        config = {"url": self.route.url, "tools": TOOLS}
        return subprocess.run(["node", self.harness, json.dumps(config), json.dumps(list(refuse)), json.dumps(list(call))],
                              capture_output=True, text=True, timeout=60)

    def test_every_tool_is_registered_under_its_name_with_the_program_s_schema(self):
        done = self.run_plugin(call=["read"])
        self.assertEqual(done.returncode, 0, done.stderr)
        out = json.loads(done.stdout)
        self.assertEqual(out["registered"], [{"name": t["name"], "parameters": t["parameters"]} for t in TOOLS])
        self.assertEqual(out["calls"], [{"ok": "found it"}])
        self.assertEqual(self.route.called(), [("read", {"path": "a.txt"})])
        self.assertEqual(out["sub"], [], "a sub-agent sees its parent's, filtered as dsh filters what it inherits")
        self.assertEqual(out["disposed"], [], "an agent that goes takes its registrations with it")

    def test_a_failed_call_is_the_tool_s_error(self):
        self.route.is_error = True
        out = json.loads(self.run_plugin(call=["lookup"]).stdout)
        self.assertEqual(out["calls"], [{"error": "found it"}])

    def test_a_name_dsh_refuses_ends_dsh_saying_why(self):
        # dsh only warns about a plugin whose `apply` throws, and goes on
        # without its tools; the plugin ends the process instead.
        done = self.run_plugin(refuse=["web_search"])
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("weft could not give DeepSeek Harness the program's tool web_search", done.stderr)


class Recognition(unittest.TestCase):
    def test_a_call_is_named_by_its_title(self):
        read = adapters.KINDS["dsh"].spec.program_tool.read
        self.assertEqual(read({"title": "read"}, {"read"}), "read")
        self.assertIsNone(read({"title": "bash"}, {"read"}))


if __name__ == "__main__":
    unittest.main()
