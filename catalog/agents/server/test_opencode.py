"""OpenCode's program tools: a plugin of weft's own (`opencode_weft.mjs`).

The turns, settings and logins are in `test_acp_agents.py`, played from
`testing/recorded/opencode.json`. These are what a recording cannot show:
where the plugin's loader goes and how OpenCode is pointed at it, beside the
permission rules in the one variable both live in; the plugin itself on
Node, against a stand-in of OpenCode's tool editor (each tool plain-named,
on the model's own list, its calls forwarded to the route); and which calls
count as the program's.

Run with the others: `python3 -m unittest test_opencode`.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP  # noqa: E402
import acp_opencode  # noqa: E402
import adapters  # noqa: E402
import js_plugin_rig  # noqa: E402


class OpenCodeProgramTools(js_plugin_rig.AdapterCase):
    kind = "opencode"

    def config(self, adapter) -> dict:
        return json.loads(adapter.setup.env[acp_opencode.CONFIG])

    def test_the_plugin_folder_joins_the_permission_rules_and_no_mcp_server(self):
        for permission in adapters.AGENTS["opencode"]["permissions"]:
            with self.subTest(permission=permission):
                adapter = self.adapter(LOOKUP, permission=permission)
                config = self.config(adapter)
                rules = json.loads(adapters.KINDS["opencode"].spec.permissions[permission].env[acp_opencode.CONFIG])
                self.assertEqual(config["permissions"], rules["permissions"], "the permission rules stay as they are")
                folder = os.path.join(self.root, acp_opencode.PLUGIN_FOLDER)
                self.assertEqual(config["plugins"], ["file://" + folder])
                with open(os.path.join(folder, "index.mjs")) as f:
                    self.assertEqual(js_plugin_rig.made(f.read()), {"url": "http://127.0.0.1:4100/mcp", "tools": LOOKUP})
                self.assertNotIn("mcpServers", adapter.setup.session)

    def test_with_a_connection_the_folder_is_in_its_private_home(self):
        adapter = self.adapter(LOOKUP, credentials={"way": "anthropicKey", "values": {"key": "k"}})
        self.assertEqual(self.config(adapter)["plugins"], ["file://" + os.path.join(adapter.home, acp_opencode.PLUGIN_FOLDER)])

    def test_no_tools_no_plugin(self):
        self.assertNotIn("plugins", self.config(self.adapter([])))

    def test_a_call_is_the_programs_by_the_name_opencode_gives_it(self):
        adapter = self.adapter([js_plugin_rig.tool("read"), js_plugin_rig.tool("lookup")])
        own = self.history_of(adapter, {"name": "read", "title": "read"}, {"name": "lookup", "title": "lookup"},
                              {"name": "shell", "title": "lookup"})
        self.assertEqual(own, ["lookup"], "the program's `read` took OpenCode's place; the third call is OpenCode's `shell`")


# OpenCode's tool editor, as a plugin's `ctx.tool.transform` hands it: the
# tools by name, a later `add` of a name taking the place of the earlier one.
HARNESS = r"""
const plugin = (await import(process.argv[2])).default;
const tools = new Map([["read", { name: "read", builtin: true }]]);
await plugin.setup({ tool: { transform: async (edit) => edit({ add: (tool) => tools.set(tool.name, tool) }) } });
const ran = {};
for (const [name, input] of Object.entries(JSON.parse(process.argv[3]))) {
  try { ran[name] = { result: await tools.get(name).execute(input) }; }
  catch (error) { ran[name] = { error: error.message }; }
}
const added = Object.fromEntries([...tools].map(([name, tool]) => [name, { builtin: tool.builtin === true, options: tool.options ?? null, input: tool.input ?? null }]));
console.log(JSON.stringify({ id: plugin.id, added, ran }));
"""


@js_plugin_rig.needs_node
class OpenCodePlugin(js_plugin_rig.AdapterCase):
    kind = "opencode"

    def test_each_tool_is_direct_under_its_plain_name_and_goes_to_the_route(self):
        url, calls = js_plugin_rig.route(self)
        adapters.SERVER_URL = url.removesuffix("/mcp")
        tools = [js_plugin_rig.tool("read"), js_plugin_rig.tool("failing", required=())]
        self.adapter(tools)
        ran = js_plugin_rig.run_node(self, HARNESS, os.path.join(self.root, acp_opencode.PLUGIN_FOLDER, "index.mjs"),
                                     json.dumps({"read": {"q": "a"}, "failing": {"q": "b"}}))
        self.assertEqual(ran["id"], "weft")
        # No namespace and out of code mode: on the model's own tool list, by its own name.
        self.assertEqual(ran["added"], {"read": {"builtin": False, "options": {"codemode": False}, "input": tools[0]["parameters"]},
                                        "failing": {"builtin": False, "options": {"codemode": False}, "input": tools[1]["parameters"]}})
        self.assertEqual(ran["ran"], {"read": {"result": {"content": 'read {"q": "a"}'}}, "failing": {"error": 'failing {"q": "b"}'}})
        self.assertEqual([c["params"]["name"] for c in calls], ["read", "failing"])


if __name__ == "__main__":
    unittest.main()
