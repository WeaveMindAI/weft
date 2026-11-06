"""OpenClaw's adapter: the config weft writes for its Gateway, weft's plugin
on Node, and the adapter against a stand-in replaying the frames the
Gateway really sent (`testing/fake_openclaw.py`, `testing/fake_openclaw.json`):
a turn through the program's tool, a shell approval, a question of its own
`ask_user`, a file write, thinking, a failed turn, a restart that finds the
session again, a Gateway still starting or dying as it starts, and what
each setting and login does to it.

Run with the others: `python3 -m unittest test_openclaw`.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

import rig  # noqa: E402 (the server's modules on the path, its logs quiet)
import js_plugin_rig  # noqa: E402
import adapter_openclaw as oc  # noqa: E402
import adapters  # noqa: E402
import js_plugin  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402

LOGINS = {"anthropicKey": {"key": "sk-ant"}, "openaiKey": {"key": "sk"}, "geminiKey": {"key": "AIza"},
          "openRouter": {"key": "sk-or"}, "openaiCompatible": {"key": "sk-compatible-secret", "baseUrl": "http://127.0.0.1:9/v1"}}
KEY = {"way": "openaiCompatible", "values": LOGINS["openaiCompatible"]}


class Adapter(adapters.Adapter):
    """What `gateway_config` reads of an adapter, without a conversation."""

    kind = "openclaw"

    def __init__(self, permission="ask", way="anthropicKey", model="claude-sonnet-4-5", tools=()):
        self.facts = adapters.AGENTS["openclaw"]
        self.permission = permission
        self.credentials = {"way": way, "values": LOGINS[way]}
        self.config = {"folder": "/home/me/work", "model": model}
        self.home = "/state/openclaw-home"
        self.tools = list(tools)


def config_of(adapter, port=4200, token="t" * 43) -> dict:
    return json.loads(oc.gateway_config(adapter, port, token)[".openclaw/openclaw.json"])


class GatewayConfig(unittest.TestCase):
    def setUp(self):
        adapters.SERVER_URL = "http://127.0.0.1:4100"

    def every_config(self):
        for permission in adapters.AGENTS["openclaw"]["permissions"]:
            for way in LOGINS:
                yield permission, way, config_of(Adapter(permission, way))

    def test_nothing_runs_on_its_own(self):
        for permission, way, config in self.every_config():
            with self.subTest(permission=permission, way=way):
                self.assertEqual(config["agents"]["defaults"]["heartbeat"], {"every": "0m"})
                # The scheduler runs the heartbeat and every job anything schedules.
                self.assertIs(config["cron"]["enabled"], False)
                self.assertIs(config["plugins"]["entries"]["memory-core"]["config"]["dreaming"]["enabled"], False)
                self.assertIs(config["tools"]["exec"]["notifyOnExit"], False)
                self.assertIs(config["update"]["checkOnStart"], False)
                self.assertEqual(config["session"]["maintenance"]["mode"], "warn")
                for tool in ("group:automation", "intent"):
                    self.assertIn(tool, config["tools"]["deny"])

    def test_its_question_tool_stays_and_what_waits_on_a_surface_weft_has_not_goes(self):
        for permission, way, config in self.every_config():
            with self.subTest(permission=permission, way=way):
                self.assertNotIn("ask_user", config["tools"]["deny"], "its questions reach weft over the Gateway")
                for tool in ("secrets", "terminal", "portal", "dashboard"):
                    self.assertIn(tool, config["tools"]["deny"])

    def test_the_folder_is_where_it_works_and_nothing_is_written_into_it(self):
        config = config_of(Adapter())
        self.assertIs(config["agents"]["defaults"]["skipBootstrap"], True)
        agent = config["agents"]["entries"][oc.AGENT]
        self.assertEqual(agent["cwd"], "/home/me/work")
        self.assertEqual(agent["workspace"], "/state/openclaw-home/workspace")
        self.assertEqual(config["logging"]["file"], "/state/openclaw-home/openclaw.log")

    def test_the_gateway_is_on_loopback_on_the_port_and_token_of_the_start(self):
        config = config_of(Adapter(), port=4321, token="a-token")
        self.assertEqual(config["gateway"], {"mode": "local", "port": 4321, "bind": "loopback",
                                             "auth": {"mode": "token", "token": "a-token"}})

    def test_each_permission_setting(self):
        expected = {"ask": ("ask", True), "auto": ("auto", True), "read_only": ("deny", True), "bypass": ("full", False)}
        self.assertEqual(list(expected), adapters.AGENTS["openclaw"]["permissions"])
        for permission, (mode, contained) in expected.items():
            with self.subTest(permission=permission):
                config = config_of(Adapter(permission))
                self.assertEqual(config["tools"]["exec"]["mode"], mode)
                self.assertIs(config["tools"]["fs"]["workspaceOnly"], contained)
                taken = {"exec", "process", "write", "edit", "apply_patch"} <= set(config["tools"]["deny"])
                self.assertIs(taken, permission == "read_only")

    def test_the_model_is_named_by_the_logins_provider(self):
        self.assertEqual(sorted(oc.PROVIDERS), sorted(adapters.AGENTS["openclaw"]["logins"]))
        for way, provider in oc.PROVIDERS.items():
            with self.subTest(way=way):
                config = config_of(Adapter(way=way, model="m-1"))
                self.assertEqual(config["agents"]["entries"][oc.AGENT]["model"], f"{provider}/m-1")

    def test_an_openai_compatible_server_is_a_provider_whose_key_stays_in_the_environment(self):
        config = config_of(Adapter(way="openaiCompatible", model="local-model"))
        provider = config["models"]["providers"]["custom"]
        self.assertEqual(provider["baseUrl"], "http://127.0.0.1:9/v1")
        self.assertEqual(provider["apiKey"], "${OPENAI_API_KEY}")
        self.assertEqual(provider["models"][0]["id"], "local-model")
        self.assertNotIn("sk-compatible-secret", json.dumps(config))

    def test_wefts_plugin_loads_at_every_start_with_the_programs_tools(self):
        for tools in ([], rig.LOOKUP + [{"name": "search", "parameters": {"type": "object"}}]):
            with self.subTest(tools=[t["name"] for t in tools]):
                files = oc.gateway_config(Adapter(tools=tools), 4200, "t")
                config = json.loads(files[".openclaw/openclaw.json"])
                self.assertEqual(config["plugins"]["load"]["paths"], ["/state/openclaw-home/weft-plugin"])
                self.assertEqual(config["plugins"]["entries"]["weft"], {"enabled": True})
                self.assertNotIn("mcp", config, "no MCP server: its tools would be `weft__<tool>`")
                manifest = json.loads(files["weft-plugin/openclaw.plugin.json"])
                names = [t["name"] for t in tools]
                # OpenClaw finds a plugin's tools in its manifest, and offers one only
                # in the profiles it names: the agent runs with `coding`.
                self.assertEqual(manifest["contracts"]["tools"], names)
                self.assertEqual(manifest["toolMetadata"], {name: {"profiles": ["coding"]} for name in names})
                self.assertEqual(json.loads(files["weft-plugin/package.json"])["openclaw"]["extensions"], ["./index.js"])
                loader = files["weft-plugin/index.js"]
                self.assertIn(f"import {{ make }} from {json.dumps('file://' + js_plugin.plugin_file('openclaw_weft.mjs'))};", loader)
                made = js_plugin_rig.made(loader)
                self.assertEqual((made["url"], [t["name"] for t in made["tools"]]), ("http://127.0.0.1:4100/mcp", names))

    def test_no_model_is_refused(self):
        with self.assertRaises(Refused) as refused:
            config_of(Adapter(model=None))
        self.assertIn("needs `model`", str(refused.exception))


class Frames(unittest.TestCase):
    """How the Gateway's own shapes read, each from a recorded frame or its schema."""

    def sent(self, turn: str) -> list:
        """The payloads of the frames the Gateway sent in the recorded turn."""
        with open(os.path.join(rig.TESTING, "fake_openclaw.json"), encoding="utf-8") as f:
            steps = json.load(f)["turns"][turn]
        return [(s["send"]["event"], s["send"]["payload"]) for s in steps if "send" in s]

    def recorded(self, turn: str, event: str) -> dict:
        return next(payload for name, payload in self.sent(turn) if name == event)

    def test_an_approval_offers_the_decisions_the_gateway_names(self):
        approval = self.recorded("go", "exec.approval.requested")
        question = oc.permission_question(approval)
        self.assertEqual(question["id"], approval["id"])
        self.assertEqual([(c["id"], c["kind"]) for c in question["permission"]["choices"]],
                         [("allow-once", ev.ALLOW_ONCE), ("allow-always", ev.ALLOW_ALWAYS), ("deny", ev.REJECT)])
        self.assertTrue(question["permission"]["detail"].startswith("touch approved.txt\n(in "))
        approval["request"]["allowedDecisions"] = ["allow-once", "deny"]
        self.assertEqual([c["id"] for c in oc.permission_question(approval)["permission"]["choices"]], ["allow-once", "deny"])
        approval["request"]["allowedDecisions"] = ["allow-once", "ask-later"]
        with self.assertRaises(Refused):
            oc.permission_question(approval)

    def test_a_question_keeps_its_ids_and_takes_a_free_answer(self):
        asked = self.recorded("pick a color", "question.requested")
        question = oc.input_question(asked)
        self.assertEqual(question["id"], asked["id"])
        [q] = question["questions"]
        self.assertEqual((q["id"], q["header"], q["allowOther"], q["multiSelect"]), ("color_choice", "Color", True, False))
        self.assertEqual([o["label"] for o in q["options"]], ["Red (Recommended)", "Blue"])

    def test_a_patch_names_each_file_and_how_it_changed(self):
        details = next(payload["data"]["result"]["details"] for _, payload in self.sent("write a file")
                       if payload.get("stream") == "tool" and payload["data"].get("phase") == "result")
        self.assertEqual(oc.file_changes(details), [("$FOLDER/notes.txt", "add")])
        update = {"changed": True, "patch": "--- /w/a.txt\n+++ /w/a.txt\n@@ -1 +1 @@\n-a\n+b\n"}
        self.assertEqual(oc.file_changes(update), [("/w/a.txt", "update")])
        gone = {"changed": True, "patch": "--- /w/a.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-a\n"}
        self.assertEqual(oc.file_changes(gone), [("/w/a.txt", "delete")])
        self.assertEqual(oc.file_changes({"status": "ok"}), [])


# OpenClaw's plugin API, as a plugin's `register(api)` gets it: its tools,
# and its `before_tool_call` hooks run on the calls given.
HARNESS = r"""
const plugin = (await import(process.argv[2])).default;
const tools = {};
const hooks = [];
plugin.register({ registerTool: (tool) => { tools[tool.name] = tool; }, on: (name, hook) => hooks.push([name, hook]) });
const ran = {};
for (const [name, params] of Object.entries(JSON.parse(process.argv[3]))) {
  try { ran[name] = { result: await tools[name].execute("call-1", params, undefined) }; }
  catch (error) { ran[name] = { error: error.message }; }
}
const hooked = [];
for (const [toolName, params] of JSON.parse(process.argv[4])) {
  for (const [name, hook] of hooks) if (name === "before_tool_call") hooked.push(hook({ toolName, params }) ?? null);
}
const registered = Object.fromEntries(Object.entries(tools).map(([name, tool]) => [name, { parameters: tool.parameters }]));
console.log(JSON.stringify({ id: plugin.id, registered, ran, hooked }));
"""


@js_plugin_rig.needs_node
class Plugin(unittest.TestCase):
    def run_plugin(self, tools, calls, hooked):
        home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, home, True)
        loader = os.path.join(home, "index.mjs")
        with open(loader, "w") as f:
            f.write(oc.plugin_files(tools)["weft-plugin/index.js"])
        return js_plugin_rig.run_node(self, HARNESS, loader, json.dumps(calls), json.dumps(hooked))

    def test_each_tool_keeps_its_plain_name_and_goes_to_the_route(self):
        url, calls = js_plugin_rig.route(self)
        adapters.SERVER_URL = url.removesuffix("/mcp")
        tools = [js_plugin_rig.tool("lookup"), js_plugin_rig.tool("failing", required=())]
        ran = self.run_plugin(tools, {"lookup": {"q": "a"}, "failing": {"q": "b"}}, [])
        self.assertEqual(ran["id"], "weft", "the id its manifest names")
        self.assertEqual(ran["registered"], {"lookup": {"parameters": tools[0]["parameters"]}, "failing": {"parameters": tools[1]["parameters"]}})
        self.assertEqual(ran["ran"], {"lookup": {"result": {"content": [{"type": "text", "text": 'lookup {"q": "a"}'}], "details": {}}},
                                      "failing": {"error": 'failing {"q": "b"}'}})
        self.assertEqual([c["params"]["name"] for c in calls], ["lookup", "failing"])

    def test_every_question_waits_as_long_as_openclaw_allows(self):
        adapters.SERVER_URL = "http://127.0.0.1:9"
        asked = {"questions": [{"id": "q", "header": "Q", "question": "?", "options": []}], "timeoutSeconds": 120}
        ran = self.run_plugin([], {}, [["ask_user", asked], ["ask_user", {"questions": []}], ["exec", {"command": "ls"}]])
        self.assertEqual(ran["registered"], {}, "a program with no tools still gets the plugin")
        self.assertEqual(ran["hooked"], [{"params": dict(asked, timeoutSeconds=3600)},
                                         {"params": {"questions": [], "timeoutSeconds": 3600}}, None])


class OpenClawTest(rig.Rig):
    def setUp(self):
        super().setUp()
        bin_dir = os.path.join(self.root, "openclaw-bin")
        os.makedirs(bin_dir)
        rig.shim(bin_dir, "openclaw", "fake_openclaw.py")
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ["PATH"]

    def setup(self, **more):
        return self.configure("openclaw", **dict({"model": "fake-model", "credentials": KEY}, **more))

    def received(self) -> list:
        return [m for m in self.logged() if "start" not in m]

    def starts(self) -> list:
        return [m["start"] for m in self.logged() if "start" in m]

    def sent(self, method: str) -> list:
        return [m["params"] for m in self.received() if m.get("method") == method]

    def test_the_programs_tool_goes_through_wefts_plugin_under_its_plain_name(self):
        self.setup()
        turn = self.turn("look it up", tools=rig.LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {"q": "weft"}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "weft means woven"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "done")
        self.assertEqual([e["type"] for e in answer["seen"]], [ev.TOOL_CALL, ev.TEXT, ev.DONE],
                         "the program's call shows once, as its tool_call")
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        self.assertEqual([m["role"] for m in history], ["user", "assistant", "tool", "assistant"])
        self.assertEqual(history[2]["content"], "weft means woven")

    def test_the_handshake_and_the_message(self):
        self.setup()
        self.server.until(self.turn("look it up", tools=rig.LOOKUP), ev.WAITING_TOOL)
        [connect] = self.sent("connect")
        self.assertEqual((connect["client"]["id"], connect["client"]["mode"], connect["role"]), ("gateway-client", "backend", "operator"))
        self.assertEqual(connect["scopes"], oc.SCOPES)
        self.assertNotIn("device", connect, "a loopback backend client with the token needs no device identity")
        config = self.starts()[0]["config"]
        self.assertEqual(connect["auth"], {"token": config["gateway"]["auth"]["token"]})
        key = self.server.conversation.conversation_id()
        self.assertRegex(key, r"^agent:weft:weft-[0-9a-f-]{36}$")
        self.assertEqual(self.sent("sessions.create"), [{"key": key, "cwd": self.folder}])
        self.assertEqual(self.sent("sessions.patch"), [{"key": key, "thinkingLevel": None}])
        [send] = self.sent("chat.send")
        self.assertEqual((send["sessionKey"], send["message"], send["suppressCommandInterpretation"]), (key, "look it up", True))

    def test_a_shell_command_asks_and_the_answer_is_its_decision(self):
        self.setup()
        turn = self.turn("go")
        waiting = self.server.until(turn, ev.WAITING_QUESTION)
        question = waiting["pending"]["question"]
        self.assertEqual(question["kind"], ev.KIND_PERMISSION)
        self.assertEqual(question["permission"]["title"], "OpenClaw wants to run a command")
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow-once"}})
        answer = self.server.until(turn, ev.DONE, after=waiting["after"])
        self.assertEqual(self.sent("exec.approval.resolve"), [{"id": question["id"], "decision": "allow-once"}])
        self.assertEqual([(e["type"], e.get("output")) for e in answer["seen"] if e["type"] == ev.TOOL_RESULT],
                         [(ev.TOOL_RESULT, "(no output)")])

    def test_its_own_question_reaches_weft_and_the_answer_goes_back(self):
        self.setup(streamBy="token")
        turn = self.turn("pick a color")
        waiting = self.server.until(turn, ev.WAITING_QUESTION)
        seen = [e["type"] for e in waiting["seen"]]
        self.assertEqual(seen[:2], [ev.THINKING, ev.TOOL_USE], "its thinking, then its own ask tool")
        self.assertEqual(waiting["seen"][1]["name"], "ask_user")
        question = waiting["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"color_choice": ["Blue"]}}})
        answer = self.server.until(turn, ev.DONE, after=waiting["after"])
        self.assertEqual(self.sent("question.resolve"), [{"id": question["id"], "answers": {"answers": {"color_choice": ["Blue"]}}}])
        self.assertEqual(answer["reply"], "Blue")
        self.assertEqual([e["type"] for e in answer["seen"]], [ev.TOOL_RESULT, ev.TEXT_DELTA, ev.TEXT, ev.DONE])

    def test_a_written_file_is_a_file_change(self):
        self.setup()
        answer = self.server.until(self.turn("write a file"), ev.DONE)
        [change] = [e for e in answer["seen"] if e["type"] == ev.FILE_CHANGE]
        self.assertEqual((change["path"], change["change"]), (os.path.join(self.folder, "notes.txt"), "add"))

    def test_a_real_models_turn_through_the_programs_tool(self):
        self.setup()
        turn = self.turn("call the lookup tool", tools=rig.LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "weft answered lookup {}"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "weft answered lookup {}\n\nRed (Recommended)")
        self.assertEqual([e["type"] for e in answer["seen"]], [ev.THINKING, ev.TOOL_CALL, ev.TEXT, ev.DONE])

    def test_a_turn_the_gateway_fails_says_why(self):
        self.setup()
        failed = self.server.until(self.turn("fail please"), ev.FAILED)
        self.assertIn("OpenClaw could not finish the turn: Couldn't connect to the AI service", failed["error"])

    def test_a_restart_finds_the_session_and_sets_its_level_again(self):
        self.setup(options={"effort": "high"})
        self.server.until(self.turn("write a file"), ev.DONE)
        key = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.setup()
        self.assertEqual(self.server.until(self.turn("write a file"), ev.DONE)["reply"], "done")
        self.assertEqual(len(self.sent("sessions.create")), 1)
        self.assertEqual(self.sent("sessions.describe"), [{"key": key}])
        self.assertEqual(self.sent("sessions.patch"), [{"key": key, "thinkingLevel": "high"}, {"key": key, "thinkingLevel": None}])

    def test_a_session_the_gateway_no_longer_has_says_to_start_over(self):
        self.setup()
        self.server.until(self.turn("write a file"), ev.DONE)
        self.server.close()
        os.remove(os.path.join(self.home, "openclaw-home", "fake-sessions.json"))
        self.server = self.start()
        self.setup()
        self.assertIn("start the conversation over", self.server.until(self.turn("write a file"), ev.FAILED)["error"])

    def test_a_gateway_still_starting_is_connected_to_again(self):
        self.setup(model="starts-slowly")
        self.assertEqual(self.server.until(self.turn("write a file"), ev.DONE)["reply"], "done")
        self.assertEqual(len(self.sent("connect")), 2, "a new socket for the second try: the first answers nothing more")

    def test_a_gateway_that_ends_as_it_starts_says_so(self):
        self.setup(model="exits-at-start")
        failed = self.server.until(self.turn("write a file"), ev.FAILED)
        self.assertIn("Gateway ended as it started (exit 3)", failed["error"])
        self.assertIn("the config is not valid", failed["error"])

    def test_the_gateway_runs_tethered_in_a_home_of_its_own_on_the_connections_key(self):
        os.environ.update(OPENCLAW_STATE_DIR="/person/state", OPENCLAW_GATEWAY_TOKEN="theirs", GOOGLE_API_KEY="not-this-one")
        self.setup()
        self.server.until(self.turn("write a file"), ev.DONE)
        start = self.starts()[0]
        env = start["env"]
        self.assertEqual(start["args"], ["gateway", "run"])
        self.assertEqual(env["OPENCLAW_HOME"], os.path.join(self.home, "openclaw-home"))
        self.assertEqual(env["OPENAI_API_KEY"], "sk-compatible-secret")
        self.assertEqual((env["OPENCLAW_HIDE_BANNER"], env["OPENCLAW_NO_AUTO_UPDATE"]), ("1", "1"))
        for name in ("OPENCLAW_STATE_DIR", "OPENCLAW_GATEWAY_TOKEN", "GOOGLE_API_KEY"):
            self.assertNotIn(name, env)
        self.assertEqual(self.server.conversation.adapter.command()[1], adapters.TETHER)
        config_path = os.path.join(self.home, "openclaw-home", ".openclaw", "openclaw.json")
        self.assertEqual(os.stat(config_path).st_mode & 0o777, 0o600, "it holds the Gateway's token")

    def test_the_gateway_ends_with_the_adapter(self):
        self.setup()
        self.server.until(self.turn("write a file"), ev.DONE)
        adapter = self.server.conversation.adapter
        adapter.stop()
        self.assertIsNotNone(adapter.proc.poll())
        self.assertIsNone(adapter.sock)

    def test_no_connection_says_what_to_wire(self):
        self.configure("openclaw", model="fake-model")
        self.assertIn("wire an access node to `access`", self.server.until(self.turn("write a file"), ev.FAILED)["error"])

    def test_an_effort_it_does_not_take_is_refused(self):
        self.setup(options={"effort": "extreme"})
        self.assertIn("takes off, minimal", self.server.until(self.turn("write a file"), ev.FAILED)["error"])


class GaveUp(unittest.TestCase):
    """A question or an approval the Gateway ended on its side (its wait ran
    out: `question.resolved` with the status `expired`, the shape its schema
    gives) is one the turn stops waiting on."""

    def test_the_turn_stops_waiting_on_it(self):
        stopped = []

        class Conv:
            def stop_waiting(self, turn, call_id):
                stopped.append(call_id)

        adapter = oc.OpenClaw.__new__(oc.OpenClaw)
        adapter.conv, adapter.turn = Conv(), object()
        adapter.begin_turn()
        adapter.waits.add("ask_1")
        adapter.on_frame({"type": "event", "event": "question.resolved", "payload": {"id": "ask_1", "status": "expired"}})
        adapter.on_frame({"type": "event", "event": "question.resolved", "payload": {"id": "ask_2", "status": "expired"}})
        self.assertEqual(stopped, ["ask_1"], "only one this turn waits on")


if __name__ == "__main__":
    unittest.main()
