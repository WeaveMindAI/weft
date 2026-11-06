"""nanobot's adapter against a stand-in replaying the frames its gateway
really sent (`testing/fake_nanobot.py`, `testing/fake_nanobot.json`): a
turn's events, its own tools and file edits, the program's tool through
weft's tool plugin, reasoning, a key the provider refuses, a restart that
attaches to the chat and sets its scope again, the gateway tethered to the
server, and what each permission setting, option and login does to it.

Run with the others: `python3 -m unittest test_nanobot`.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_nanobot as nb  # noqa: E402
import adapters  # noqa: E402
import agent_server  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402

KEY = {"way": "openaiCompatible", "values": {"host": "http://127.0.0.1:9/v1", "key": "k-test"}}


class ToolPlugin(unittest.TestCase):
    """`plugin_nanobot.py` loaded as nanobot loads it (an entry point's
    module), against a stand-in of nanobot's `Tool`."""

    def test_each_program_tool_is_a_tool_class_forwarding_to_the_route(self):
        import asyncio  # noqa: PLC0415
        import importlib.util  # noqa: PLC0415
        import shutil  # noqa: PLC0415
        import types  # noqa: PLC0415
        import unittest.mock  # noqa: PLC0415
        from route_stub import RouteStub  # noqa: PLC0415

        class Tool:
            pass

        class ToolResult(str):
            @classmethod
            def error(cls, text):
                return ("error", text)

        base = types.SimpleNamespace(Tool=Tool, ToolResult=ToolResult)
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        route = RouteStub("found it")
        self.addCleanup(route.close)
        shutil.copy(nb.PLUGIN_FILE, os.path.join(folder, "weft_nanobot.py"))
        with open(os.path.join(folder, "server.path"), "w", encoding="utf-8") as f:
            f.write(os.path.dirname(nb.PLUGIN_FILE))
        tool = {"name": "lookup_file", "description": "The program's reader", "parameters": {"type": "object", "properties": {}}}
        with open(os.path.join(folder, "tools.json"), "w", encoding="utf-8") as f:
            json.dump({"url": route.url, "tools": [tool, dict(tool, name="lookup")]}, f)
        with unittest.mock.patch.dict(sys.modules, {"nanobot": types.ModuleType("nanobot"), "nanobot.agent.tools.base": base}):
            spec = importlib.util.spec_from_file_location("weft_nanobot", os.path.join(folder, "weft_nanobot.py"))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        first, second = module.tool_0(), module.tool_1()
        self.assertEqual((first.name, first.description, first.parameters), ("lookup_file", tool["description"], tool["parameters"]))
        self.assertEqual(second.name, "lookup")
        self.assertEqual(module.tool_0._scopes, {"core"}, "the main agent's alone, never a subagent's")
        self.assertEqual(asyncio.run(first.execute(path="a.txt")), "found it")
        self.assertEqual(route.called(), [("lookup_file", {"path": "a.txt"})])
        route.is_error = True
        self.assertEqual(asyncio.run(second.execute()), ("error", "found it"))


class NanobotTest(Rig):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), "nanobot", "fake_nanobot.py")

    def setup(self, **more):
        return self.configure("nanobot", **dict({"model": "stub-model", "permission": "restricted", "credentials": KEY}, **more))

    def received(self) -> list:
        return [m for m in self.logged() if "start" not in m]

    def starts(self) -> list:
        return [m["start"] for m in self.logged() if "start" in m]

    def sent(self, kind: str) -> list:
        return [m for m in self.received() if m.get("type") == kind]

    def test_a_turn_streams_its_text_and_ends_with_the_reply(self):
        self.setup(streamBy="token")
        turn = self.turn("say hi")
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "echo: say hi")
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.TEXT_DELTA, "echo: say hi"), (ev.TEXT, "echo: say hi"), (ev.DONE, None)])
        new_chat = self.sent("new_chat")[0]
        self.assertEqual(new_chat["workspace_scope"], {"project_path": self.folder, "access_mode": "restricted"})
        self.assertEqual(self.sent("message")[0], {"type": "message", "chat_id": self.server.conversation.conversation_id(),
                                                   "content": "say hi", "turn_id": turn, "webui": True})
        config = self.starts()[0]["config"]
        self.assertEqual(config["agents"]["defaults"]["idleCompactAfterMinutes"], 0)
        self.assertFalse(config["agents"]["defaults"]["dream"]["enabled"])
        self.assertFalse(config["gateway"]["heartbeat"]["enabled"])
        self.assertFalse(config["tools"]["my"]["enable"])
        self.assertNotIn("mcpServers", config["tools"], "the program's tools are never an MCP server")
        self.assertNotIn("PYTHONPATH", self.starts()[0]["env"], "no program tools, no plugin")
        self.assertEqual(config["providers"], {"custom": {"apiKey": "${OPENAI_COMPATIBLE_API_KEY}", "apiBase": "${OPENAI_COMPATIBLE_BASE_URL}"}},
                         "the key is named, never written")

    def test_its_own_tools_and_file_edits_become_events(self):
        self.setup()
        turn = self.turn("please write a file")
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "All done.")
        seen = [e for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT, ev.FILE_CHANGE)]
        self.assertEqual([(e["type"], e.get("name"), e.get("isError")) for e in seen],
                         [(ev.TOOL_USE, "write_file", None), (ev.FILE_CHANGE, None, None), (ev.TOOL_RESULT, None, False),
                          (ev.TOOL_USE, "exec", None), (ev.TOOL_RESULT, None, True)])
        self.assertEqual((seen[1]["path"], seen[1]["change"]), (os.path.join(self.folder, "hello.txt"), "add"))
        self.assertIn("blocked by deny pattern", seen[4]["output"])
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        self.assertEqual([m["role"] for m in history], ["user", "assistant", "tool", "assistant", "tool", "assistant"])

    def test_the_programs_tool_goes_through_weft_s_tool_plugin(self):
        self.setup()
        turn = self.turn("tool please", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "The tool said: 42")
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]], "the program's call shows once, as its tool_call")
        start = self.starts()[0]
        plugin = os.path.join(self.home, "nanobot-home", "instance", nb.PLUGIN_DIR)
        self.assertEqual(start["env"]["PYTHONPATH"].split(os.pathsep)[0], plugin)
        with open(os.path.join(plugin, "weft_nanobot_tools-1.dist-info", "entry_points.txt"), encoding="utf-8") as f:
            self.assertEqual(f.read(), "[nanobot.tools]\nweft_0 = weft_nanobot:tool_0\n")
        with open(os.path.join(plugin, "tools.json"), encoding="utf-8") as f:
            listed = json.load(f)
        self.assertEqual((listed["url"], [t["name"] for t in listed["tools"]]), (adapters.mcp_url(), ["lookup"]))
        with open(nb.PLUGIN_FILE, encoding="utf-8") as f, open(os.path.join(plugin, "weft_nanobot.py"), encoding="utf-8") as g:
            self.assertEqual(g.read(), f.read())
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        self.assertEqual([c["name"] for m in history for c in m.get("tool_calls", [])], ["lookup"],
                         "the history names the program's tool as the program does")

    def test_a_program_tool_named_like_a_lone_built_in_turns_only_that_one_off(self):
        image = nb.instance_config({"model": "m", "options": {}}, "full", "openaiKey", "/w", "/s", "t", [{"name": "generate_image"}])
        self.assertEqual(image["tools"]["imageGeneration"], {"enabled": False}, "that group's switch is `enabled`")
        apps = nb.instance_config({"model": "m", "options": {}}, "restricted", "openaiKey", "/w", "/s", "t", [{"name": "run_cli_app"}])
        self.assertEqual(apps["tools"]["cliApps"], {"enable": False})
        self.assertTrue(apps["tools"]["restrictToWorkspace"], "the permission's own keys stay")
        self.assertEqual(apps["tools"]["exec"], {}, "the shell stays on")

    def test_a_name_of_a_group_of_several_never_turns_the_group_off(self):
        # The build refuses these names (`reservedToolNames`); a tool list the
        # build never saw still leaves every group as the settings make it.
        grouped = ["read_file", "grep", "exec", "rg", "web_search", "web_fetch"]
        config = nb.instance_config({"model": "m", "options": {}}, "restricted", "openaiKey", "/w", "/s", "t",
                                    [{"name": n} for n in grouped])["tools"]
        self.assertNotIn("file", config)
        self.assertNotIn("enable", config["exec"])
        self.assertTrue(config["web"]["enable"])
        with open(os.path.join(os.path.dirname(nb.PLUGIN_FILE), "..", "nanobot", "metadata.json"), encoding="utf-8") as f:
            reserved = set(json.load(f)["lists"]["reservedToolNames"])
        self.assertTrue(set(grouped) <= reserved)
        self.assertEqual(reserved & set(nb.SWITCHES), set(), "a name nanobot can give up alone is never refused")

    def test_a_call_of_the_program_s_name_is_the_program_s_and_a_built_in_s_is_its_own(self):
        adapter = nb.Nanobot.__new__(nb.Nanobot)
        adapter.program_tools = adapters.ProgramTools([{"name": "generate_image"}])
        adapter.ours, adapter.program_calls = set(), set()
        emitted = []
        adapter.conv = type("Conv", (), {"emit": lambda _self, turn, kind, **fields: emitted.append((kind, fields.get("name"))),
                                         "remember": lambda _self, turn, message: emitted.append(("history", message))})()
        adapter.turn = "t"
        adapter._tool("t", {"call_id": "c1", "name": "generate_image", "phase": "start", "arguments": {}})
        adapter._tool("t", {"call_id": "c2", "name": "exec", "phase": "start", "arguments": {}})
        self.assertEqual(adapter.ours, {"c1"})
        self.assertEqual([e for e in emitted if e[0] == ev.TOOL_USE], [(ev.TOOL_USE, "exec")])
        self.assertEqual([m["tool_calls"][0]["name"] for kind, m in emitted if kind == "history"], ["exec"],
                         "the route writes the program's call; the adapter writes the agent's own")

    def test_reasoning_is_thinking(self):
        self.setup()
        answer = self.server.until(self.turn("think hard"), ev.DONE)
        self.assertEqual([e["text"] for e in answer["seen"] if e["type"] == ev.THINKING], ["Pondering the question."])
        self.assertEqual(answer["reply"], "echo: think hard")

    def test_a_refused_key_says_to_reconnect(self):
        self.setup()
        answer = self.server.until(self.turn("fail please"), ev.FAILED)
        self.assertIn("refused the connection's key", answer["error"])
        self.assertIn("reconnect `access`", answer["error"])

    def test_an_error_frame_fails_the_turn(self):
        self.setup()
        answer = self.server.until(self.turn("nothing the gateway knows"), ev.FAILED)
        self.assertIn("nanobot refused the message: unknown type: 'bogus'", answer["error"])

    def test_a_gateway_that_exits_before_answering_says_so(self):
        self.setup(model="exits-before-ready")
        error = self.server.until(self.turn("say hi"), ev.FAILED)["error"]
        self.assertIn("nanobot exited (exit 3) before answering", error)

    def test_a_gateway_that_exits_between_two_answers_of_one_request_says_so(self):
        # `new_chat` is answered twice; the second is failed by the gateway's
        # exit, which takes its box out of `waiting` before it is read.
        self.setup(model="exits-after-attached")
        error = self.server.until(self.turn("say hi"), ev.FAILED)["error"]
        self.assertIn("nanobot exited (exit 3) before answering", error)

    def test_a_frame_of_another_turn_is_not_this_turns(self):
        self.setup()
        self.server.until(self.turn("say hi"), ev.DONE)
        adapter = self.server.conversation.adapter
        turn = agent_server.Turn("t-x", "x", [])
        adapter.turn, turn.status = turn, ev.WORKING
        before = len(self.server.conversation.events)
        adapter.on_frame({"event": "turn_end", "chat_id": adapter.chat, "turn_id": "a-scheduled-job"})
        adapter.on_frame({"event": "delta", "chat_id": "another-chat", "turn_id": turn.id, "text": "no"})
        self.assertEqual((len(self.server.conversation.events), turn.status), (before, ev.WORKING))

    def test_a_restart_attaches_to_the_chat_and_sets_its_scope_again(self):
        self.setup()
        self.server.until(self.turn("say hi"), ev.DONE)
        chat = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.setup(permission="full", options={"appendSystemPrompt": "Answer like a pirate."})
        self.assertEqual(self.server.until(self.turn("say hi"), ev.DONE)["reply"], "echo: say hi")
        self.assertEqual(len(self.sent("new_chat")), 1)
        self.assertEqual(self.sent("attach"), [{"type": "attach", "chat_id": chat}])
        self.assertEqual(self.sent("set_workspace_scope")[0]["workspace_scope"], {"project_path": self.folder, "access_mode": "full"})
        self.assertFalse(self.starts()[1]["config"]["tools"]["restrictToWorkspace"])
        workspace = self.starts()[1]["config"]["agents"]["defaults"]["workspace"]
        with open(os.path.join(workspace, "USER.md"), encoding="utf-8") as f:
            self.assertEqual(f.read(), "Answer like a pirate.\n")

    def test_a_chat_nanobot_no_longer_has_says_to_start_over(self):
        self.setup()
        self.server.until(self.turn("say hi"), ev.DONE)
        instance = os.path.dirname(self.starts()[0]["config"]["agents"]["defaults"]["workspace"])
        self.server.close()
        for folder, _, names in os.walk(os.path.join(instance, "sessions")):
            for name in names:
                os.remove(os.path.join(folder, name))
        self.server = self.start()
        self.setup()
        self.assertIn("start the conversation over", self.server.until(self.turn("say hi"), ev.FAILED)["error"])

    def test_the_key_reaches_nanobot_as_its_variable_in_a_home_of_its_own(self):
        os.environ.update(NANOBOT_TOOLS__EXEC__ENABLE="true", XDG_DATA_HOME="/person/share", ANTHROPIC_API_KEY="not-this-one",
                          OAUTH_CLI_KIT_TOKEN_PATH="/person/codex.json")
        self.setup()
        self.server.until(self.turn("say hi"), ev.DONE)
        env = self.starts()[0]["env"]
        self.assertEqual((env["OPENAI_COMPATIBLE_API_KEY"], env["OPENAI_COMPATIBLE_BASE_URL"]), ("k-test", "http://127.0.0.1:9/v1"))
        self.assertEqual(env["HOME"], os.path.join(self.home, "nanobot-home"), "never the person's home, so never their ~/.codex")
        for name in ("NANOBOT_TOOLS__EXEC__ENABLE", "XDG_DATA_HOME", "ANTHROPIC_API_KEY", "OAUTH_CLI_KIT_TOKEN_PATH"):
            self.assertNotIn(name, env)

    def test_no_connection_says_what_to_wire(self):
        self.configure("nanobot", model="stub-model", permission="restricted")
        self.assertIn("Wire a NanobotAccess into `access`", self.server.until(self.turn("say hi"), ev.FAILED)["error"])

    def test_the_persons_own_build_runs_under_the_tether(self):
        own = os.path.join(self.root, "own")
        os.makedirs(own)
        log = os.path.join(self.root, "own.log")
        shim(own, "my-nanobot", "fake_nanobot.py", {"FAKE_LOG": log})
        self.setup(program=os.path.join(own, "my-nanobot"))
        self.server.until(self.turn("say hi"), ev.DONE)
        with open(log) as f:
            started = [json.loads(line)["start"] for line in f if "start" in line]
        self.assertEqual(started[0]["args"][:2], ["--home", self.server.conversation.adapter.instance])
        self.assertFalse(os.path.exists(self.log), "the nanobot on PATH never ran")

    def test_the_gateway_and_its_socket_end_with_the_adapter(self):
        self.setup()
        self.server.until(self.turn("say hi"), ev.DONE)
        adapter = self.server.conversation.adapter
        folder = adapter.socket_dir
        adapter.stop()
        self.assertEqual((adapter.proc.poll() is not None, os.path.exists(folder)), (True, False))


class TetherTest(unittest.TestCase):
    def test_the_gateway_ends_when_the_agent_server_goes(self):
        folder = tempfile.mkdtemp()
        tether = subprocess.Popen([sys.executable, adapters.TETHER, folder, "--", sys.executable, "-c", "import time; time.sleep(60)"],
                                  stdin=subprocess.PIPE)
        tether.stdin.close()
        self.assertEqual(tether.wait(10), 143, "the gateway ended by SIGTERM")
        self.assertFalse(os.path.exists(folder))

    def test_its_exit_is_the_gateways(self):
        folder = tempfile.mkdtemp()
        tether = subprocess.Popen([sys.executable, adapters.TETHER, folder, "--", sys.executable, "-c", "raise SystemExit(3)"],
                                  stdin=subprocess.PIPE)
        self.assertEqual(tether.wait(10), 3)
        tether.stdin.close()


class NanobotSettingsTest(unittest.TestCase):
    def config(self, permission="restricted", way="anthropicKey", tools=None, **options):
        adapters.SERVER_URL = "http://127.0.0.1:1"
        return nb.instance_config({"model": "m", "options": options}, permission, way, "/w", "/s/ws.sock", "t", tools or [])

    def test_every_permission_is_an_access_mode_and_its_tools(self):
        self.assertEqual(nb.policy("restricted"), ("restricted", {"restrictToWorkspace": True, "exec": {}}))
        self.assertEqual(nb.policy("full"), ("full", {"restrictToWorkspace": False, "exec": {}}))
        self.assertEqual(nb.policy("no_shell"), ("restricted", {"restrictToWorkspace": True, "exec": {"enable": False},
                                                                "cliApps": {"enable": False}}))
        with self.assertRaises(Refused):
            nb.policy("ask")

    def test_sandboxed_needs_the_sandbox_program(self):
        path = os.environ["PATH"]
        self.addCleanup(os.environ.__setitem__, "PATH", path)
        empty = tempfile.mkdtemp()
        os.environ["PATH"] = empty
        with self.assertRaises(Refused) as refused:
            nb.policy("sandboxed")
        self.assertIn("bubblewrap", str(refused.exception))
        shim(empty, "bwrap", "fake_nanobot.py")
        self.assertEqual(nb.policy("sandboxed")[1]["exec"], {"sandbox": "bwrap"})

    def test_each_option_lands_on_its_config_key(self):
        config = self.config(effort="high", maxTokens=32000, contextWindowTokens=128000, maxToolIterations=50,
                             fallbackModels=["claude-haiku-4-5"], commandMaxSeconds=7200, denyPatterns=[r"git\s+push"],
                             allowPatterns=[r"ls.*"], webTools=False)
        defaults = config["agents"]["defaults"]
        self.assertEqual((defaults["provider"], defaults["model"], defaults["reasoningEffort"], defaults["maxTokens"],
                          defaults["contextWindowTokens"], defaults["maxToolIterations"]),
                         ("anthropic", "m", "high", 32000, 128000, 50))
        self.assertEqual(defaults["fallbackModels"], [{"model": "claude-haiku-4-5", "provider": "anthropic"}])
        self.assertEqual(config["tools"]["exec"], {"maxRuntime": 7200, "denyPatterns": [r"git\s+push"], "allowPatterns": [r"ls.*"]})
        self.assertFalse(config["tools"]["web"]["enable"])
        self.assertEqual(config["providers"], {"anthropic": {"apiKey": "${ANTHROPIC_API_KEY}"}})
        self.assertEqual(config["channels"]["websocket"]["unixSocketPath"], "/s/ws.sock")

    def test_a_value_nanobot_does_not_take_is_refused(self):
        for options in ({"effort": "extreme"}, {"maxTokens": 0}, {"maxTokens": True}, {"denyPatterns": ["("]},
                        {"fallbackModels": "m"}, {"webTools": "yes"}):
            with self.assertRaises(Refused, msg=options):
                self.config(**options)
        with self.assertRaises(Refused):
            nb.instance_config({"options": {}}, "restricted", "openaiKey", "/w", "", "", [])

    def test_a_file_edit_is_an_add_a_delete_or_an_update(self):
        self.assertEqual([nb.file_change({"operation": op}) for op in ("create", "delete", None)], ["add", "delete", "update"])


if __name__ == "__main__":
    unittest.main()
