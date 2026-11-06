"""Pi against its stand-in (`testing/fake_pi.py`, lines real `pi --mode rpc`
wrote): a turn's events, a permission allowed, refused with a note and
allowed from now on, the ask tool (two questions open at once among them),
the program's tools under their plain names (one replacing a built-in), a
restart that resumes the session, a folder Pi
has no session for, a refused key, and what every permission setting, login
way and option does to the command line and the home. weft's extension
(`pi_weft.ts`) runs on Node against a stand-in of Pi's extension API: which
tool calls it asks about under each setting, what it does with each answer,
its ask tool, and the program's tools it registers and forwards.

Run with the others: `python3 -m unittest test_pi`.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import js_plugin_rig  # noqa: E402
import adapter_pi  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402

KEY = {"way": "openaiCompatible", "values": {"host": "http://127.0.0.1:9/v1", "key": "k-test"}}


class PiTest(Rig):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), "pi", "fake_pi.py")

    def setup(self, **more):
        return self.configure("pi", **dict({"model": "fake-model", "credentials": KEY}, **more))

    def starts(self) -> list:
        return [m for m in self.logged() if "argv" in m]

    def sent(self, kind: str) -> list:
        return [m for m in self.logged() if m.get("type") == kind]

    def question(self, turn, after=0):
        pending = self.server.until(turn, ev.WAITING_QUESTION, after=after)
        return pending["pending"]["question"], pending["after"]

    def test_a_turn_streams_its_text_and_ends_with_the_reply(self):
        self.setup(streamBy="token")
        answer = self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(answer["reply"], "done")
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.TEXT_DELTA, "done"), (ev.TEXT, "done"), (ev.DONE, None)])
        argv = self.starts()[0]["argv"]
        session = self.server.conversation.conversation_id()
        self.assertEqual(argv, ["--mode", "rpc", "--provider", "compatible", "--model", "fake-model", "--session-id", session,
                                "-e", os.path.join(self.home, "pi-home", adapter_pi.LOADER), "--weft-permission", "ask", "--no-approve"])
        self.assertEqual(self.sent("prompt")[0]["message"], "hello")

    def test_an_allowed_tool_runs_and_its_result_is_streamed(self):
        self.setup()
        turn = self.turn("run it")
        question, after = self.question(turn)
        self.assertEqual(question["kind"], ev.KIND_PERMISSION)
        permission = question["permission"]
        self.assertEqual((permission["tool"], permission["detail"], permission["input"]),
                         ("bash", "echo hello-from-fake", {"command": "echo hello-from-fake"}))
        self.assertEqual([(c["id"], c["label"], c["kind"]) for c in permission["choices"]],
                         [("allow_once", "Allow once", ev.ALLOW_ONCE), ("allow_always", "Allow bash from now on", ev.ALLOW_ALWAYS),
                          ("reject", "Refuse", ev.REJECT)])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow_once"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(answer["reply"], "ran: hello-from-fake")
        self.assertEqual([(e["type"], e.get("output")) for e in answer["seen"] if e["type"] == ev.TOOL_RESULT],
                         [(ev.TOOL_RESULT, "hello-from-fake\n")])
        response = self.sent("extension_ui_response")[0]
        self.assertEqual(json.loads(response["value"]), {"choice": "allow_once", "note": ""})

    def test_a_refusal_carries_the_note_and_the_model_carries_on(self):
        self.setup()
        turn = self.turn("run it")
        question, after = self.question(turn)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "reject", "note": "not today"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(answer["reply"], "refused: The person refused this bash call: not today")
        self.assertIn(True, [e["isError"] for e in answer["seen"] if e["type"] == ev.TOOL_RESULT])

    def test_allowed_from_now_on_holds_for_the_conversation_and_across_a_restart(self):
        self.setup()
        turn = self.turn("run it")
        question, _ = self.question(turn)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow_always"}})
        self.server.until(turn, ev.DONE)
        self.assertEqual(self.server.until(self.turn("run again"), ev.DONE)["reply"], "ran: hello-from-fake")
        self.server.close()
        self.server = self.start()
        self.setup()
        self.assertEqual(self.server.until(self.turn("run once more"), ev.DONE)["reply"], "ran: hello-from-fake")
        self.assertEqual(len(self.sent("extension_ui_response")), 1)

    def test_the_ask_tool_puts_its_question_to_weft_and_gives_back_the_answer(self):
        self.setup()
        turn = self.turn("ask me")
        question, after = self.question(turn)
        self.assertEqual(question["kind"], ev.KIND_QUESTION)
        (asked,) = question["questions"]
        self.assertEqual((asked["question"], [o["label"] for o in asked["options"]], asked["allowOther"], asked["multiSelect"]),
                         ("Which colour?", ["red", "blue"], True, False))
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"answer": ["green"]}}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(answer["reply"], "green")
        self.assertEqual([e["type"] for e in answer["seen"]], [ev.TEXT, ev.DONE], "the ask tool is the question, not a tool use")

    def test_two_questions_open_at_once_reach_weft_one_after_the_other(self):
        self.setup()
        turn = self.turn("two questions")
        first, after = self.question(turn)
        self.assertEqual(first["questions"][0]["question"], "Which colour?")
        self.server.post("/turns", {"answer": {"questionId": first["id"], "answers": {"answer": ["red"]}}})
        second, after = self.question(turn, after=after)
        self.assertEqual((second["questions"][0]["question"], second["questions"][0]["multiSelect"]), ("Which size?", True))
        self.server.post("/turns", {"answer": {"questionId": second["id"], "answers": {"answer": ["S", "L"]}}})
        self.assertEqual(self.server.until(turn, ev.DONE, after=after)["reply"], "red / S\nL")

    def history_calls(self, turn) -> list:
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        return [c["name"] for m in history for c in m.get("tool_calls", [])]

    def test_the_programs_tool_goes_out_under_its_plain_name_through_the_extension(self):
        self.setup()
        turn = self.turn("tool lookup", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "looked up: 42")
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]], "the call goes out as the program's tool call")
        self.assertEqual([m["route"] for m in self.logged() if "route" in m], ["lookup"])
        self.assertFalse(os.path.exists(os.path.join(self.home, "pi-home", "mcp.json")), "no MCP server: the extension has the tools")
        self.assertEqual(self.history_calls(turn), ["lookup"], "the route wrote it, once, under its plain name")

    def test_a_program_tool_named_like_a_built_in_takes_its_place(self):
        self.setup()
        read = [{"name": "read", "description": "Read through weft", "parameters": {"type": "object", "properties": {}}}]
        turn = self.turn("tool read", tools=read)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual(call["name"], "read")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "weft's read"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "looked up: weft's read")
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]])
        self.assertEqual(self.history_calls(turn), ["read"])

    def test_a_built_in_the_program_has_no_tool_of_is_pis_own(self):
        self.setup(permission="bypass")
        turn = self.turn("tool read", tools=LOOKUP)
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "looked up: built-in read")
        self.assertIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]], "the agent's own tool shows as its tool use")
        self.assertEqual([m["route"] for m in self.logged() if "route" in m], [])
        self.assertEqual(self.history_calls(turn), ["read"], "the adapter wrote the agent's own call")

    def test_a_run_that_outlived_its_turn_fails_the_next_turn_loudly(self):
        self.setup()
        self.server.until(self.turn("hello"), ev.DONE)
        adapter = self.server.conversation.adapter
        adapter.on_message({"type": "agent_start"})
        adapter.dialogs.append(({"id": "d-1"}, {"weft": "question"}))
        error = self.server.until(self.turn("hello again"), ev.FAILED)["error"]
        self.assertIn("Pi's run of the last turn is still going as this one begins (1 dialog(s) of it open: d-1)", error)
        self.assertEqual(len(self.sent("prompt")), 1, "nothing was sent to the run of the last turn")
        # An aborted run settles with its dialogs resolved by Pi, and the next turn runs.
        adapter.on_message({"type": "agent_settled", "aborted": True})
        self.assertEqual(self.server.until(self.turn("hello once more"), ev.DONE)["reply"], "done")

    def test_a_restart_resumes_the_session_with_the_new_settings(self):
        self.setup()
        turn = self.turn("run it")
        self.question(turn)
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.setup(permission="bypass")
        self.assertEqual(self.server.call("GET", f"/events?turn={turn}")[1]["status"], ev.LOST)
        self.assertEqual(self.server.until(self.turn("run again"), ev.DONE)["reply"], "ran: hello-from-fake")
        argv = self.starts()[1]["argv"]
        self.assertEqual((argv[argv.index("--session-id") + 1], argv[argv.index("--weft-permission") + 1]), (session, "bypass"))

    def test_a_folder_pi_has_no_session_for_says_to_start_over(self):
        self.setup()
        self.server.until(self.turn("hello"), ev.DONE)
        self.server.close()
        self.server = self.start()
        elsewhere = os.path.join(self.root, "elsewhere")
        os.makedirs(elsewhere)
        self.setup(folder=elsewhere)
        error = self.server.until(self.turn("hello"), ev.FAILED)["error"]
        self.assertIn("Pi no longer has this conversation", error)
        self.assertIn("start the conversation over", error)
        self.assertEqual(len(self.sent("prompt")), 1, "no turn runs on an empty session")

    def test_a_refused_key_fails_the_turn_with_the_providers_answer(self):
        self.setup()
        error = self.server.until(self.turn("fail"), ev.FAILED)["error"]
        self.assertIn("the model call failed: 401", error)
        self.assertIn("Incorrect API key provided", error)

    def test_reasoning_is_shown(self):
        self.setup()
        answer = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]][:2], [(ev.THINKING, "Let me think."), (ev.TEXT, "thought")])

    def test_a_write_is_a_file_change_with_its_whole_path(self):
        self.setup(permission="accept_edits")
        answer = self.server.until(self.turn("write a"), ev.DONE)
        self.assertEqual([(e["type"], e.get("path"), e.get("change")) for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.FILE_CHANGE)],
                         [(ev.TOOL_USE, None, None), (ev.FILE_CHANGE, os.path.join(self.folder, "a.txt"), "write")])

    def test_no_connection_and_no_model_are_refused_before_anything_starts(self):
        with self.assertRaises(Refused) as refused:
            adapter_pi.Pi(self.server.conversation, {"folder": self.folder, "model": "m"}, [])
        self.assertIn("PiAccess", str(refused.exception))
        with self.assertRaises(Refused) as refused:
            adapter_pi.Pi(self.server.conversation, {"folder": self.folder, "credentials": KEY}, [])
        self.assertIn("set `model`", str(refused.exception))
        self.assertFalse(os.path.exists(self.log), "nothing started")


class PiSettingsTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)

        class Conversation:
            home = self.root

            def conversation_id(self):
                return None

        self.conv = Conversation()
        adapters.SERVER_URL = "http://127.0.0.1:1"

    def adapter(self, permission="ask", credentials=KEY, tools=(), **options):
        config = {"folder": self.root, "model": "m-1", "permission": permission, "credentials": credentials, "options": options}
        return adapter_pi.Pi(self.conv, config, list(tools))

    def test_every_permission_setting_reaches_the_extension_and_read_only_takes_the_changing_tools_away(self):
        for permission in adapter_pi.PERMISSIONS:
            cmd = self.adapter(permission).command()
            self.assertEqual(cmd[cmd.index("--weft-permission") + 1], permission)
            tools = cmd[cmd.index("--tools") + 1].split(",") if "--tools" in cmd else None
            self.assertEqual(tools, ["read", "grep", "find", "ls", "ask_user"] if permission == "read_only" else None, permission)

    def test_read_only_keeps_every_program_tool_whatever_its_name(self):
        tools = [{"name": name, "parameters": {"type": "object"}} for name in ("lookup", "bash", "read")]
        cmd = self.adapter("read_only", tools=tools).command()
        self.assertEqual(cmd[cmd.index("--tools") + 1].split(","), ["read", "grep", "find", "ls", "ask_user", "lookup", "bash"])

    def test_the_extension_is_loaded_through_a_loader_holding_the_route_and_the_tools(self):
        adapter = self.adapter(tools=LOOKUP)
        cmd = adapter.command()
        loader = cmd[cmd.index("-e") + 1]
        self.assertEqual(loader, os.path.join(adapter.home, adapter_pi.LOADER))
        adapter.prepare()
        with open(loader) as f:
            text = f.read()
        self.assertIn(f"import {{ make }} from {json.dumps('file://' + adapter_pi.EXTENSION)};", text)
        self.assertEqual(js_plugin_rig.made(text), {"url": adapters.mcp_url(), "tools": [{"name": "lookup", "description": "Look up the magic number",
                                                                       "parameters": {"type": "object", "properties": {}}}]})

    def test_every_login_way_hands_over_exactly_its_key_for_its_provider(self):
        os.environ["PI_CODING_AGENT_SESSION_DIR"] = "/the/persons/own"
        self.addCleanup(os.environ.pop, "PI_CODING_AGENT_SESSION_DIR", None)
        logins = adapters.AGENTS["pi"]["logins"]
        variables = {v for way in logins.values() for v in way["env"].values()}
        for way, facts in logins.items():
            values = {"key": f"k-{way}", "host": "http://127.0.0.1:9/v1"}
            adapter = self.adapter(credentials={"way": way, "values": values})
            env = adapter.env()
            (variable,) = facts["env"].values()
            self.assertEqual(env[variable], f"k-{way}", way)
            self.assertEqual(sorted(v for v in variables - {variable} if v in env), [], f"{way}: another way's key")
            self.assertEqual(env["PI_CODING_AGENT_DIR"], adapter.home)
            self.assertNotIn("PI_CODING_AGENT_SESSION_DIR", env, "the person's own Pi settings")
            self.assertEqual((env["PI_SKIP_VERSION_CHECK"], env["PI_TELEMETRY"]), ("1", "0"))
            cmd = adapter.command()
            self.assertEqual(cmd[cmd.index("--provider") + 1], adapter_pi.PROVIDERS[way])
            adapter.prepare()
            models = os.path.join(adapter.home, "models.json")
            if way == "openaiCompatible":
                with open(models) as f:
                    self.assertEqual(json.load(f), {"providers": {"compatible": {
                        "baseUrl": "http://127.0.0.1:9/v1", "api": "openai-completions", "apiKey": "$OPENAI_COMPATIBLE_API_KEY",
                        "models": [{"id": "m-1"}]}}})
            else:
                self.assertFalse(os.path.exists(models), f"{way}: a compatible server left declared")
            for name in os.listdir(adapter.home):
                with open(os.path.join(adapter.home, name)) as f:
                    self.assertNotIn(f"k-{way}", f.read(), f"{way}: the key is written to {name}")

    def test_the_options_become_their_flags_and_a_bad_value_is_refused(self):
        adapter = self.adapter(effort="high", appendSystemPrompt="  Be brief.  ", noContextFiles=True)
        cmd = adapter.command()
        prompt = os.path.join(adapter.home, adapter_pi.APPENDED_PROMPT)
        self.assertEqual(cmd[cmd.index("--no-approve") + 1:], ["--thinking", "high", "--append-system-prompt", prompt, "--no-context-files"])
        adapter.prepare()
        with open(prompt) as f:
            self.assertEqual(f.read(), "Be brief.\n")
        for bad in ({"effort": "ultra"}, {"noContextFiles": "yes"}, {"appendSystemPrompt": 3}):
            with self.assertRaises(Refused, msg=bad):
                self.adapter(**bad)


# Runs the extension's default export against a stand-in of Pi's extension
# API, scenario in on argv: every `tool_call` it was handed, what the
# handler asked (the dialog's title) and what it returned.
HARNESS = r"""
const scenario = JSON.parse(process.argv[3]);
const ext = (await import(process.argv[2])).make(scenario.program || { url: "http://127.0.0.1:1/mcp", tools: [] });
const handlers = {}, tools = {}, flags = {}, entries = [...(scenario.entries || [])];
ext({
  registerFlag: (name, options) => { flags[name] = options.default; },
  getFlag: (name) => flags[name],
  on: (event, handler) => { handlers[event] = handler; },
  registerTool: (tool) => { tools[tool.name] = tool; },
  appendEntry: (customType, data) => entries.push({ type: "custom", customType, data }),
});
if (scenario.permission !== undefined) flags["weft-permission"] = scenario.permission;
const answers = [...(scenario.answers || [])];
const asked = [];
const ui = {
  select: async (title, options) => { asked.push({ method: "select", title: JSON.parse(title), options }); return answers.shift() ?? undefined; },
  input: async (title) => { asked.push({ method: "input", title: JSON.parse(title) }); return answers.shift() ?? undefined; },
};
const ctx = { cwd: scenario.cwd, ui, sessionManager: { getEntries: () => entries } };
await handlers.session_start({}, ctx);
const results = [];
for (const call of scenario.calls || []) {
  const before = asked.length;
  const result = await handlers.tool_call({ toolCallId: "c", toolName: call.tool, input: call.input || {} }, ctx);
  results.push({ asked: asked.slice(before), result: result ?? null });
}
let ask = null;
if (scenario.ask) ask = await tools.ask_user.execute("call-1", scenario.ask, undefined, undefined, ctx);
const ran = {};
for (const [name, params] of Object.entries(scenario.run || {})) ran[name] = await tools[name].execute("call-2", params, undefined);
const registered = Object.fromEntries(Object.entries(tools).map(([name, tool]) => [name, { description: tool.description, parameters: tool.parameters }]));
console.log(JSON.stringify({ flags, results, ask, entries, tools: Object.keys(tools), registered, ran }));
"""


class Extension(unittest.TestCase):
    """Runs the extension in the harness."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.folder = os.path.join(self.root, "work")
        os.makedirs(self.folder)
        self.harness = os.path.join(self.root, "harness.mjs")
        with open(self.harness, "w") as f:
            f.write(HARNESS)

    def run_extension(self, **scenario) -> dict:
        scenario.setdefault("cwd", self.folder)
        done = subprocess.run(["node", "--experimental-strip-types", "--no-warnings", self.harness, adapter_pi.EXTENSION,
                               json.dumps(scenario)], capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        return json.loads(done.stdout)



@unittest.skipUnless(shutil.which("node"), "needs Node 22.6 or newer on PATH, which runs the extension")
class PiExtensionTest(Extension):
    def asks(self, permission: str, calls: list) -> list:
        """Whether the extension asks about each call, refusing every one it
        asks about, the program having one tool, `lookup`."""
        refusals = [json.dumps({"choice": "reject"})] * len(calls)
        program = {"url": "http://127.0.0.1:1/mcp", "tools": LOOKUP}
        results = self.run_extension(permission=permission, calls=calls, answers=refusals, program=program)["results"]
        return [bool(r["asked"]) for r in results]

    def test_it_takes_the_permission_flag_and_adds_the_ask_tool(self):
        ran = self.run_extension()
        self.assertEqual((ran["flags"], ran["tools"]), ({"weft-permission": "ask"}, ["ask_user"]))

    def test_what_each_setting_asks_about(self):
        os.symlink("/etc", os.path.join(self.folder, "etc-link"))
        calls = [{"tool": "read", "input": {"path": "src/main.rs"}},
                 {"tool": "ls", "input": {}},
                 {"tool": "read", "input": {"path": "/etc/hostname"}},
                 {"tool": "read", "input": {"path": "etc-link/hostname"}},
                 {"tool": "write", "input": {"path": "new/file.txt", "content": "x"}},
                 {"tool": "edit", "input": {"path": "../outside.txt"}},
                 {"tool": "bash", "input": {"command": "ls"}},
                 {"tool": "lookup", "input": {}},
                 {"tool": "ask_user", "input": {"question": "?"}},
                 {"tool": "something_else", "input": {}},
                 {"tool": "mcp__weft__lookup", "input": {}}]
        self.assertEqual(self.asks("ask", calls), [False, False, True, True, True, True, True, False, False, True, True])
        self.assertEqual(self.asks("accept_edits", calls), [False, False, True, True, False, True, True, False, False, True, True])
        self.assertEqual(self.asks("read_only", calls), [False] * len(calls))
        self.assertEqual(self.asks("bypass", calls), [False] * len(calls))

    def test_a_permission_question_names_the_call_and_offers_weft_three_choices(self):
        ran = self.run_extension(permission="ask", calls=[{"tool": "write", "input": {"path": "a.txt", "content": "x"}}],
                                 answers=[json.dumps({"choice": "allow_once"})])
        (result,) = ran["results"]
        (asked,) = result["asked"]
        self.assertEqual(asked, {"method": "select", "options": [c for c, _, _ in adapter_pi.CHOICES],
                                 "title": {"weft": "permission", "toolCallId": "c", "tool": "write", "input": {"path": "a.txt", "content": "x"},
                                           "path": os.path.join(self.folder, "a.txt")}})
        self.assertIsNone(result["result"], "allowed: the tool runs")

    def test_a_refusal_blocks_the_call_with_the_note_and_no_answer_blocks_it_too(self):
        bash = {"tool": "bash", "input": {"command": "rm -r x"}}
        ran = self.run_extension(permission="ask", calls=[bash, bash], answers=[json.dumps({"choice": "reject", "note": " not that "}), None])
        self.assertEqual([r["result"] for r in ran["results"]],
                         [{"block": True, "reason": "The person refused this bash call: not that"},
                          {"block": True, "reason": "No answer came: the turn ended before the person answered."}])

    def test_allowed_from_now_on_is_kept_in_the_session_and_read_back_from_it(self):
        bash = {"tool": "bash", "input": {"command": "ls"}}
        ran = self.run_extension(permission="ask", calls=[bash, bash], answers=[json.dumps({"choice": "allow_always"})])
        self.assertEqual([bool(r["asked"]) for r in ran["results"]], [True, False])
        self.assertEqual(ran["entries"], [{"type": "custom", "customType": "weft-allowed", "data": {"tool": "bash"}}])
        resumed = self.run_extension(permission="ask", calls=[bash], entries=ran["entries"])
        self.assertEqual(resumed["results"], [{"asked": [], "result": None}])

    def test_the_ask_tool_asks_through_an_input_dialog_and_returns_the_answer(self):
        ran = self.run_extension(ask={"question": "Which colour?", "options": ["red", 3], "multiSelect": True}, answers=["blue"])
        self.assertEqual(ran["ask"], {"content": [{"type": "text", "text": "blue"}], "details": {"answer": "blue"}})
        unanswered = self.run_extension(ask={"question": "Which colour?"}, answers=[None])["ask"]
        self.assertTrue(unanswered["isError"])


@unittest.skipUnless(shutil.which("node"), "needs Node 22.6 or newer on PATH, which runs the extension")
class PiExtensionProgramToolsTest(Extension):
    def setUp(self):
        super().setUp()
        self.url, self.calls = js_plugin_rig.route(self)

    def program(self, *names) -> dict:
        return {"url": self.url, "tools": [js_plugin_rig.tool(n) for n in names]}

    def test_each_program_tool_is_registered_under_its_plain_name_and_forwarded_to_the_route(self):
        ran = self.run_extension(program=self.program("lookup"), run={"lookup": {"q": "x"}})
        self.assertEqual(ran["tools"], ["lookup", "ask_user"])
        self.assertEqual(ran["registered"]["lookup"]["parameters"]["required"], ["q"], "the program's own schema")
        self.assertEqual(ran["ran"]["lookup"], {"content": [{"type": "text", "text": 'lookup {"q": "x"}'}], "details": {}, "isError": False})
        self.assertEqual([(c["method"], c["params"]) for c in self.calls], [("tools/call", {"name": "lookup", "arguments": {"q": "x"}})])

    def test_a_replaced_built_in_answers_in_the_built_ins_result_shape(self):
        ran = self.run_extension(program=self.program("read", "edit", "write", "failing"),
                                 run={"read": {"q": "a"}, "edit": {"q": "b"}, "write": {"q": "c"}, "failing": {"q": "d"}})
        self.assertEqual(ran["ran"]["read"]["details"], {})
        self.assertEqual(ran["ran"]["edit"]["details"], {"diff": "", "patch": ""})
        self.assertNotIn("details", ran["ran"]["write"], "write's details are undefined")
        self.assertEqual((ran["ran"]["failing"]["isError"], ran["ran"]["failing"]["content"][0]["text"]), (True, 'failing {"q": "d"}'))

    def test_a_program_tool_named_ask_user_takes_the_place_of_wefts(self):
        ran = self.run_extension(program=self.program("ask_user"), run={"ask_user": {"q": "?"}})
        self.assertEqual(ran["tools"], ["ask_user"])
        self.assertEqual(ran["registered"]["ask_user"]["description"], "ask_user of weft")
        self.assertEqual([c["params"]["name"] for c in self.calls], ["ask_user"])


if __name__ == "__main__":
    unittest.main()
