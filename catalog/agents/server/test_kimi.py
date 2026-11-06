"""Kimi Code through its SDK.

Against the bridge's stand-in (`testing/fake_kimi.py`, lines the real bridge
wrote on Kimi Code 2.5.1): what `start` carries for every permission
setting, the model, the options and the program's tools (a new one and one
replacing the built-in `Read`), the private home and its `config.toml` with
a key, the machine's own login, a turn streamed both ways, reasoning, a
permission allowed and refused with a note, the program's tools approved
without a question, a call of the program's `lookup` and of its `Read` by
their plain names and kept in the history as the program's, a sub-agent's
call by the same route, a question, a restart that resumes the session, a
session Kimi no longer has, and a failed turn.

The bridge itself (`kimi_bridge.mjs`) runs on Node against a stand-in of
the SDK and its engine: which tools it registers and takes out (a built-in
put back), the name it gives a call of the program's tool, and its asks.
And when `kimi-code-sdk` is installed (`agents.json`, `kimi.install`), one
conversation runs on the real engine against a stand-in of Moonshot's chat
endpoint, which needs no account: the model is offered `lookup` and the
program's `Read` by their plain names, and both reach weft.

Run with the others: `python3 -m unittest test_kimi`.
"""

import http.server
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_kimi  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402

KEY = {"way": "apiKey", "values": {"key": "sk-test"}}
READ = [{"name": "Read", "description": "Read a file of the program's", "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}]
NODE = shutil.which("node")
# The real one, found the way the agent server finds it: on PATH (weft puts
# its install folder's `bin` there).
SDK = shutil.which(adapter_kimi.PROGRAM)


class KimiTest(Rig):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), adapter_kimi.PROGRAM, "fake_kimi.py")

    def setup(self, **more):
        return self.configure("kimi", **dict({"model": "kimi-k2-turbo-preview", "credentials": KEY}, **more))

    def sent(self, kind: str) -> list:
        return [m for m in self.logged() if m.get("type") == kind]

    def question(self, turn, after=0):
        pending = self.server.until(turn, ev.WAITING_QUESTION, after=after)
        return pending["pending"]["question"], pending["after"]

    def history(self, turn) -> list:
        return self.server.call("GET", f"/history?turn={turn}")[1]["messages"]

    def tool_calls(self, turn) -> list:
        return [(c["name"], json.loads(c["arguments"])) for m in self.history(turn) for c in m.get("tool_calls", [])]

    # ----- what reaches Kimi ------------------------------------------------

    def test_start_carries_the_settings_and_the_programs_tools_by_their_plain_names(self):
        self.setup(options={"thinking": "on", "addDirs": ["/x", "/y"]})
        self.server.until(self.turn("hello", tools=LOOKUP + READ), ev.DONE)
        start = self.sent("start")[0]
        self.assertEqual(self.logged()[0]["argv"], [adapter_kimi.BRIDGE])
        self.assertEqual({k: start[k] for k in ("workDir", "resume", "model", "thinking", "permission", "plan", "additionalDirs")},
                         {"workDir": self.folder, "resume": False, "model": "kimi-k2-turbo-preview", "thinking": "on",
                          "permission": "manual", "plan": False, "additionalDirs": ["/x", "/y"]})
        self.assertEqual(start["tools"], LOOKUP + READ, "the program's tools go over whole, under their own names")
        self.assertEqual(self.server.conversation.conversation_id(), start["session"], "the session this side minted is kept")

    def test_each_permission_setting_is_a_kimi_mode(self):
        for permission, (mode, plan) in adapter_kimi.PERMISSIONS.items():
            self.setup(permission=permission)
            self.server.until(self.turn("hello"), ev.DONE)
            start = self.sent("start")[-1]
            self.assertEqual((start["permission"], start["plan"]), (mode, plan), permission)
        self.assertEqual(adapter_kimi.PERMISSIONS, {"ask": ("manual", False), "plan": ("manual", True), "auto": ("yolo", False),
                                                    "bypass": ("auto", False)})

    def test_a_key_runs_in_a_private_home_whose_config_reads_it_from_the_environment(self):
        self.setup()
        self.server.until(self.turn("hello"), ev.DONE)
        home = os.path.join(self.home, "kimi-home")
        self.assertEqual(self.logged()[0]["env"], {"KIMI_CODE_HOME": home, "KIMI_API_KEY": "sk-test"})
        config = os.path.join(home, "config.toml")
        self.assertEqual(stat.S_IMODE(os.stat(config).st_mode), 0o600)
        with open(config) as f:
            text = f.read()
        self.assertIn('api_key_env = "KIMI_API_KEY"', text)
        self.assertIn('default_model = "kimi-k2-turbo-preview"', text)
        self.assertNotIn("sk-test", text, "the key stays in the environment")

    def test_the_machines_own_login_is_the_persons_own_kimi_home(self):
        os.environ["KIMI_CODE_HOME"] = os.path.join(self.root, "persons-kimi")
        self.configure("kimi")
        self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(self.logged()[0]["env"], {"KIMI_CODE_HOME": os.path.join(self.root, "persons-kimi")})
        self.assertEqual(self.sent("start")[0]["model"], "", "no model: Kimi's own default")

    def test_a_key_needs_a_model_and_thinking_takes_on_or_off(self):
        self.configure("kimi", credentials=KEY)
        self.assertIn("with a Moonshot API key needs `model`", self.server.until(self.turn("hello"), ev.FAILED)["error"])
        self.setup(options={"thinking": "max"})
        self.assertIn("`thinking` takes on, off, and got 'max'", self.server.until(self.turn("hello"), ev.FAILED)["error"])
        self.assertFalse(os.path.exists(self.log), "refused before anything starts")

    # ----- a turn ---------------------------------------------------------------

    def test_a_turn_streams_by_token_and_ends_with_the_reply(self):
        self.setup(streamBy="token")
        answer = self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(answer["reply"], "hello from kimi")
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.TEXT_DELTA, "hello"), (ev.TEXT_DELTA, " from kimi"), (ev.TEXT, "hello from kimi"), (ev.DONE, None)])
        self.assertEqual(self.sent("prompt")[0]["text"], "hello")

    def test_by_message_only_whole_messages_go_out_and_reasoning_comes_first(self):
        self.setup()
        answer = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.THINKING, "let me think"), (ev.TEXT, "thought and answered"), (ev.DONE, None)])

    def test_a_failed_turn_says_why(self):
        self.setup()
        error = self.server.until(self.turn("fail"), ev.FAILED)["error"]
        self.assertIn('declares api_key_env = "KIMI_API_KEY" in config.toml, but the environment variable is not set', error)

    def test_moonshots_refusal_fails_the_turn_with_its_words(self):
        self.setup()
        error = self.server.until(self.turn("broke"), ev.FAILED)["error"]
        self.assertEqual(error, "Your account org-0000 <ak-0000> is suspended due to insufficient balance, please recharge your "
                                "account or check your plan and billing details")

    def test_an_allowed_command_runs(self):
        self.setup()
        turn = self.turn("runit")
        question, after = self.question(turn)
        permission = question["permission"]
        self.assertEqual((permission["tool"], permission["title"], permission["detail"]), ("Bash", "Running: echo hi", "echo hi"))
        self.assertEqual([(c["id"], c["kind"]) for c in permission["choices"]],
                         [("approve_once", ev.ALLOW_ONCE), ("approve_always", ev.ALLOW_ALWAYS), ("reject", ev.REJECT)])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "approve_always"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual([(e["type"], e.get("name"), e.get("output")) for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT)],
                         [(ev.TOOL_USE, "Bash", None), (ev.TOOL_RESULT, None, "hi\n")])
        self.assertEqual(self.sent("answer")[0]["response"], {"decision": "approved", "scope": "session"})
        self.assertEqual(self.tool_calls(turn), [("Bash", {"command": "echo hi", "description": "say hi"})])

    def test_a_refused_command_carries_the_note_and_the_turn_goes_on(self):
        self.setup()
        turn = self.turn("runit")
        question, after = self.question(turn)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "reject", "note": "not now"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(self.sent("answer")[0]["response"], {"decision": "rejected", "feedback": "not now"})
        result = [e for e in answer["seen"] if e["type"] == ev.TOOL_RESULT][0]
        self.assertTrue(result["isError"])
        self.assertIn("Reason: not now", result["output"])

    def test_a_question_takes_an_option_or_an_answer_of_ones_own(self):
        self.setup()
        turn = self.turn("askit")
        question, after = self.question(turn)
        self.assertEqual(question["kind"], ev.KIND_QUESTION)
        asked = question["questions"][0]
        self.assertEqual((asked["header"], asked["question"], [o["label"] for o in asked["options"]], asked["allowOther"]),
                         ("Color", "Which color?", ["red", "blue"], True))
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {asked["id"]: ["green, please"]}}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(self.sent("answer")[0]["response"], {"answers": {"Which color?": "green, please"}})
        self.assertIn("green, please", answer["reply"])

    # ----- the program's tools ------------------------------------------------------

    def test_the_programs_tool_is_approved_and_called_by_its_plain_name(self):
        self.setup()
        turn = self.turn("lookup", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {"word": "x"}))
        self.assertEqual(self.sent("answer")[0]["response"], {"decision": "approved"}, "no question: the program handed it over")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "tool said: 42")
        self.assertEqual(self.sent("answer")[1]["response"], {"output": "42", "isError": False})
        self.assertEqual([e["type"] for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.QUESTION)], [],
                         "the call goes out as the program's tool call, never as Kimi's own tool or a question")
        self.assertEqual(self.tool_calls(turn), [("lookup", {"word": "x"})])
        self.assertEqual([m["content"] for m in self.history(turn) if m["role"] == "tool"], ["42"])

    def test_a_program_tool_named_like_a_built_in_is_the_programs(self):
        self.setup()
        turn = self.turn("readit", tools=READ)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("Read", {"path": "a.txt"}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program's file"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]])
        self.assertEqual(self.tool_calls(turn), [("Read", {"path": "a.txt"})])

    def test_kimis_own_built_in_stays_kimis_when_the_program_has_no_tool_of_its_name(self):
        self.setup(permission="bypass")
        answer = self.server.until(self.turn("runit", tools=READ), ev.DONE)
        self.assertEqual([e["name"] for e in answer["seen"] if e["type"] == ev.TOOL_USE], ["Bash"])
        self.assertNotIn(ev.TOOL_CALL, [e["type"] for e in answer["seen"]])

    def test_a_sub_agents_call_reaches_weft_by_its_route(self):
        self.setup()
        turn = self.turn("subagent", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual(call["name"], "lookup")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "7"}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "the sub-agent got: 7")
        self.assertEqual(self.tool_calls(turn), [("lookup", {"word": "y"})], "the route writes the history, whoever called")

    # ----- picking the conversation up again ------------------------------------------

    def test_a_restart_resumes_the_session_with_the_new_settings_and_tools(self):
        self.setup()
        self.server.until(self.turn("hello", tools=LOOKUP + READ), ev.DONE)
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.setup(permission="plan")
        self.assertEqual(self.server.until(self.turn("hello again", tools=LOOKUP), ev.DONE)["reply"], "hello from kimi")
        start = self.sent("start")[1]
        self.assertEqual((start["session"], start["resume"], start["permission"], start["plan"], [t["name"] for t in start["tools"]]),
                         (session, True, "manual", True, ["lookup"]))

    def test_a_session_kimi_no_longer_has_says_to_start_over(self):
        self.setup()
        self.server.until(self.turn("hello"), ev.DONE)
        session = self.server.conversation.conversation_id()
        shutil.rmtree(os.path.join(self.home, "kimi-home", "sessions"))
        self.server.close()
        self.server = self.start()
        self.setup()
        error = self.server.until(self.turn("hello"), ev.FAILED)["error"]
        self.assertIn(f'Session "{session}" was not found', error)
        self.assertIn("start it over from the node's card", error)


# ----- the bridge, on Node --------------------------------------------------------------

# A stand-in of the SDK and its engine, just what the bridge reaches: a
# registry of one tool per name, the user tools on it, a profile's active
# tools, the activation that registers a built-in where its name is free and
# active, and the pending calls of user tools.
STAND_IN = textwrap.dedent("""
    import { main } from %(bridge)s;
    const log = (m) => process.stderr.write(JSON.stringify(m) + "\\n");
    const registry = new Map([["Read", "builtin"], ["Bash", "builtin"]]);
    const builtIns = ["Read", "Bash"];
    const active = new Set(builtIns);
    let persisted = JSON.parse(process.env.PERSISTED || "[]");
    for (const name of persisted) registry.set(name, "user"), active.add(name);
    const pending = [];
    const ids = { tools: {}, profile: {}, registry: {}, activation: {}, lifecycle: {} };
    const services = new Map([
      [ids.tools, { list: () => persisted.map((name) => ({ name })),
                    register: (t) => { registry.set(t.name, "user"); active.add(t.name); persisted = [...new Set([...persisted, t.name])]; },
                    unregister: (name) => { registry.delete(name); active.delete(name); persisted = persisted.filter((n) => n !== name); } }],
      [ids.profile, { addActiveTool: (n) => active.add(n), removeActiveTool: (n) => active.delete(n) }],
      [ids.registry, { resolve: (n) => registry.get(n),
                       list: () => [...registry].map(([name, source]) => ({ name, source })).filter((t) => active.has(t.name)) }],
      [ids.activation, { activate: async () => { for (const n of builtIns) if (!registry.has(n) && active.has(n)) registry.set(n, "builtin"); } }],
    ]);
    const agent = { accessor: { get: (id) => services.get(id) } };
    const engine = {
      MAIN_AGENT_ID: "main", INTERACTION_TAG_TOOL_CALL_ID: "toolCallId",
      IAgentUserToolService: ids.tools, IAgentProfileService: ids.profile, IAgentToolRegistryService: ids.registry,
      IAgentToolActivationService: ids.activation, IAgentLifecycleService: ids.lifecycle,
      getLiveSessionById: () => ({ accessor: { get: () => ({ handleOf: () => agent }) } }),
      interactions: { findAll: (q) => pending.filter((p) => p.tags.toolCallId === q.tags.toolCallId && q.kind === "user_tool") },
    };
    let client;
    class SDKRpcClientV2 { constructor(o) { client = this; log({ options: o }); } suppressEngineSessionStarted() {} }
    const session = {
      id: "s-1", onEvent: (f) => { session.emit = f; },
      setApprovalHandler: (f) => { session.approve = f; }, setQuestionHandler: (f) => { session.ask = f; },
      setPermission: async (m) => log({ setPermission: m }), setPlanMode: async (p) => log({ setPlanMode: p }),
      setModel: async (m) => log({ setModel: m }), setThinking: async (t) => log({ setThinking: t }),
      prompt: async (text) => {
        session.emit({ type: "turn.started", agentId: "main", turnId: 0 });
        session.emit({ type: "assistant.delta", agentId: "sub-1", turnId: 0, delta: "a sub-agent's" });
        pending.push({ payload: { name: "Read" }, tags: { toolCallId: "c-1" } });
        const result = await client.toolCall({ toolCallId: "c-1", args: { path: "a" } });
        const approved = await session.approve({ toolName: "Bash", action: "Run" });
        const unknown = await client.toolCall({ toolCallId: "c-unknown", args: {} });
        log({ result, approved, unknown });
      },
    };
    class KimiHarness {
      constructor(rpc, o) { log({ harness: Object.keys(o).sort() }); }
      async createSession(o) { log({ createSession: o }); return session; }
      async resumeSession(o) { log({ resumeSession: o }); return session; }
      async close() {}
    }
    await main({ SDKRpcClientV2, KimiHarness }, engine);
""")


@unittest.skipUnless(NODE, "the bridge runs on Node, which is not installed")
class BridgeTest(unittest.TestCase):
    def run_bridge(self, lines, persisted=()):
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        driver = os.path.join(folder, "driver.mjs")
        with open(driver, "w") as f:
            f.write(STAND_IN % {"bridge": json.dumps("file://" + adapter_kimi.BRIDGE)})
        proc = subprocess.Popen([NODE, driver], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                env=dict(os.environ, PERSISTED=json.dumps(list(persisted))))
        written = []
        for line in lines:
            proc.stdin.write(json.dumps(line) + "\n")
            proc.stdin.flush()
            while True:
                message = json.loads(proc.stdout.readline())
                written.append(message)
                if message.get("type") == "ask":
                    proc.stdin.write(json.dumps({"type": "answer", "ask": message["ask"], "response": {"answered": message["kind"]}}) + "\n")
                    proc.stdin.flush()
                    continue
                if message.get("id") == line.get("id"):
                    break
        out, err = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, err)
        return written, [json.loads(line) for line in err.splitlines()]

    def start(self, tools, resume=False):
        return {"id": 1, "type": "start", "workDir": "/w", "session": "s-1", "resume": resume, "model": "m", "thinking": "on",
                "permission": "manual", "plan": True, "additionalDirs": ["/x"], "tools": tools}

    def test_it_registers_the_programs_tools_over_the_built_ins_of_their_names(self):
        written, logged = self.run_bridge([self.start(LOOKUP + READ), {"id": 2, "type": "tools"}])
        self.assertEqual(written[0], {"id": 1, "result": {"session": "s-1"}})
        self.assertEqual(sorted(map(tuple, (t.values() for t in written[1]["result"]["tools"]))),
                         [("Bash", "builtin"), ("Read", "user"), ("lookup", "user")])
        self.assertEqual(logged[0], {"options": {"identity": {"productName": "weft", "version": "1", "platform": "weft"}}})
        self.assertEqual(logged[2], {"createSession": {"id": "s-1", "workDir": "/w", "additionalDirs": ["/x"], "model": "m", "thinking": "on",
                                                       "permission": "manual", "planMode": True}})

    def test_a_resumed_session_gets_its_settings_again_and_a_built_in_back(self):
        written, logged = self.run_bridge([self.start(LOOKUP, resume=True), {"id": 2, "type": "tools"}], persisted=["lookup", "Read", "gone"])
        self.assertEqual(sorted(map(tuple, (t.values() for t in written[1]["result"]["tools"]))),
                         [("Bash", "builtin"), ("Read", "builtin"), ("lookup", "user")])
        self.assertEqual(logged[2:7], [{"resumeSession": {"id": "s-1", "workDir": "/w", "additionalDirs": ["/x"]}},
                                       {"setPermission": "manual"}, {"setPlanMode": True}, {"setModel": "m"}, {"setThinking": "on"}])

    def test_a_call_of_the_programs_tool_is_asked_by_its_name_and_only_the_main_agents_events_go_out(self):
        written, logged = self.run_bridge([self.start(READ), {"id": 2, "type": "prompt", "text": "go"}])
        asks = [m for m in written if m.get("type") == "ask"]
        self.assertEqual([(a["kind"], a["request"]) for a in asks],
                         [("tool", {"toolCallId": "c-1", "name": "Read", "args": {"path": "a"}}), ("approval", {"toolName": "Bash", "action": "Run"})])
        self.assertEqual([m["event"]["type"] for m in written if m.get("type") == "event"], ["turn.started"])
        self.assertEqual(logged[-1]["result"], {"answered": "tool"})
        self.assertEqual(logged[-1]["approved"], {"answered": "approval"})
        self.assertTrue(logged[-1]["unknown"]["isError"])
        self.assertIn("found 0 pending calls for tool call c-unknown", logged[-1]["unknown"]["output"])


# ----- the real engine -----------------------------------------------------------------

class FakeModel(http.server.BaseHTTPRequestHandler):
    """Moonshot's chat endpoint as the engine streams it: the first answer
    calls the tool the message names, the next one says what it gave."""

    offered = []

    def do_POST(self):  # noqa: N802 (the stdlib's spelling)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        FakeModel.offered.append([t["function"]["name"] for t in body.get("tools") or []])
        last = body["messages"][-1]
        if last["role"] == "tool":
            delta = {"role": "assistant", "content": "got " + json.dumps(last["content"])}
            chunks = [delta, None]
        else:
            name = "Read" if "read" in json.dumps(last["content"]) else "lookup"
            chunks = [{"role": "assistant", "tool_calls": [{"index": 0, "id": f"call_{name}", "type": "function",
                                                             "function": {"name": name, "arguments": "{}"}}]}, None]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for delta in chunks:
            choice = {"index": 0, "delta": delta or {}, "finish_reason": None if delta else ("stop" if last["role"] == "tool" else "tool_calls")}
            self.wfile.write(f"data: {json.dumps({'id': 'c', 'object': 'chat.completion.chunk', 'choices': [choice]})}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")

    def log_message(self, *args):
        pass


@unittest.skipUnless(SDK, f"Kimi Code's SDK (`{adapter_kimi.PROGRAM}`) is not on PATH; agents.json's kimi.install builds it, into a folder whose `bin` goes on PATH")
class RealEngineTest(Rig):
    def test_the_model_is_offered_the_programs_tools_by_their_plain_names_and_both_reach_weft(self):
        model = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeModel)
        threading.Thread(target=model.serve_forever, daemon=True).start()
        self.addCleanup(model.server_close)
        self.addCleanup(model.shutdown)
        written = adapter_kimi.kimi_config
        url = f"http://127.0.0.1:{model.server_address[1]}/v1"
        adapter_kimi.kimi_config = lambda name: written(name).replace('type = "kimi"', f'type = "kimi"\nbase_url = "{url}"')
        self.addCleanup(setattr, adapter_kimi, "kimi_config", written)
        self.configure("kimi", model="kimi-k2-turbo-preview", credentials=KEY, program=SDK)
        for message, name in (("lookup please", "lookup"), ("read please", "Read")):
            turn = self.turn(message, tools=LOOKUP + READ)
            call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
            self.assertEqual(call["name"], name)
            self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": f"{name} from weft"}})
            self.assertEqual(self.server.until(turn, ev.DONE)["reply"], f'got "{name} from weft"')
        offered = FakeModel.offered[0]
        self.assertIn("lookup", offered)
        self.assertEqual(offered.count("Read"), 1, "the program's Read in place of Kimi's")
        self.assertFalse([n for n in offered if n.startswith("mcp__")])


if __name__ == "__main__":
    unittest.main()
