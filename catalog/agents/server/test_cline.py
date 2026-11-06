"""Cline CLI through its SDK.

Against the bridge's stand-in (`testing/fake_cline.py`, lines the real bridge
wrote on Cline 3.0.74): what `start` carries for every permission setting,
the model, the effort and the program's tools (a new one and one replacing
the built-in `read_files`), where the SDK is found, the private home of a
connection and the machine's own login, a turn streamed both ways,
reasoning, an approval allowed for the session and refused with a note,
the program's tools called by their plain names without an approval and
kept in the history as the program's, Cline's own `read_files` when the
program has none, a file Cline wrote, a question, a restart that resumes
the session, a session Cline no longer has, and a failed turn.

And when Cline's npm package is installed (`cline` on PATH, with its SDK),
the real bridge runs the real engine against a stand-in of an
OpenAI-compatible chat endpoint, which needs no account: the model is
offered `lookup` and the program's `read_files` by their plain names, both
reach weft, and Cline's `ask_question` reaches it as a question.

Run with the others: `python3 -m unittest test_cline`.
"""

import http.server
import json
import os
import pathlib
import shutil
import subprocess
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_cline  # noqa: E402
import events as ev  # noqa: E402

KEY = {"way": "providerKey", "values": {"key": "sk-test", "provider": "openrouter"}}
READ = [{"name": "read_files", "description": "Read a file of the program's", "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}]


def installed_cline():
    """The `cline` on PATH when it is Cline's npm package, with the SDK and a Node new enough for it."""
    found = shutil.which("cline")
    node = shutil.which("node")
    if found is None or node is None:
        return None
    try:
        adapter_cline.sdk_entry(found)
    except Exception:  # noqa: BLE001 (not that package: the test is skipped)
        return None
    version = subprocess.run([node, "--version"], capture_output=True, text=True).stdout.strip()
    return found if int(version.lstrip("v").split(".")[0]) >= 22 else None


CLINE = installed_cline()


class ClineTest(Rig):
    def setUp(self):
        super().setUp()
        # Cline's npm package, as the adapter finds the SDK in it: `cline` on
        # PATH is a link to the package's `bin/cline`.
        package = os.path.join(self.root, "cline-package")
        self.sdk = os.path.join(package, adapter_cline.SDK)
        os.makedirs(os.path.dirname(self.sdk))
        open(self.sdk, "w").close()
        os.makedirs(os.path.join(package, "bin"))
        shim(os.path.join(package, "bin"), "cline", "fake_cline.py")
        os.symlink(os.path.join(package, "bin", "cline"), os.path.join(self.root, "bin", "cline"))
        # The bridge runs on Node: the stand-in takes its place.
        shim(os.path.join(self.root, "bin"), "node", "fake_cline.py")

    def setup(self, **more):
        return self.configure("cline", **more)

    def sent(self, kind: str) -> list:
        return [m for m in self.logged() if m.get("type") == kind]

    def question(self, turn, after=0):
        pending = self.server.until(turn, ev.WAITING_QUESTION, after=after)
        return pending["pending"]["question"], pending["after"]

    def history(self, turn) -> list:
        return self.server.call("GET", f"/history?turn={turn}")[1]["messages"]

    def tool_calls(self, turn) -> list:
        return [(c["name"], json.loads(c["arguments"])) for m in self.history(turn) for c in m.get("tool_calls", [])]

    # ----- what reaches Cline -----------------------------------------------

    def test_start_carries_the_settings_and_the_programs_tools_by_their_plain_names(self):
        self.setup(model="openai/gpt-4o-mini", options={"effort": "high"})
        self.server.until(self.turn("hello", tools=LOOKUP + READ), ev.DONE)
        start = self.sent("start")[0]
        self.assertEqual(self.logged()[0]["argv"], [adapter_cline.BRIDGE, pathlib.Path(self.sdk).as_uri()])
        self.assertEqual({k: start[k] for k in ("cwd", "resume", "model", "mode", "autoApprove", "effort")},
                         {"cwd": self.folder, "resume": False, "model": "openai/gpt-4o-mini", "mode": "act", "autoApprove": False,
                          "effort": "high"})
        self.assertEqual(start["tools"], LOOKUP + READ, "the program's tools go over whole, under their own names")
        self.assertEqual(self.server.conversation.conversation_id(), start["session"], "the session this side minted is kept")

    def test_each_permission_setting_is_a_cline_mode(self):
        for permission, (mode, approve_all) in adapter_cline.PERMISSIONS.items():
            self.setup(permission=permission)
            self.server.until(self.turn("hello"), ev.DONE)
            start = self.sent("start")[-1]
            self.assertEqual((start["mode"], start["autoApprove"]), (mode, approve_all), permission)
        self.assertEqual(adapter_cline.PERMISSIONS, {"ask": ("act", False), "plan": ("plan", False), "bypass": ("act", True)})

    def test_a_bad_effort_and_a_key_for_a_provider_no_key_logs_in_to_are_refused_before_anything_starts(self):
        self.setup(options={"effort": "max"})
        self.assertIn("`effort` takes none, low, medium, high, xhigh, and got 'max'", self.server.until(self.turn("hello"), ev.FAILED)["error"])
        self.setup(credentials={"way": "providerKey", "values": {"key": "x", "provider": "claude-code"}})
        self.assertIn("Cline's provider 'claude-code' takes no API key", self.server.until(self.turn("hello"), ev.FAILED)["error"])
        self.assertFalse(os.path.exists(self.log))

    def test_a_cline_that_is_not_its_npm_package_is_refused_saying_why(self):
        os.remove(self.sdk)
        self.setup()
        error = self.server.until(self.turn("hello"), ev.FAILED)["error"]
        self.assertIn("is not Cline's npm package", error)
        self.assertIn("take this one off PATH", error)

    def test_a_key_runs_in_a_private_home_and_nothing_moves_its_folders_out(self):
        os.environ.update({name: f"/mine/{name}" for name in adapter_cline.MOVERS})
        os.environ["CLINE_PROVIDER"] = "anthropic"
        self.setup(credentials={"way": "clineKey", "values": {"key": "ck-test"}})
        self.server.until(self.turn("hello"), ev.DONE)
        home = os.path.join(self.home, "cline-home")
        self.assertEqual(self.logged()[0]["env"], {"CLINE_DIR": home, "CLINE_API_KEY": "ck-test"}, "Cline's own provider: no CLINE_PROVIDER")
        self.setup(credentials=KEY)
        self.server.until(self.turn("hello"), ev.DONE)
        starts = [m for m in self.logged() if "argv" in m]
        self.assertEqual(starts[-1]["env"], {"CLINE_DIR": home, "CLINE_API_KEY": "sk-test", "CLINE_PROVIDER": "openrouter"})

    def test_the_machines_own_login_is_the_persons_own_cline(self):
        mine = {"CLINE_DIR": os.path.join(self.root, "mine"), **{name: os.path.join(self.root, "mine", name) for name in adapter_cline.MOVERS}}
        os.environ.update(mine)
        os.environ["CLINE_API_KEY"] = "a key of the person's shell"
        self.setup()
        self.server.until(self.turn("hello", tools=LOOKUP), ev.DONE)
        self.assertEqual(self.logged()[0]["env"], mine, "its folders as they are, and no login of weft's")
        self.assertEqual(self.sent("start")[0]["model"], "", "no model: the person's own, else the provider's default")

    # ----- a turn ---------------------------------------------------------------

    def test_a_turn_streams_by_token_and_ends_with_the_reply(self):
        self.setup(streamBy="token")
        answer = self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(answer["reply"], "hello from cline")
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.TEXT_DELTA, "hello"), (ev.TEXT_DELTA, " from cline"), (ev.TEXT, "hello from cline"), (ev.DONE, None)])
        self.assertEqual(self.sent("prompt")[0]["text"], "hello")

    def test_by_message_only_whole_messages_go_out_and_reasoning_comes_first(self):
        self.setup()
        answer = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.THINKING, "let me think"), (ev.TEXT, "thought and answered"), (ev.DONE, None)])

    def test_a_failed_turn_says_why(self):
        self.setup()
        self.assertIn("Incorrect API key provided", self.server.until(self.turn("fail"), ev.FAILED)["error"])

    def test_a_command_allowed_for_the_session_is_asked_once(self):
        self.setup()
        turn = self.turn("runit")
        question, after = self.question(turn)
        permission = question["permission"]
        self.assertEqual((permission["tool"], permission["title"], permission["detail"]),
                         ("run_commands", "Cline CLI wants to use run_commands", "echo hi"))
        self.assertEqual([(c["id"], c["kind"]) for c in permission["choices"]],
                         [("allow_once", ev.ALLOW_ONCE), ("allow_always", ev.ALLOW_ALWAYS), ("reject", ev.REJECT)])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow_always"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual([(e["type"], e.get("name"), e.get("output")) for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT)],
                         [(ev.TOOL_USE, "run_commands", None), (ev.TOOL_RESULT, None, '[{"query": "echo hi", "result": "hi\\n", "success": true}]')])
        self.assertEqual(self.sent("answer")[0]["response"], {"approved": True})
        self.assertEqual(self.tool_calls(turn), [("run_commands", {"commands": ["echo hi"]})])
        again = self.server.until(self.turn("runit"), ev.DONE)
        self.assertNotIn(ev.QUESTION, [e["type"] for e in again["seen"]], "allowed for the rest of the session")
        self.assertEqual(self.sent("answer")[1]["response"], {"approved": True})

    def test_a_refused_command_carries_the_note_and_the_turn_goes_on(self):
        self.setup()
        turn = self.turn("runit")
        question, after = self.question(turn)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "reject", "note": "not now"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(self.sent("answer")[0]["response"],
                         {"approved": False, "reason": "This tool call was rejected by the user and not executed. not now"})
        result = [e for e in answer["seen"] if e["type"] == ev.TOOL_RESULT][0]
        self.assertTrue(result["isError"])
        self.assertIn("not executed. not now -- NOT a tool or system failure", result["output"])

    def test_bypass_asks_nothing(self):
        self.setup(permission="bypass")
        answer = self.server.until(self.turn("runit"), ev.DONE)
        self.assertNotIn(ev.QUESTION, [e["type"] for e in answer["seen"]])

    def test_a_file_cline_wrote_is_a_file_change_in_its_folder(self):
        self.setup(permission="bypass")
        answer = self.server.until(self.turn("editit"), ev.DONE)
        self.assertEqual([(e["path"], e["change"]) for e in answer["seen"] if e["type"] == ev.FILE_CHANGE],
                         [(os.path.join(self.folder, "new.txt"), "add")])
        self.assertEqual(adapter_cline.file_changes("apply_patch", {"input": "*** Begin Patch\n*** Update File: a.py\n@@\n*** Delete File: b.py\n"}),
                         [("a.py", "update"), ("b.py", "delete")])

    def test_a_question_takes_an_option_or_an_answer_of_ones_own(self):
        self.setup()
        turn = self.turn("askit")
        question, after = self.question(turn)
        self.assertEqual(question["kind"], ev.KIND_QUESTION)
        asked = question["questions"][0]
        self.assertEqual((asked["question"], [o["label"] for o in asked["options"]], asked["allowOther"], asked["multiSelect"]),
                         ("Which color?", ["red", "blue"], True, False))
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {asked["id"]: ["green, please"]}}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(self.sent("answer")[0]["response"], {"answer": "green, please"})
        self.assertEqual(answer["reply"], "tool said: green, please")

    # ----- the program's tools ------------------------------------------------------

    def test_the_programs_tool_is_called_by_its_plain_name_without_an_approval(self):
        self.setup()
        turn = self.turn("lookup", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "tool said: 42")
        self.assertEqual(self.sent("answer")[0]["response"], {"output": "42", "isError": False})
        self.assertEqual([e["type"] for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT, ev.QUESTION)], [],
                         "the call goes out as the program's tool call, never as Cline's own tool or a question")
        self.assertEqual(self.tool_calls(turn), [("lookup", {})])
        self.assertEqual([m["content"] for m in self.history(turn) if m["role"] == "tool"], ["42"])

    def test_a_program_tool_named_like_a_built_in_is_the_programs(self):
        self.setup()
        turn = self.turn("readit", tools=READ)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("read_files", {"files": [{"path": "a.txt"}]}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program's file", "isError": True}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(self.sent("answer")[0]["response"], {"output": "the program's file", "isError": True})
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]])
        self.assertEqual(self.tool_calls(turn), [("read_files", {"files": [{"path": "a.txt"}]})])

    def test_a_program_tools_name_run_by_something_else_of_clines_fails_the_turn_saying_what_to_do(self):
        self.setup()
        answer = self.server.until(self.turn("shadowed", tools=LOOKUP), ev.FAILED)
        self.assertIn("ran a tool of its own named `lookup` instead of the program's `lookup`", answer["error"])
        self.assertIn("Rename the program's tool, or remove or rename the Cline plugin that defines `lookup`", answer["error"])
        self.assertNotIn(ev.TOOL_RESULT, [e["type"] for e in answer["seen"]])

    def test_clines_own_built_in_stays_clines_when_the_program_has_no_tool_of_its_name(self):
        self.setup(permission="bypass")
        answer = self.server.until(self.turn("readit", tools=LOOKUP), ev.DONE)
        self.assertEqual([e["name"] for e in answer["seen"] if e["type"] == ev.TOOL_USE], ["read_files"])
        self.assertNotIn(ev.TOOL_CALL, [e["type"] for e in answer["seen"]])
        self.assertEqual(answer["reply"], "tool said: cline's own file")

    # ----- picking the conversation up again ------------------------------------------

    def test_a_restart_resumes_the_session_with_the_new_settings_and_tools(self):
        self.setup(credentials=KEY)
        self.server.until(self.turn("hello", tools=LOOKUP + READ), ev.DONE)
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.setup(permission="plan", credentials=KEY)
        self.assertEqual(self.server.until(self.turn("hello again", tools=LOOKUP), ev.DONE)["reply"], "hello from cline")
        start = self.sent("start")[1]
        self.assertEqual((start["session"], start["resume"], start["mode"], [t["name"] for t in start["tools"]]),
                         (session, True, "plan", ["lookup"]))

    def test_a_session_cline_no_longer_has_says_to_start_over(self):
        self.setup(credentials=KEY)
        self.server.until(self.turn("hello"), ev.DONE)
        session = self.server.conversation.conversation_id()
        shutil.rmtree(os.path.join(self.home, "cline-home", "data", "sessions"))
        self.server.close()
        self.server = self.start()
        self.setup(credentials=KEY)
        error = self.server.until(self.turn("hello"), ev.FAILED)["error"]
        self.assertIn(f"Cline has no conversation {session} any more", error)
        self.assertIn("start it over from the node's card", error)


# ----- the real engine -----------------------------------------------------------------

class FakeModel(http.server.BaseHTTPRequestHandler):
    """An OpenAI-compatible chat endpoint as the engine streams it: the first
    answer calls the tool the message names, the next one says what it gave."""

    offered = []

    def do_POST(self):  # noqa: N802 (the stdlib's spelling)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        FakeModel.offered.append([t["function"]["name"] for t in body.get("tools") or []])
        messages = body["messages"]
        # The person's message, and the tool's answer when one came after it
        # (Cline may add a note of its own after a tool's result).
        asked = max(i for i, m in enumerate(messages) if m["role"] == "user" and "please" in json.dumps(m["content"]))
        answered = [m for m in messages[asked:] if m["role"] == "tool"]
        if answered:
            chunks = [{"role": "assistant", "content": "got " + json.dumps(answered[-1]["content"])}]
            finish = "stop"
        else:
            said = json.dumps(messages[asked]["content"])
            name, args = (("read_files", {"files": [{"path": "a"}]}) if "read" in said else
                          ("ask_question", {"question": "Which?", "options": ["red", "blue"]}) if "ask" in said else ("lookup", {}))
            chunks = [{"role": "assistant", "tool_calls": [{"index": 0, "id": f"call_{name}", "type": "function",
                                                             "function": {"name": name, "arguments": json.dumps(args)}}]}]
            finish = "tool_calls"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for delta, reason in ((chunks[0], None), ({}, finish)):
            choice = {"index": 0, "delta": delta, "finish_reason": reason}
            self.wfile.write(f"data: {json.dumps({'id': 'c', 'object': 'chat.completion.chunk', 'model': 'm', 'choices': [choice]})}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")

    def log_message(self, *args):
        pass


@unittest.skipUnless(CLINE, "Cline's npm package (with its SDK, on Node 22 or newer) is not on PATH")
class RealEngineTest(Rig):
    def test_the_model_is_offered_the_programs_tools_by_their_plain_names_and_all_reach_weft(self):
        model = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeModel)
        threading.Thread(target=model.serve_forever, daemon=True).start()
        self.addCleanup(model.server_close)
        self.addCleanup(model.shutdown)
        self.configure("cline", model="m", credentials={"way": "providerKey", "values": {"key": "k", "provider": "openai-compatible"}},
                       program=CLINE)
        # Where the stand-in endpoint is, as `cline auth` keeps a provider's settings.
        settings = os.path.join(self.home, "cline-home", "data", "settings", "providers.json")
        os.makedirs(os.path.dirname(settings), exist_ok=True)
        with open(settings, "w") as f:
            json.dump({"version": 1, "lastUsedProvider": "openai-compatible", "modes": {}, "providers": {"openai-compatible": {
                "settings": {"provider": "openai-compatible", "baseUrl": f"http://127.0.0.1:{model.server_address[1]}/v1"},
                "tokenSource": "manual", "updatedAt": "2026-01-01T00:00:00.000Z"}}}, f)
        # A Cline plugin of the person's own with a tool of the program's tool's
        # name: Cline loads it, and the program's `lookup` still wins.
        plugins = os.path.join(self.home, "cline-home", "plugins")
        os.makedirs(plugins)
        with open(os.path.join(plugins, "mine.js"), "w") as f:
            f.write("export default { name: 'mine', manifest: { capabilities: ['tools'] }, setup(api) {\n"
                    "  for (const name of ['lookup', 'persons_own']) api.registerTool({ name, description: 'the plugin', "
                    "inputSchema: { type: 'object', properties: {} }, async execute() { return 'from the plugin'; } });\n} };\n")
        for message, name in (("lookup please", "lookup"), ("read please", "read_files")):
            turn = self.turn(message, tools=LOOKUP + READ)
            call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
            self.assertEqual(call["name"], name)
            self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": f"{name} from weft"}})
            self.assertEqual(self.server.until(turn, ev.DONE)["reply"], f'got "{name} from weft"')
        turn = self.turn("ask please", tools=LOOKUP + READ)
        pending = self.server.until(turn, ev.WAITING_QUESTION)["pending"]
        asked = pending["question"]["questions"][0]
        self.assertEqual((asked["question"], [o["label"] for o in asked["options"]]), ("Which?", ["red", "blue"]))
        self.server.post("/turns", {"answer": {"questionId": pending["question"]["id"], "answers": {asked["id"]: ["blue"]}}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], 'got "blue"')
        offered = FakeModel.offered[0]
        self.assertIn("lookup", offered)
        self.assertIn("persons_own", offered, "the plugin is loaded")
        self.assertEqual(offered.count("lookup"), 1)
        self.assertEqual(offered.count("read_files"), 1, "the program's read_files in place of Cline's")
        self.assertFalse([n for n in offered if "__" in n], "no prefixed name")


if __name__ == "__main__":
    unittest.main()
