"""Google Antigravity through its SDK.

Against the bridge's stand-in (`testing/fake_antigravity.py`, lines the real
bridge wrote on google-antigravity 0.1.23): what `start` carries (the
settings, the program's tools, the built-ins left on when a program tool
takes a built-in's name by either of its names, the key's variable), the
names refused before anything starts, the private home with a key and the
person's own environment without one, a turn streamed both ways, thinking,
each permission setting (what asks and what goes ahead, an allowed command,
a refused one with its note), the program's tools going ahead without a
question and called by their plain names (a new one, and one that replaced
the built-in `view_file`), kept in the history as the program's, a
question answered with an option or an answer of one's own, a restart that
resumes the conversation, a conversation the harness no longer has, and a
failed turn.

And when `antigravity-sdk` is installed (`agents.json`, `antigravity.install`),
conversations run on the real SDK and harness against a stand-in of Gemini's
API (`testing/gemini_standin.py`), which needs no account: the model is
offered the program's tools by their plain names, the program's `view_file`
in the built-in's place and its `edit_file` with the built-in of that SDK
name turned off, the calls reach weft by the route, a permission and a
question go through, and a new process picks the conversation up again.

Run with the others: `python3 -m unittest test_antigravity`.
"""

import json
import os
import shutil
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_antigravity  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402
import gemini_standin  # noqa: E402

KEY = {"way": "apiKey", "values": {"key": "AIza-test"}}
VERTEX_KEY = {"way": "vertexKey", "values": {"key": "AQ-test"}}
VIEW_FILE = [{"name": "view_file", "description": "Read a file of the program's",
              "parameters": {"type": "object", "properties": {"AbsolutePath": {"type": "string"}}}}]
EDIT_FILE = [{"name": "edit_file", "description": "The program's own editor", "parameters": {"type": "object", "properties": {}}}]
# The real one, found the way the agent server finds it: on PATH (weft puts
# its install folder's `bin` there).
SDK = shutil.which(adapter_antigravity.PROGRAM)


class Harness(Rig):
    """What both the stand-in's tests and the real SDK's read."""

    def setup(self, **more):
        return self.configure("antigravity", **dict({"credentials": KEY}, **more))

    def question(self, turn, after=0):
        pending = self.server.until(turn, ev.WAITING_QUESTION, after=after)
        return pending["pending"]["question"], pending["after"]

    def history(self, turn) -> list:
        return self.server.call("GET", f"/history?turn={turn}")[1]["messages"]

    def tool_calls(self, turn) -> list:
        return [(c["name"], json.loads(c["arguments"])) for m in self.history(turn) for c in m.get("tool_calls", [])]


class AntigravityTest(Harness):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), adapter_antigravity.PROGRAM, "fake_antigravity.py")
        with open(os.path.join(self.folder, "a.txt"), "w") as f:
            f.write("hi there\n")

    def sent(self, kind: str) -> list:
        return [m for m in self.logged() if m.get("type") == kind]

    def answers(self) -> list:
        return [m["response"] for m in self.sent("answer")]

    # ----- what reaches the bridge ------------------------------------------

    def test_start_carries_the_settings_and_the_programs_tools_by_their_plain_names(self):
        self.setup(model="gemini-3.1-pro", options={"effort": "high", "appendSystemPrompt": "be brief", "addDirs": ["/x", "/y"]})
        self.server.until(self.turn("hello", tools=LOOKUP + VIEW_FILE), ev.DONE)
        start = self.sent("start")[0]
        self.assertEqual(self.logged()[0]["argv"], [adapter_antigravity.BRIDGE])
        self.assertEqual({k: start[k] for k in ("workDir", "dirs", "resume", "model", "effort", "systemPrompt", "key")},
                         {"workDir": self.folder, "dirs": ["/x", "/y"], "resume": False, "model": "gemini-3.1-pro", "effort": "high",
                          "systemPrompt": "be brief", "key": {"env": "GEMINI_API_KEY", "vertex": False}})
        self.assertEqual(start["tools"], LOOKUP + VIEW_FILE, "the program's tools go over whole, under their own names")
        self.assertEqual(start["saveDir"], os.path.join(self.home, adapter_antigravity.SAVE_DIR))
        self.assertEqual(self.server.conversation.conversation_id(), start["conversation"], "the conversation this side minted is kept")
        self.assertRegex(start["conversation"], r"^[0-9a-f]{32}$")

    def test_a_program_tool_named_like_a_built_in_by_either_name_turns_that_built_in_off(self):
        self.setup()
        named = [{"name": n, "description": "", "parameters": {"type": "object", "properties": {}}}
                 for n in ("view_file", "write_to_file", "edit_file", "lookup")]
        self.server.until(self.turn("hello", tools=named), ev.DONE)
        enabled = self.sent("start")[0]["enabled"]
        self.assertEqual(sorted(set(adapter_antigravity.BUILT_IN) - set(enabled)), ["create_file", "edit_file", "view_file"],
                         "`view_file` by both names, `create_file` by the model's (`write_to_file`), `edit_file` by the SDK's")
        self.server.until(self.turn("hello", tools=LOOKUP), ev.DONE)
        self.assertEqual(self.sent("start")[1]["enabled"], list(adapter_antigravity.BUILT_IN), "every built-in back once freed")

    def test_the_names_the_node_refuses_are_never_ones_antigravity_can_turn_off(self):
        nodes = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(nodes, "google_antigravity", "metadata.json"), encoding="utf-8") as f:
            reserved = json.load(f)["lists"]["reservedToolNames"]
        built_in = set(adapter_antigravity.BUILT_IN) | set(adapter_antigravity.BUILT_IN.values())
        self.assertEqual(set(reserved) & built_in, set(), "a built-in it can turn off is never refused")

    def test_a_bad_option_is_refused_before_anything_starts(self):
        self.setup(options={"effort": "max"})
        self.assertIn("`effort` takes minimal, low, medium, high, extra_high, and got 'max'",
                      self.server.until(self.turn("hello"), ev.FAILED)["error"])
        self.assertFalse(os.path.exists(self.log), "refused before anything starts")

    def test_a_key_runs_in_a_private_home_with_only_its_own_variable(self):
        os.environ.update(GOOGLE_GENAI_USE_VERTEXAI="true", GOOGLE_CLOUD_PROJECT="mine", GEMINI_API_KEY="the-persons-own")
        self.setup(credentials=VERTEX_KEY)
        self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(self.logged()[0]["env"], {"HOME": os.path.join(self.home, "antigravity-home"), "GOOGLE_API_KEY": "AQ-test"})
        self.assertEqual(self.sent("start")[0]["key"], {"env": "GOOGLE_API_KEY", "vertex": True})
        self.assertNotIn("AQ-test", json.dumps(self.sent("start")), "the key stays in the environment")

    def test_the_machines_own_login_is_the_persons_own_environment(self):
        os.environ.update(GEMINI_API_KEY="the-persons-own", GOOGLE_CLOUD_PROJECT="mine")
        self.setup(credentials=None)
        self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(self.logged()[0]["env"], {"HOME": os.environ["HOME"], "GEMINI_API_KEY": "the-persons-own",
                                                   "GOOGLE_CLOUD_PROJECT": "mine"})
        self.assertIsNone(self.sent("start")[0]["key"])

    # ----- a turn ---------------------------------------------------------------

    def test_a_turn_streams_by_token_and_ends_with_the_reply(self):
        self.setup(streamBy="token")
        answer = self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(answer["reply"], "hello from antigravity")
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.TEXT_DELTA, "hello"), (ev.TEXT_DELTA, " from"), (ev.TEXT_DELTA, " antigravity"),
                          (ev.TEXT, "hello from antigravity"), (ev.DONE, None)])
        self.assertEqual(self.sent("prompt")[0]["text"], "hello")

    def test_by_message_only_whole_messages_go_out_and_thinking_comes_first(self):
        self.setup()
        answer = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.THINKING, "let me think"), (ev.TEXT, "thought and answered"), (ev.DONE, None)])

    def test_a_failed_turn_says_why(self):
        self.setup()
        error = self.server.until(self.turn("fail"), ev.FAILED)["error"]
        self.assertIn("Google Antigravity could not run the turn: Agent execution terminated due to error.", error)
        self.assertIn("API key not valid", error)

    # ----- permissions ----------------------------------------------------------------

    def test_each_permission_setting_asks_for_what_it_does_not_let_go_ahead(self):
        names = ("view_file", "ask_question", "create_file", "edit_file", "run_command", "search_web", "invoke_subagent")
        for permission, asks in (("ask", {"create_file", "edit_file", "run_command", "search_web", "invoke_subagent"}),
                                 ("accept_edits", {"run_command", "search_web", "invoke_subagent"}),
                                 ("bypass", set())):
            free = adapter_antigravity.PERMISSIONS[permission]
            self.assertEqual({n for n in names if free is not None and n not in free}, asks, permission)

    def test_an_allowed_command_runs_and_shows_as_its_own_tool(self):
        self.setup()
        turn = self.turn("runit")
        question, after = self.question(turn)
        permission = question["permission"]
        self.assertEqual((permission["tool"], permission["title"], permission["detail"]),
                         ("run_command", "Google Antigravity wants to run a command", "echo hi"))
        self.assertEqual([(c["id"], c["kind"]) for c in permission["choices"]], [("allow", ev.ALLOW_ONCE), ("reject", ev.REJECT)])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual([(e["type"], e.get("name"), e.get("output")) for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT)],
                         [(ev.TOOL_USE, "run_command", None), (ev.TOOL_RESULT, None, "hi\r\n")])
        self.assertEqual(self.answers()[0], {"allow": True})
        self.assertEqual(self.tool_calls(turn), [("run_command", {"CommandLine": "echo hi", "Cwd": self.folder, "WaitMsBeforeAsync": 5000})])

    def test_a_refused_command_carries_the_note_and_the_turn_goes_on(self):
        self.setup()
        turn = self.turn("runit")
        question, after = self.question(turn)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "reject", "note": "not now"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual(self.answers()[0], {"allow": False, "message": "not now"})
        self.assertIn("denied by pre-tool hook: not now", answer["reply"])
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]], "a refused call never ran")

    def test_a_file_change_goes_ahead_under_accept_edits_and_says_what_changed(self):
        self.setup(permission="accept_edits")
        answer = self.server.until(self.turn("writeit"), ev.DONE)
        self.assertEqual(self.answers()[0], {"allow": True}, "no question")
        self.assertEqual([(e["type"], e.get("path"), e.get("change")) for e in answer["seen"] if e["type"] == ev.FILE_CHANGE],
                         [(ev.FILE_CHANGE, os.path.join(self.folder, "new.txt"), "add")])

    # ----- questions -------------------------------------------------------------------

    def test_a_question_takes_an_option_or_an_answer_of_ones_own(self):
        for given, picked, said in ((["blue"], {"selectedOptionIds": ["2"], "freeformResponse": "", "skipped": False}, "A1: blue"),
                                    (["green, please"], {"selectedOptionIds": [], "freeformResponse": "green, please", "skipped": False},
                                     "A1: green, please")):
            self.setup()
            turn = self.turn("askit")
            question, after = self.question(turn)
            self.assertEqual(question["kind"], ev.KIND_QUESTION)
            asked = question["questions"][0]
            self.assertEqual((asked["question"], [o["label"] for o in asked["options"]], asked["multiSelect"], asked["allowOther"]),
                             ("Which color?", ["red", "blue"], False, True))
            self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {asked["id"]: given}}})
            answer = self.server.until(turn, ev.DONE, after=after)
            self.assertEqual(self.answers()[-1], {"responses": [picked]})
            self.assertIn(said, answer["reply"])
            self.assertEqual(self.answers()[-2], {"allow": True}, "its question tool goes ahead: it is itself a question")

    # ----- the program's tools ------------------------------------------------------

    def test_the_programs_tool_goes_ahead_and_is_called_by_its_plain_name(self):
        self.setup()
        turn = self.turn("lookup", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {"word": "x"}))
        self.assertEqual(self.answers()[0], {"allow": True}, "no question: the program handed it over")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertIn('{\\"result\\": \\"42\\"}', answer["reply"])
        self.assertEqual(self.answers()[1], {"output": "42", "isError": False})
        self.assertEqual([e["type"] for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT, ev.QUESTION)], [],
                         "the call goes out as the program's tool call, never as Antigravity's own tool or a question")
        self.assertEqual(self.tool_calls(turn), [("lookup", {"word": "x"})])
        self.assertEqual([m["content"] for m in self.history(turn) if m["role"] == "tool"], ["42"])

    def test_a_program_tool_named_like_a_built_in_is_the_programs(self):
        self.setup()
        turn = self.turn("readit", tools=VIEW_FILE)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("view_file", {"AbsolutePath": os.path.join(self.folder, "a.txt")}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program's file"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual([e["type"] for e in answer["seen"] if e["type"] in (ev.TOOL_USE, ev.TOOL_RESULT)], [],
                         "its second post-tool line (the call's summary) is nothing of the stream either")
        self.assertEqual(self.tool_calls(turn), [("view_file", {"AbsolutePath": os.path.join(self.folder, "a.txt")})])
        self.assertEqual([m["content"] for m in self.history(turn) if m["role"] == "tool"], ["the program's file"])
        self.assertNotIn("view_file", self.sent("start")[0]["enabled"])

    def test_antigravitys_own_built_in_stays_its_own_when_the_program_has_no_tool_of_its_name(self):
        self.setup(permission="bypass")
        answer = self.server.until(self.turn("runit", tools=VIEW_FILE), ev.DONE)
        self.assertEqual([e["name"] for e in answer["seen"] if e["type"] == ev.TOOL_USE], ["run_command"])
        self.assertNotIn(ev.TOOL_CALL, [e["type"] for e in answer["seen"]])

    # ----- picking the conversation up again ------------------------------------------

    def test_a_restart_resumes_the_conversation_with_the_new_settings_and_tools(self):
        self.setup()
        self.server.until(self.turn("hello", tools=LOOKUP + VIEW_FILE), ev.DONE)
        conversation = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.setup(options={"effort": "low"})
        self.assertEqual(self.server.until(self.turn("hello again", tools=LOOKUP), ev.DONE)["reply"], "hello from antigravity")
        start = self.sent("start")[1]
        self.assertEqual((start["conversation"], start["resume"], start["effort"], [t["name"] for t in start["tools"]]),
                         (conversation, True, "low", ["lookup"]))
        self.assertIn("view_file", start["enabled"], "the built-in is back once the program drops its tool")

    def test_a_conversation_the_harness_no_longer_has_says_to_start_over(self):
        self.setup()
        self.server.until(self.turn("hello"), ev.DONE)
        conversation = self.server.conversation.conversation_id()
        shutil.rmtree(os.path.join(self.home, adapter_antigravity.SAVE_DIR))
        self.server.close()
        self.server = self.start()
        self.setup()
        error = self.server.until(self.turn("hello"), ev.FAILED)["error"]
        self.assertIn(f'conversation "{conversation}" not found', error)
        self.assertIn("start it over from the node's card", error)


# ----- the real SDK ------------------------------------------------------------------

@unittest.skipUnless(SDK, f"Google Antigravity's SDK (`{adapter_antigravity.PROGRAM}`) is not on PATH; agents.json's antigravity.install sets it up, in a folder whose `bin` goes on PATH")
class RealSdkTest(Harness):
    """The real bridge, SDK and harness, against the stand-in of Gemini's API."""

    def setUp(self):
        super().setUp()
        with open(os.path.join(self.folder, "a.txt"), "w") as f:
            f.write("hi there\n")
        self.requests = os.path.join(self.root, "gemini.log")
        model = gemini_standin.serve(0, self.requests, self.folder)
        threading.Thread(target=model.serve_forever, daemon=True).start()
        self.addCleanup(model.server_close)
        self.addCleanup(model.shutdown)
        url = f"http://127.0.0.1:{model.server_address[1]}"
        written = adapter_antigravity.Antigravity.start_message
        adapter_antigravity.Antigravity.start_message = lambda adapter, *a: dict(written(adapter, *a), baseUrl=url)
        self.addCleanup(setattr, adapter_antigravity.Antigravity, "start_message", written)

    def offered(self) -> list:
        """The tools the model was offered in its first request: (name, description)."""
        with open(self.requests) as f:
            body = json.loads(f.readline())["body"]
        return [(d["name"], d.get("description", "")) for t in body.get("tools") or [] for d in t.get("functionDeclarations") or []]

    def test_the_programs_tools_reach_the_model_by_their_plain_names_and_reach_weft_by_the_route(self):
        self.configure("antigravity", program=SDK, credentials=None, permission="ask")
        tools = LOOKUP + VIEW_FILE + EDIT_FILE
        for message, name, args in (("lookup please", "lookup", {"word": "x"}),
                                    ("readit", "view_file", {"AbsolutePath": os.path.join(self.folder, "a.txt")})):
            turn = self.turn(message, tools=tools)
            call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
            self.assertEqual((call["name"], json.loads(call["arguments"])), (name, args))
            self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": f"{name} from weft"}})
            self.assertIn(f"{name} from weft", self.server.until(turn, ev.DONE)["reply"])
            self.assertEqual(self.tool_calls(turn)[-1], (name, args), "the history has it as the program's")
        self.assertEqual([n for n, _ in self.tool_calls(turn)], ["lookup", "view_file"], "and nothing of it as Antigravity's own")
        offered = dict(self.offered())
        self.assertEqual([n for n, _ in self.offered()].count("view_file"), 1)
        self.assertEqual(offered["view_file"], "Read a file of the program's", "the program's view_file in the built-in's place")
        self.assertEqual(offered["edit_file"], "The program's own editor")
        self.assertNotIn("replace_file_content", offered, "the built-in the SDK names edit_file is turned off")
        self.assertIn("lookup", offered)
        self.assertFalse([n for n in offered if n.startswith("mcp") or n == "call_mcp_tool"])

    def test_a_permission_a_question_and_a_new_process_go_through_in_a_private_home(self):
        self.configure("antigravity", program=SDK, credentials=KEY, permission="ask")
        turn = self.turn("runit")
        question, after = self.question(turn)
        self.assertEqual(question["permission"]["detail"], "echo hi")
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow"}})
        answer = self.server.until(turn, ev.DONE, after=after)
        self.assertEqual([e.get("output") for e in answer["seen"] if e["type"] == ev.TOOL_RESULT], ["hi\r\n"])
        turn = self.turn("askit")
        question, after = self.question(turn)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"q0": ["blue"]}}})
        self.assertIn("A1: blue", self.server.until(turn, ev.DONE, after=after)["reply"])
        conversation = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.configure("antigravity", program=SDK, credentials=KEY, permission="ask")
        self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "hello from antigravity")
        self.assertTrue(os.path.isdir(os.path.join(self.home, "antigravity-home", ".gemini", "antigravity")),
                        "the harness kept its own data in the private home")
        self.assertEqual(self.server.conversation.conversation_id(), conversation)
        with open(self.requests) as f:
            last = json.loads(f.readlines()[-1])["body"]["contents"]
        self.assertIn("runit", json.dumps(last[0]), "the new process carried the conversation on")


if __name__ == "__main__":
    unittest.main()
