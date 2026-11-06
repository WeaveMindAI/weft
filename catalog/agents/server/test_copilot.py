"""GitHub Copilot CLI over the Copilot SDK's protocol (`copilot --headless`).

What the adapter sends for a configuration (the program's tools under their
plain names, a built-in replaced, the options, the logins), read straight
off its functions; then whole turns through the agent server against
`testing/fake_copilot.py`, which replays what Copilot 1.0.105 really wrote:
the program's tools coming back by their plain names and kept in the
history as the program's, a question, a permission, the permission
settings, a resumed conversation, a sub-agent, and the ways a turn fails.

Run with the others: `python3 -m unittest test_copilot`.
"""

import io
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_copilot as copilot  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402

# A program tool named like one of Copilot's built-ins, beside a new one.
VIEW = {"name": "view", "description": "The program's own file viewer", "parameters": {"type": "object", "properties": {}}}
TOOLS = [VIEW, *LOOKUP]
PROVIDER = {"way": "ownProvider", "values": {"baseUrl": "http://127.0.0.1:1/v1", "type": "openai", "key": "sk-test",
                                             "model": "their-model", "wireApi": "completions"}}


class Home:
    """Enough of a conversation for an adapter built outside a server."""

    def __init__(self, home):
        self.home = home


def adapter(tools=(), home="/tmp/unused", **config):
    return copilot.Copilot(Home(home), {"agent": "copilot", "folder": "/w", **config}, list(tools))


class WhatIsSent(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)
    def test_the_programs_tools_keep_their_plain_names_and_replace_a_built_in_of_theirs(self):
        sent = adapter(TOOLS).session_params()["tools"]
        self.assertEqual([t["name"] for t in sent], ["view", "lookup"])
        # Every one overrides: a built-in of its name is replaced, and a new
        # name is just a new tool (Copilot takes the flag on both).
        self.assertTrue(all(t["overridesBuiltInTool"] and t["skipPermission"] for t in sent))
        self.assertEqual(sent[0]["description"], "The program's own file viewer")

    def test_the_tool_lists_never_hide_the_programs_tools(self):
        params = adapter(TOOLS, options={"availableTools": ["grep"]}).session_params()
        self.assertEqual(params["availableTools"], ["grep", "lookup", "view"])
        with self.assertRaises(Refused) as refused:
            adapter(TOOLS, options={"excludedTools": ["bash", "lookup"]})
        self.assertIn("`excludedTools` names the program's tool lookup", str(refused.exception))

    def test_the_options_and_the_model(self):
        params = adapter(model="gpt-5", options={"effort": "high", "addDirs": ["/a", "/b"], "maxAiCredits": 50.0}).session_params()
        self.assertEqual((params["model"], params["reasoningEffort"], params["additionalDirectories"]), ("gpt-5", "high", ["/a", "/b"]))
        self.assertEqual(params["sessionLimits"], {"maxAiCredits": 50})
        self.assertNotIn("model", adapter().session_params(), "left out, Copilot's default")
        with self.assertRaises(Refused):
            adapter(options={"effort": "extreme"})

    def test_a_provider_of_ones_own_goes_over_the_protocol_and_the_nodes_model_wins(self):
        home = self.home
        params = adapter(home=home, credentials=PROVIDER).session_params()
        self.assertEqual(params["provider"], {"type": "openai", "baseUrl": "http://127.0.0.1:1/v1", "apiKey": "sk-test", "wireApi": "completions"})
        self.assertEqual(params["model"], "their-model")
        self.assertEqual(adapter(home=home, credentials=PROVIDER, model="mine").session_params()["model"], "mine")

    def test_a_connections_login_is_the_only_one_it_has(self):
        home = self.home
        token = adapter(home=home, credentials={"way": "githubToken", "values": {"token": "github_pat_x"}})
        self.assertEqual(token.command()[1:], ["--headless", "--stdio", "--no-auto-update", "--no-auto-login",
                                               "--auth-token-env", "COPILOT_GITHUB_TOKEN"])
        self.assertEqual(adapter(home=home, credentials=PROVIDER).command()[4:], ["--no-auto-login"])
        self.assertEqual(adapter().command()[1:], ["--headless", "--stdio", "--no-auto-update"], "the machine's own login")

    def test_a_permission_request_reads_as_weft_asks_it(self):
        data = {"requestId": "r1",
                "permissionRequest": {"kind": "shell", "fullCommandText": "touch made.txt", "intention": "make a file"},
                "promptRequest": {"kind": "commands", "commandIdentifiers": ["touch made.txt"]}}
        question, decision_of = copilot.permission_question("r1", data)
        self.assertEqual(question["permission"]["detail"], "touch made.txt\n(make a file)")
        self.assertEqual([c["id"] for c in question["permission"]["choices"]], ["approve-once", "approve-for-session", "reject"])
        self.assertEqual(decision_of({"choice": "approve-for-session"}),
                         {"kind": "approve-for-session", "approval": {"kind": "commands", "commandIdentifiers": ["touch made.txt"]}})
        self.assertEqual(decision_of({"choice": "reject", "note": "not that"}), {"kind": "reject", "feedback": "not that"})
        url = {"permissionRequest": {"kind": "url", "url": "https://x.org/a"}, "promptRequest": {"kind": "url", "url": "https://x.org/a"}}
        self.assertEqual(copilot.permission_question("r2", url)[1]({"choice": "approve-for-session"}),
                         {"kind": "approve-for-session", "domain": "x.org"})
        hook = {"permissionRequest": {"kind": "hook"}, "promptRequest": {"kind": "hook"}}
        self.assertEqual([c["id"] for c in copilot.permission_question("r3", hook)[0]["permission"]["choices"]],
                         ["approve-once", "reject"], "no session approval covers it")

    def test_messages_are_framed_both_ways(self):
        raw = io.BytesIO()
        written = io.TextIOWrapper(raw, write_through=True)
        copilot.FramedIn(written).write(json.dumps({"id": 1, "é": "ü"}) + "\n")
        framed = raw.getvalue()
        self.assertTrue(framed.startswith(b"Content-Length: %d\r\n\r\n" % len(json.dumps({"id": 1, "é": "ü"}).encode())))
        read = list(copilot.FramedOut(io.TextIOWrapper(io.BytesIO(framed + b"stray\r\n" + framed))))
        self.assertEqual(read, [json.dumps({"id": 1, "é": "ü"}), "stray", json.dumps({"id": 1, "é": "ü"})])


class AStoppedTurn(unittest.TestCase):
    """A turn the adapter stops (a model the plan does not offer) fails once
    Copilot took the stop and ended its turn, in either order 1.0.105 says
    them in, and never before."""

    def stopped(self):
        stopping = adapter(model="gpt-5-mini")
        failed, sent = [], []
        stopping.conv = type("Conv", (), {"fail": lambda _, turn, why: failed.append(why)})()
        stopping.send = sent.append
        stopping.session, stopping.turn = "s-1", type("Turn", (), {"status": ev.WORKING})()
        stopping.on_message(self.event("session.auto_mode_resolved", {"chosenModel": "gpt-6-luna",
                                                                      "availableModels": ["gpt-6-luna"]}))
        self.assertEqual([m["method"] for m in sent], ["session.abort"])
        return stopping, failed, sent[0]["id"]

    @staticmethod
    def event(kind, data):
        return {"method": "session.event", "params": {"event": {"type": kind, "data": data}}}

    def test_an_abort_that_lands_while_the_turn_runs(self):
        stopping, failed, rid = self.stopped()
        stopping.on_message(self.event("abort", {"reason": "user_initiated"}))
        stopping.on_message({"jsonrpc": "2.0", "id": rid, "result": {"success": True}})
        self.assertEqual(failed, [], "Copilot's turn is not over yet")
        stopping.on_message(self.event("session.idle", {"aborted": True}))
        self.assertEqual(len(failed), 1)
        self.assertIn("would run gpt-6-luna instead of the node's model gpt-5-mini", failed[0])

    def test_an_abort_that_lands_after_the_turn_went_idle(self):
        stopping, failed, rid = self.stopped()
        stopping.on_message(self.event("assistant.message", {"content": "an answer of the wrong model"}))
        stopping.on_message(self.event("session.idle", {}))
        self.assertEqual(failed, [], "Copilot has not taken the stop yet")
        stopping.on_message({"jsonrpc": "2.0", "id": rid, "result": {"success": True}})
        self.assertEqual(len(failed), 1)
        self.assertEqual(stopping.reply, [], "nothing it said after the stop is the turn's")

    def test_a_stop_copilot_did_not_take_fails_saying_so(self):
        stopping, failed, rid = self.stopped()
        stopping.on_message({"jsonrpc": "2.0", "id": rid, "error": {"code": -32603, "message": "no"}})
        self.assertEqual(len(failed), 1)
        self.assertIn("did not take the stop (session.abort)", failed[0])


class CopilotTurns(Rig):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), "copilot", "fake_copilot.py")

    def received(self, method=None) -> list:
        return [m for m in self.logged() if "start" not in m and (method is None or m.get("method") == method)]

    def starts(self) -> list:
        return [m["start"] for m in self.logged() if "start" in m]

    def history(self, turn) -> list:
        return self.server.call("GET", f"/history?turn={turn}")[1]["messages"]

    def test_a_program_tool_named_like_a_built_in_comes_back_by_its_plain_name_and_is_the_programs_in_the_history(self):
        self.configure("copilot")
        turn = self.turn("call:view call:grep call:lookup", tools=TOOLS)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], call["callId"], json.loads(call["arguments"])), ("view", "call_0", {"q": "x"}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program's view"}})
        call = self.server.until(turn, ev.WAITING_TOOL, after=call["n"])["pending"]
        self.assertEqual(call["name"], "lookup")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "no such key", "isError": True}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(done["reply"], "view: the program's view\ngrep: built-in grep ran\nlookup failed: no such key")
        uses = [e["name"] for e in done["seen"] if e["type"] == ev.TOOL_USE]
        self.assertEqual(uses, ["grep"], "a call of the program's tool is its tool_call, never the agent's own tool use")
        calls = [c for m in self.history(turn) for c in m.get("tool_calls") or []]
        self.assertEqual([(c["id"], c["name"]) for c in calls], [("call_0", "view"), ("call_1", "grep"), ("call_2", "lookup")])
        results = {m["tool_call_id"]: m["content"] for m in self.history(turn) if m["role"] == "tool"}
        self.assertEqual(results, {"call_0": "the program's view", "call_1": "built-in grep ran", "call_2": "no such key"})
        answered = [m["params"] for m in self.received("session.tools.handlePendingToolCall")]
        self.assertEqual([a["result"] for a in answered],
                         [{"textResultForLlm": "the program's view", "resultType": "success"},
                          {"textResultForLlm": "no such key", "resultType": "failure", "error": "no such key"}])
        created = self.received("session.create")[0]["params"]
        self.assertEqual([(t["name"], t["overridesBuiltInTool"]) for t in created["tools"]], [("view", True), ("lookup", True)])
        self.assertEqual(self.received("session.tools.initializeAndValidate")[0]["params"]["sessionId"], self.session_id())

    def session_id(self):
        return self.server.conversation.conversation_id()

    def test_a_question_waits_for_the_person(self):
        self.configure("copilot")
        turn = self.turn("ask")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual(question["questions"][0]["question"], "Red or blue?")
        self.assertEqual([o["label"] for o in question["questions"][0]["options"]], ["Red", "Blue"])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"answer": ["purple"]}}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], 'ask: {"answer": "purple", "wasFreeform": true}')
        turn = self.turn("ask")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"answer": ["Blue"]}}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], 'ask: {"answer": "Blue", "wasFreeform": false}')

    def test_a_refused_permission_goes_back_to_the_model_with_the_note_and_the_turn_goes_on(self):
        self.configure("copilot")
        turn = self.turn("run")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual((question["kind"], question["permission"]["tool"]), ("permission", "shell"))
        self.assertEqual(question["permission"]["title"], "GitHub Copilot CLI wants to run a command")
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "reject", "note": "use another way"}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(done["reply"], 'run: {"kind": "reject", "feedback": "use another way"}')
        result = [e for e in done["seen"] if e["type"] == ev.TOOL_RESULT][0]
        self.assertTrue(result["isError"])

        turn = self.turn("run")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "approve-for-session"}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(done["reply"][len("run: "):]),
                         {"kind": "approve-for-session", "approval": {"kind": "commands", "commandIdentifiers": ["touch made.txt"]}})
        self.assertIn({"type": ev.FILE_CHANGE, "path": "made.txt", "change": "add"},
                      [{k: e[k] for k in ("type", "path", "change")} for e in done["seen"] if e["type"] == ev.FILE_CHANGE])

    def test_each_permission_setting_sets_both_modes(self):
        for permission, (mode, approvals) in copilot.PERMISSIONS.items():
            with self.subTest(permission):
                self.configure("copilot", permission=permission)
                done = self.server.until(self.turn("run"), ev.DONE, ev.WAITING_QUESTION)
                if approvals == "allow-all":
                    self.assertEqual(done["status"], ev.DONE, "nothing asks")
                else:
                    self.server.post("/turns", {"answer": {"questionId": done["pending"]["question"]["id"], "choice": "approve-once"}})
                self.assertEqual(self.received("session.mode.set")[-1]["params"]["mode"], mode)
                self.assertEqual(self.received("session.permissions.setMode")[-1]["params"]["mode"], approvals)

    def test_a_restarted_server_resumes_the_conversation(self):
        self.configure("copilot")
        first = self.turn("hello")
        self.server.until(first, ev.DONE)
        conversation = self.session_id()
        self.server.close()
        self.server = self.start()
        self.configure("copilot", permission="plan")
        self.assertEqual(self.server.until(self.turn("again", tools=TOOLS), ev.DONE)["reply"], "done")
        resumed = self.received("session.resume")
        self.assertEqual(len(resumed), 1)
        self.assertEqual(resumed[0]["params"]["sessionId"], conversation)
        self.assertEqual([t["name"] for t in resumed[0]["params"]["tools"]], ["view", "lookup"], "today's tools")
        self.assertEqual(self.received("session.mode.set")[-1]["params"]["mode"], "plan")

    def test_a_conversation_copilot_no_longer_has_says_to_start_over(self):
        self.configure("copilot")
        self.server.until(self.turn("hello"), ev.DONE)
        # Gone from Copilot's own sessions; new tools start it again, to resume.
        os.remove(os.path.join(self.root, "copilot-home", "session-state", f"{self.session_id()}.json"))
        failed = self.server.until(self.turn("again", tools=TOOLS), ev.FAILED)
        self.assertIn("Session not found", failed["error"])
        self.assertIn("start it over from the node's card", failed["error"])

    def test_a_github_token_is_its_only_login_in_a_home_of_its_own(self):
        os.environ["GH_TOKEN"] = "the-persons-own"
        os.environ["COPILOT_PROVIDER_BASE_URL"] = "http://the-persons-own"
        self.configure("copilot", credentials={"way": "githubToken", "values": {"token": "github_pat_x"}})
        self.server.until(self.turn("hello"), ev.DONE)
        start = self.starts()[-1]
        self.assertEqual(start["env"]["COPILOT_GITHUB_TOKEN"], "github_pat_x")
        self.assertEqual(start["env"]["COPILOT_HOME"], os.path.join(self.home, "copilot-home"))
        self.assertNotIn("COPILOT_PROVIDER_BASE_URL", start["env"])
        self.assertIn("--no-auto-login", start["args"], "so GH_TOKEN and `gh` are never tried")

    def test_a_bad_token_fails_the_turn_with_githubs_answer(self):
        self.configure("copilot", credentials={"way": "githubToken", "values": {"token": "bad"}})
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("Bad credentials", failed["error"])

    def test_streaming_by_token_sends_the_pieces_and_the_whole(self):
        self.configure("copilot", streamBy="token")
        done = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([e["type"] for e in done["seen"]], [ev.THINKING, ev.TEXT_DELTA, ev.TEXT_DELTA, ev.TEXT, ev.DONE])
        self.assertTrue(self.received("session.create")[0]["params"]["streaming"])
        self.configure("copilot", streamBy="message")
        done = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([e["type"] for e in done["seen"]], [ev.THINKING, ev.TEXT, ev.DONE])

    def test_a_sub_agents_call_of_the_programs_tool_waits_and_its_messages_are_no_reply(self):
        self.configure("copilot")
        turn = self.turn("sub:lookup", tools=TOOLS)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual(call["name"], "lookup")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(done["reply"], "sub: sub done")
        self.assertNotIn("sub done", [e.get("text") for e in done["seen"] if e["type"] == ev.TEXT and e["text"] != "sub: sub done"])

    def test_a_call_copilot_gave_up_on_stops_the_wait(self):
        self.configure("copilot")
        done = self.server.until(self.turn("abandon", tools=TOOLS), ev.DONE)
        gave_up = [e for e in done["seen"] if e["type"] == ev.TOOL_RESULT and e["id"] == "call_abandoned"]
        self.assertIn("the agent stopped waiting for this call", gave_up[0]["output"])

    def test_an_error_it_says_fails_the_turn(self):
        self.configure("copilot")
        failed = self.server.until(self.turn("fail"), ev.FAILED)
        self.assertEqual(failed["error"], "GitHub Copilot CLI could not finish the turn: Execution failed: the model refused")

    def test_a_model_the_plan_does_not_offer_is_stopped_before_it_runs(self):
        # A Free plan offers its auto mode alone: Copilot would run its own pick.
        os.environ["FAKE_COPILOT_PLAN"] = "gpt-6-luna"
        self.configure("copilot", model="gpt-5-mini")
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertEqual(failed["error"], "GitHub Copilot CLI would run gpt-6-luna instead of the node's model gpt-5-mini, which "
                                          "this Copilot plan does not offer (it offers gpt-6-luna); set `model` to one of those "
                                          "or to `auto`, or leave it empty")
        self.assertFalse([e for e in failed["seen"] if e["type"] == ev.TEXT], "nothing of the model's reaches the turn")
        self.assertIn("session.abort", [m.get("method") for m in self.received()])

    def test_auto_or_a_model_the_plan_offers_runs_on_its_pick(self):
        os.environ["FAKE_COPILOT_PLAN"] = "gpt-6-luna"
        for model in ("auto", "gpt-6-luna", None):
            with self.subTest(model=model):
                self.configure("copilot", model=model)
                self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "done")

    def test_a_crash_loses_the_turn_and_the_next_resumes(self):
        self.configure("copilot")
        self.server.until(self.turn("crash"), ev.LOST)
        self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "done")
        self.assertEqual([m["method"] for m in self.received() if m.get("method") in ("session.create", "session.resume")],
                         ["session.create", "session.resume"])

    def test_another_protocol_version_is_refused(self):
        os.environ["FAKE_PROTOCOL"] = "4"
        self.configure("copilot")
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("speaks version 4 of the Copilot SDK's protocol, and weft speaks version 3", failed["error"])


if __name__ == "__main__":
    unittest.main()
