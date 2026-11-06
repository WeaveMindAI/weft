"""Mistral Vibe over its own app server (`vibe-app-server --agent-socket`).

What the adapter sends for a configuration (the program's tools declared
on the agent socket under their plain names, a built-in turned off for one
that replaces it, a name Vibe keeps refused, the options, the logins),
read straight off its functions and its socket; then whole turns through
the agent server against `testing/fake_vibe.py`, which replays what Vibe
3.0.4 really wrote: the program's tools coming back by their plain names
and kept in the history as the program's, a question, a permission, the
permission settings, a resumed conversation, streaming, and the ways a
turn fails.

Run with the others: `python3 -m unittest test_vibe`.
"""

import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_vibe as vibe  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402

# A program tool named like one of Vibe's own tools it can turn off, beside a new one.
TODO = {"name": "todo", "description": "The program's own todo list", "parameters": {"type": "object", "properties": {}}}
TOOLS = [TODO, *LOOKUP]
KEY = {"way": "apiKey", "values": {"key": "mistral-key"}}


class Home:
    """Enough of a conversation for an adapter built outside a server."""

    def __init__(self, home):
        self.home = home


def adapter(tools=(), home="/tmp/unused", **config):
    return vibe.Vibe(Home(home), {"agent": "vibe", "folder": "/w", **config}, list(tools))


def ask_socket(path: str, message: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(path)
        s.sendall((json.dumps(message) + "\n").encode())
        return json.loads(s.makefile().readline())


class WhatIsSent(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)

    def test_the_socket_declares_the_programs_tools_by_their_plain_names_for_the_model_to_call(self):
        agent = adapter(TOOLS)
        agent.prepare()
        self.addCleanup(agent.stop_rest)
        path = agent.command()[2]
        self.assertEqual(agent.command()[1], "--agent-socket")
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600, "Vibe asks for a socket only its user can reach")
        self.assertEqual(os.stat(os.path.dirname(path)).st_mode & 0o777, 0o700)
        hello = ask_socket(path, {"jsonrpc": "2.0", "id": 1, "method": "agent/initialize",
                                  "params": {"protocolVersion": 2, "vibeVersion": "3.0.4"}})["result"]
        self.assertEqual([(t["namespace"], t["name"], t["modelAccess"]) for t in hello["tools"]],
                         [("weft", "todo", "direct"), ("weft", "lookup", "direct")])
        self.assertEqual(hello["tools"][0]["description"], "The program's own todo list")
        self.assertGreater(hello["toolTimeoutSeconds"], 10 * 365 * 86400, "no deadline on a call a person answers")
        other = ask_socket(path, {"jsonrpc": "2.0", "id": 2, "method": "agent/initialize", "params": {"protocolVersion": 3}})
        self.assertIn("weft speaks version 2 of the agent socket", other["error"]["message"])
        self.assertIn("error", ask_socket(path, {"jsonrpc": "2.0", "id": 3, "method": "sandbox/execute", "params": {}}),
                      "weft serves no sandbox")
        agent.stop_rest()
        self.assertFalse(os.path.exists(os.path.dirname(path)))

    def test_a_tool_named_like_one_it_can_turn_off_turns_it_off_and_a_new_name_turns_off_nothing(self):
        self.assertEqual(adapter(TOOLS).agent_config()["disabledTools"], ["todo"])
        self.assertEqual(adapter(LOOKUP).agent_config()["disabledTools"], [])
        both = [{"name": "ask_user_question"}, {"name": "cron"}, {"name": "unified_harness_scratchpad"}]
        self.assertEqual(adapter(both).agent_config()["disabledTools"], ["ask_user_question", "cron", "unified_harness_scratchpad"])

    def test_the_names_the_node_refuses_are_never_ones_vibe_can_turn_off(self):
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "..", "mistral_vibe", "metadata.json")) as f:
            reserved = set(json.load(f)["lists"]["reservedToolNames"])
        self.assertEqual(reserved & set(vibe.REPLACEABLE_TOOL_NAMES), set())

    def test_the_options_the_model_and_the_permission(self):
        agent = adapter(model="devstral", permission="accept_edits",
                        options={"effort": "high", "addDirs": ["/a", "/b"], "maxTurns": 7.0})
        config = agent.agent_config()
        self.assertEqual((config["cwd"], config["agent"], config["workspaceRoots"], config["maxTurns"]),
                         ("/w", "accept-edits", ["/a", "/b"], 7))
        agent.session = "s1"
        self.assertEqual(agent.model_settings(), {"sessionId": "s1", "modelAlias": "devstral", "reasoningEffort": "high",
                                                  "sessionOnly": True})
        self.assertIsNone(adapter().model_settings(), "left out, Vibe's own")
        with self.assertRaises(Refused):
            adapter(options={"effort": "extreme"})
        with self.assertRaises(Refused):
            adapter(options={"maxTurns": 0})

    def test_a_connections_key_is_its_only_login_in_a_home_of_its_own(self):
        agent = adapter(home=self.home, credentials=KEY, folder=self.home)
        env = {"MISTRAL_API_KEY": "the-persons-own", "VIBE_ACTIVE_MODEL": "theirs", "vibe_providers": "theirs", "PATH": "/bin"}
        agent.credential_env(env)
        self.assertEqual(env, {"MISTRAL_API_KEY": "mistral-key", "VIBE_HOME": os.path.join(self.home, "vibe-home"), "PATH": "/bin",
                               "VIBE_ENABLE_UPDATE_CHECKS": "false", "VIBE_ENABLE_AUTO_UPDATE": "false"})
        agent.prepare()
        self.addCleanup(agent.stop_rest)
        with open(os.path.join(self.home, "vibe-home", "trusted_folders.toml")) as f:
            self.assertEqual(f.read(), f"trusted = [{json.dumps(self.home)}]\nuntrusted = []\n")
        own = adapter(home=self.home)
        env = {"VIBE_ACTIVE_MODEL": "theirs"}
        own.credential_env(env)
        self.assertEqual(env["VIBE_ACTIVE_MODEL"], "theirs", "the machine's own login keeps the person's own settings")
        self.assertNotIn("VIBE_HOME", env)

    def test_a_permission_request_reads_as_weft_asks_it(self):
        callback = {"callbackId": "approval-call_1", "detail": {
            "kind": "approval", "effect": {"toolName": "file_system.bash", "display": {"summary": "bash: touch made.txt"},
                                           "kind": "shell", "input": {"command": "touch made.txt"}},
            "choices": ["approve", "approve_for_session", "approve_permanently", "deny", "cancel_turn"], "reason": "it writes"}}
        question, output_of = vibe.permission_question(callback)
        permission = question["permission"]
        self.assertEqual((question["id"], permission["tool"], permission["title"], permission["detail"]),
                         ("approval-call_1", "bash", "Mistral Vibe wants to use bash", "bash: touch made.txt\n(it writes)"))
        self.assertEqual([c["id"] for c in permission["choices"]], ["approve", "approve_for_session", "deny"],
                         "never a grant written into the person's config for good")
        self.assertEqual(output_of({"choice": "deny", "note": "not that"}),
                         {"type": "approval", "decision": {"type": "deny"}, "feedback": "not that"})
        self.assertEqual(output_of({"choice": "approve_for_session"}),
                         {"type": "approval", "decision": {"type": "approve_for_session"}, "feedback": None})

    def test_a_patch_applies_as_vibe_sends_it(self):
        entry = {"content": [{"type": "text", "text": "Hel"}], "state": {"status": "running", "outputText": ""}}
        vibe.apply_patch(entry, [{"op": "append", "path": "/content/0/text", "value": "lo"},
                                 {"op": "add", "path": "/state/output", "value": {"a": 1}},
                                 {"op": "append", "path": "/state/outputText", "value": "x"},
                                 {"op": "remove", "path": "/state/status"}])
        self.assertEqual(entry, {"content": [{"type": "text", "text": "Hello"}], "state": {"outputText": "x", "output": {"a": 1}}})

    def test_the_program_is_vibes_app_server(self):
        self.assertEqual(adapter().program(), "vibe-app-server")
        self.assertEqual(adapter(program="~/vibe/bin/vibe-app-server").program(), os.path.expanduser("~/vibe/bin/vibe-app-server"))


class VibeTurns(Rig):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), "vibe-app-server", "fake_vibe.py")

    def received(self, method=None) -> list:
        return [m for m in self.logged() if "start" not in m and "socket" not in m
                and (method is None or m.get("method") == method)]

    def socket_calls(self) -> list:
        return [m for m in self.logged() if "socket" in m and m["socket"]["method"] == "tools/call"]

    def starts(self) -> list:
        return [m["start"] for m in self.logged() if "start" in m]

    def history(self, turn) -> list:
        return self.server.call("GET", f"/history?turn={turn}")[1]["messages"]

    def test_a_program_tool_replacing_a_built_in_comes_back_by_its_plain_name_and_is_the_programs_in_the_history(self):
        self.configure("vibe")
        turn = self.turn("call:todo call:grep call:lookup", tools=TOOLS)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("todo", {"q": "x"}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program's todo"}})
        call = self.server.until(turn, ev.WAITING_TOOL, after=call["n"])["pending"]
        self.assertEqual(call["name"], "lookup")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "no such key", "isError": True}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(done["reply"], "todo: the program's todo\ngrep: built-in grep ran\nlookup failed: no such key")
        uses = [e["name"] for e in done["seen"] if e["type"] == ev.TOOL_USE]
        self.assertEqual(uses, ["grep"], "a call of the program's tool is its tool_call, never the agent's own tool use")
        calls = [c for m in self.history(turn) for c in m.get("tool_calls") or []]
        self.assertEqual([c["name"] for c in calls], ["todo", "grep", "lookup"])
        results = [m["content"] for m in self.history(turn) if m["role"] == "tool"]
        self.assertEqual(results, ["the program's todo", "built-in grep ran", "no such key"])
        replies = [m["reply"]["result"] for m in self.socket_calls()]
        self.assertEqual(replies, [{"output": "the program's todo"},
                                   {"output": None, "error": {"message": "no such key", "code": "tool_failed"}}])
        started = self.received("session/start")[0]["params"]["agentConfig"]
        self.assertEqual(started["disabledTools"], ["todo"], "its own todo is off, so the program's takes its place")

    def test_a_question_waits_for_the_person(self):
        self.configure("vibe")
        turn = self.turn("ask")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual([(q["question"], q["multiSelect"], q["allowOther"]) for q in question["questions"]],
                         [("Red or blue?", False, True), ("Which sizes?", True, False)])
        self.assertEqual([o["label"] for o in question["questions"][0]["options"]], ["Red", "Blue"])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"q0": ["purple"], "q1": ["S", "M"]}}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(done["reply"][len("ask: "):]), {"answers": [
            {"question": "Red or blue?", "answer": "purple", "isOther": True},
            {"question": "Which sizes?", "answer": "S, M", "isOther": False}], "cancelled": False})
        status, refused = self.server.call("POST", "/turns", {"answer": {"questionId": question["id"], "answers": {"q0": ["x"]}}})
        self.assertEqual(status, 409, refused)

    def test_a_refused_permission_goes_back_to_the_model_with_the_note_and_the_turn_goes_on(self):
        self.configure("vibe")
        turn = self.turn("run")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual((question["kind"], question["permission"]["tool"]), ("permission", "bash"))
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "deny", "note": "use another way"}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(done["reply"][len("run: "):]),
                         {"type": "approval", "decision": {"type": "deny"}, "feedback": "use another way"})
        result = [e for e in done["seen"] if e["type"] == ev.TOOL_RESULT][0]
        self.assertTrue(result["isError"])
        self.assertEqual(result["output"], "Tool execution denied by approval callback")
        self.assertFalse(os.path.exists(os.path.join(self.folder, "made.txt")))

        turn = self.turn("run")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "approve_for_session"}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(done["reply"][len("run: "):])["decision"], {"type": "approve_for_session"})
        self.assertTrue(os.path.exists(os.path.join(self.folder, "made.txt")))

    def test_each_permission_setting_runs_it_as_its_agent(self):
        for permission, agent in vibe.PERMISSIONS.items():
            with self.subTest(permission):
                self.configure("vibe", permission=permission)
                done = self.server.until(self.turn("write"), ev.DONE, ev.WAITING_QUESTION)
                if agent == "ask":
                    self.assertEqual(done["status"], ev.WAITING_QUESTION, "a file write asks")
                    self.server.post("/turns", {"answer": {"questionId": done["pending"]["question"]["id"], "choice": "approve"}})
                    done = self.server.until(done["turn"], ev.DONE, after=done["after"])
                else:
                    self.assertEqual(done["status"], ev.DONE, "a file write goes ahead")
                self.assertEqual(self.received("session/agent/update")[-1]["params"]["agentName"], agent)
                changes = [(e["path"], e["change"]) for e in done["seen"] if e["type"] == ev.FILE_CHANGE]
                self.assertEqual([os.path.basename(p) for p, _ in changes], ["made.txt"])

    def test_a_restarted_server_resumes_the_conversation_on_todays_settings(self):
        self.configure("vibe")
        self.server.until(self.turn("hello"), ev.DONE)
        conversation = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.configure("vibe", permission="bypass", model="devstral", options={"effort": "low"})
        self.assertEqual(self.server.until(self.turn("again", tools=TOOLS), ev.DONE)["reply"], "done")
        resumed = self.received("session/resume")
        self.assertEqual([r["params"]["sessionId"] for r in resumed], [conversation])
        self.assertEqual(resumed[0]["params"]["agentConfig"]["disabledTools"], ["todo"], "today's tools")
        # A resumed session keeps the agent it was left on: switched again.
        self.assertEqual(self.received("session/agent/update")[-1]["params"]["agentName"], "auto-approve")
        self.assertEqual(self.received("config/model/write")[-1]["params"],
                         {"sessionId": conversation, "modelAlias": "devstral", "reasoningEffort": "low", "sessionOnly": True})

    def test_a_conversation_is_kept_only_once_a_turn_ran_on_it(self):
        self.configure("vibe")
        self.assertIsNone(self.server.conversation.conversation_id())
        self.server.until(self.turn("hello"), ev.DONE)
        self.assertIsNotNone(self.server.conversation.conversation_id())

    def test_a_conversation_vibe_no_longer_has_says_to_start_over(self):
        self.configure("vibe")
        self.server.until(self.turn("hello"), ev.DONE)
        shutil.rmtree(os.path.join(self.root, "vibe-sessions"))
        failed = self.server.until(self.turn("again", tools=TOOLS), ev.FAILED)
        self.assertIn("Session not found", failed["error"])
        self.assertIn("start it over from the node's card", failed["error"])

    def test_a_key_is_its_only_login_in_a_home_of_its_own(self):
        os.environ["MISTRAL_API_KEY"] = "the-persons-own"
        os.environ["VIBE_ACTIVE_MODEL"] = "theirs"
        self.configure("vibe", credentials=KEY)
        self.server.until(self.turn("hello"), ev.DONE)
        env = self.starts()[-1]["env"]
        self.assertEqual((env["MISTRAL_API_KEY"], env["VIBE_HOME"]), ("mistral-key", os.path.join(self.home, "vibe-home")))
        self.assertNotIn("VIBE_ACTIVE_MODEL", env)

    def test_a_bad_key_fails_the_turn_with_vibes_answer(self):
        self.configure("vibe", credentials=KEY)
        failed = self.server.until(self.turn("fail"), ev.FAILED)
        self.assertEqual(failed["error"], "Mistral Vibe could not finish the turn: Invalid API key (from env var MISTRAL_API_KEY). "
                                          "Please check your API key and try again.")

    def test_a_plan_mistral_throttles_fails_the_turn_once_vibe_gives_up(self):
        self.configure("vibe", credentials=KEY)
        failed = self.server.until(self.turn("throttled"), ev.FAILED)
        self.assertIn("Mistral Vibe could not finish the turn: API error occurred: Status 429.", failed["error"])
        self.assertIn('"message":"Rate limit exceeded"', failed["error"])

    def test_streaming_by_token_sends_the_pieces_and_the_whole(self):
        self.configure("vibe", streamBy="token")
        done = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([e["type"] for e in done["seen"]], [ev.THINKING, ev.TEXT_DELTA, ev.TEXT_DELTA, ev.TEXT, ev.DONE])
        self.assertEqual([e["text"] for e in done["seen"] if e["type"] == ev.TEXT_DELTA], ["Hel", "lo"])
        self.configure("vibe", streamBy="message")
        done = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([e["type"] for e in done["seen"]], [ev.THINKING, ev.TEXT, ev.DONE])
        self.assertEqual(done["reply"], "Hello")

    def test_a_turn_stopped_at_its_limit_fails_saying_so(self):
        self.configure("vibe", options={"maxTurns": 3})
        failed = self.server.until(self.turn("limit"), ev.FAILED)
        self.assertIn("stopped the turn at its limit", failed["error"])
        self.assertEqual(self.received("session/start")[0]["params"]["agentConfig"]["maxTurns"], 3)

    def test_a_crash_loses_the_turn_and_the_next_resumes(self):
        self.configure("vibe")
        self.server.until(self.turn("hello"), ev.DONE)
        self.server.until(self.turn("crash"), ev.LOST)
        self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "done")
        self.assertEqual([m["method"] for m in self.received() if m.get("method") in ("session/start", "session/resume")],
                         ["session/start", "session/resume"])


if __name__ == "__main__":
    unittest.main()
