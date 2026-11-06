"""Muse Code against its stand-in (`testing/fake_muse.py`, MSP from real
`muse serve` lines): a turn's events, an approval stage by stage, a refusal
with its note, the ask tool, the program's tool over the session's MCP
server, a resume after a restart that cancels the turn the old server left
waiting, the login failures, and what each setting does to the command line
and the session."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

import rig  # noqa: E402
from rig import LOOKUP, Rig  # noqa: E402
import adapter_muse  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402


class MuseTest(Rig):
    def setUp(self):
        super().setUp()
        rig.shim(os.path.join(self.root, "bin"), "muse", "fake_muse.py")

    def sent(self, method):
        return [m["params"] for m in self.logged() if m.get("method") == method]

    def test_a_turn_streams_its_text_and_ends_with_the_final_answer(self):
        self.configure("muse", streamBy="token")
        answer = self.server.until(self.turn("hello from weft"), ev.DONE)
        self.assertEqual(answer["reply"], "echo: hello from weft")
        self.assertEqual([(e["type"], e.get("text")) for e in answer["seen"]],
                         [(ev.TEXT_DELTA, "echo:"), (ev.TEXT_DELTA, " hello from weft"), (ev.TEXT, "echo: hello from weft"),
                          (ev.DONE, None)], "the reminder child is Muse's own, nothing of weft's")
        start = self.sent("session/start")[0]
        self.assertEqual((start["approvalMode"], start["permissionProfile"]), ("promptUnmatched", ":ask-me"))
        self.assertEqual(start["config"]["mcpServers"]["weft"]["transport"], "streamableHttp")
        self.assertEqual(self.sent("initialize")[0]["capabilities"]["requestedCapabilities"], ["sessionMcp"])
        self.assertIn({"jsonrpc": "2.0", "method": "initialized"}, self.logged())

    def test_a_pipeline_is_approved_stage_by_stage(self):
        self.configure("muse")
        turn = self.turn("run a pipe")
        after = 0
        for stage in (0, 1):
            pending = self.server.until(turn, ev.WAITING_QUESTION, after=after)
            after = pending["after"]
            question = pending["pending"]["question"]
            self.assertEqual(question["permission"]["tool"], "shell")
            self.assertEqual(question["permission"]["detail"], "ls | wc -l\n(in /w)")
            self.assertEqual([(c["id"], c["kind"]) for c in question["permission"]["choices"]],
                             [("c-once", ev.ALLOW_ONCE), ("c-session", ev.ALLOW_ALWAYS), ("c-deny", ev.REJECT)])
            self.assertTrue(question["id"].endswith(f"-{stage}"))
            self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "c-once"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "approved,approved")
        self.assertIn({"type": ev.TOOL_RESULT, "output": "Tue", "isError": False},
                      [{k: e[k] for k in ("type", "output", "isError")} for e in answer["seen"] if e["type"] == ev.TOOL_RESULT])
        decided = self.sent("approval/decide")
        self.assertEqual([d["requirementId"]["sourceIndex"] for d in decided], [0, 1])

    def test_a_refusal_carries_the_note_as_feedback(self):
        self.configure("muse")
        turn = self.turn("run it")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "c-deny", "note": "not today"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(answer["reply"], "denied (not today)")
        self.assertIn(True, [e["isError"] for e in answer["seen"] if e["type"] == ev.TOOL_RESULT])

    def test_the_ask_tool_takes_labels_and_free_text_and_its_countdown_is_stopped(self):
        self.configure("muse")
        turn = self.turn("ask me")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual(question["kind"], ev.KIND_QUESTION)
        colour, sizes = question["questions"]
        self.assertEqual((colour["multiSelect"], colour["allowOther"], colour["options"][0]),
                         (False, True, {"label": "Red", "description": "warm"}))
        self.assertTrue(sizes["multiSelect"])
        engaged = self.sent("userInput/engaged")
        self.assertEqual(len(engaged), 1)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"colour": ["purple"], "sizes": ["S", "L"]}}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(answer["reply"]), [{"questionId": "colour", "freeText": "purple"},
                                                       {"questionId": "sizes", "selectedLabels": ["S", "L"]}])
        self.assertEqual(self.sent("userInput/answer")[0]["userInputId"], engaged[0]["userInputId"])

    def test_the_programs_tool_is_called_over_the_sessions_mcp_server(self):
        self.configure("muse")
        turn = self.turn("tool please", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {"key": "x"}))
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(answer["reply"]), {"content": [{"type": "text", "text": "42"}], "isError": False, "structuredContent": {"result": 42}})
        self.assertEqual([m["mcp"] for m in self.logged() if "mcp" in m],
                         ["initialize", "notifications/initialized", "tools/list", "tools/call"])
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in answer["seen"]], "the program's tool shows once, as its call")
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        calls = [c["name"] for m in history for c in m.get("tool_calls") or []]
        self.assertEqual(calls, ["lookup"], "named as the program names it")

    def test_its_own_tool_stays_its_own_beside_a_program_tool_of_the_same_plain_name(self):
        # The model sees the program's as `mcp__weft__shell`: Muse's own `shell`
        # is still Muse's, by the exact name, never by what the name ends with.
        self.configure("muse")
        tools = [{"name": "shell", "description": "the program's", "parameters": {"type": "object", "properties": {}}}]
        turn = self.turn("run it", tools=tools)
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "c-once"}})
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual([e["name"] for e in answer["seen"] if e["type"] == ev.TOOL_USE], ["shell"])
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        self.assertEqual([c["name"] for m in history for c in m.get("tool_calls") or []], ["shell"])
        self.assertIn("Tue", [m["content"] for m in history if m["role"] == "tool"], "its result is the agent's too")

    def test_a_restart_resumes_the_session_and_cancels_the_turn_left_waiting(self):
        self.configure("muse")
        turn = self.turn("run it")
        self.server.until(turn, ev.WAITING_QUESTION)
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.configure("muse", model="muse-spark-1.3", permission="bypass")
        self.assertEqual(self.server.call("GET", f"/events?turn={turn}")[1]["status"], ev.LOST)
        answer = self.server.until(self.turn("hello again", tools=LOOKUP), ev.DONE)
        self.assertEqual(answer["reply"], "echo: hello again")
        resume = self.sent("session/resume")[0]
        self.assertEqual((resume["sessionId"], resume["modelId"], resume["permissionProfile"]), (session, "muse-spark-1.3", ":unrestricted"))
        self.assertEqual(resume["config"]["mcpServers"]["weft"]["url"], adapters.mcp_url(), "given again: a resume starts no server on its own")
        self.assertEqual(self.sent("session/setApprovalMode")[0]["mode"], "allowAll")
        self.assertEqual(len(self.sent("turn/cancel")), 1)
        self.assertEqual(self.sent("approval/decide"), [], "the re-sent request of the lost turn reaches nobody")

    def test_a_session_muse_no_longer_has_says_to_start_over(self):
        self.configure("muse")
        self.server.until(self.turn("hello"), ev.DONE)
        os.remove(os.path.join(self.root, "muse-sessions.json"))
        self.server.close()
        self.server = self.start()
        self.configure("muse")
        answer = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("start the conversation over", answer["error"])

    def test_no_login_and_a_rejected_key_each_say_what_to_do(self):
        self.configure("muse")
        self.assertIn("this machine has no Muse Code login", self.server.until(self.turn("fail"), ev.FAILED)["error"])
        self.configure("muse", credentials={"way": "apiKey", "values": {"key": "rejected"}})
        error = self.server.until(self.turn("fail"), ev.FAILED)["error"]
        self.assertIn("Meta rejected the connection's API key", error)

    def test_the_key_reaches_muse_as_its_variable_and_no_other_vendors_key_does(self):
        os.environ["OPENAI_API_KEY"] = "not-muses"
        adapter = adapter_muse.Muse(self.server.conversation, {"folder": self.folder, "credentials": {"way": "apiKey", "values": {"key": "k"}}}, [])
        env = adapter.env()
        self.assertEqual(env["META_API_KEY"], "k")
        self.assertNotIn("OPENAI_API_KEY", env)
        adapter = adapter_muse.Muse(self.server.conversation, {"folder": self.folder}, [])
        self.assertNotIn("META_API_KEY", adapter.env(), "the machine's own login")

    def test_effort_and_extra_folders_ride_every_turn(self):
        extra = os.path.join(self.root, "extra")
        os.makedirs(extra)
        self.configure("muse", options={"effort": "low", "addDirs": [extra]})
        self.server.until(self.turn("hello"), ev.DONE)
        sent = self.sent("turn/start")[0]
        self.assertEqual((sent["reasoningEffort"], sent["workspaceRoots"]), ("low", [self.folder, extra]))


class MuseSettingsTest(unittest.TestCase):
    def test_every_switch_and_the_network_become_their_flags(self):
        options = {"trustWorkspace": True, "contributorMode": True, "disableQuestions": True, "disableSandbox": True, "network": "proxy_only"}
        self.assertEqual(adapter_muse.command({"options": options})[8:],
                         ["--trust-workspace", "--contributor-mode", "on", "--disable-request-user-input", "--disable-sandbox",
                          "--sandbox-network", "proxy-only"])
        self.assertEqual(adapter_muse.command({"options": {}}),
                         ["muse", "serve", "--provider", "meta", "-c", "context.foreign_personal_rules=false",
                          "-c", "context.foreign_personal_skills=false"], "never another tool's personal rules or skills")

    def test_a_value_muse_does_not_take_is_refused(self):
        with self.assertRaises(Refused):
            adapter_muse.command({"options": {"network": "open"}})
        with self.assertRaises(Refused):
            adapter_muse.command({"options": {"trustWorkspace": "yes"}})
        with self.assertRaises(Refused):
            adapter_muse.effort({"options": {"effort": "none"}})
        with self.assertRaises(Refused):
            adapter_muse.workspace_roots({"folder": "/w", "options": {"addDirs": ["/no/such/folder"]}})

    def test_every_permission_setting_is_a_profile_and_a_mode(self):
        self.assertEqual(adapter_muse.PERMISSIONS["auto_review"], (":auto-review", "onRequest"))
        self.assertEqual(adapter_muse.PERMISSIONS["read_only"][0], ":read-only")
        self.assertEqual(adapter_muse.PERMISSIONS["deny_unmatched"][1], "denyUnmatched")

    def test_a_choice_weft_cannot_name_fails_rather_than_guessing(self):
        params = {"subject": {"kind": "network", "host": "example.com", "port": 443, "protocol": "https"}, "rawArgs": "{}",
                  "availableChoices": [{"choiceId": "x", "decision": "approvedOnTuesdays", "label": "Tuesdays", "scope": "once"}]}
        with self.assertRaises(Refused):
            adapter_muse.approval_question("q", params)
        params["availableChoices"] = [{"choiceId": "a", "decision": "approvedPolicyAmendment", "label": "Always", "scope": "localPersistent",
                                       "rulePreview": "allow example.com:443"}]
        question = adapter_muse.approval_question("q", params)
        self.assertEqual(question["permission"]["detail"], "https://example.com:443")
        self.assertEqual(question["permission"]["choices"], [ev.choice("a", "Always (allow example.com:443)", ev.ALLOW_ALWAYS)])

    def test_command_ids_are_uuid7(self):
        ids = {adapter_muse.uuid7() for _ in range(100)}
        self.assertEqual(len(ids), 100)
        for value in ids:
            self.assertEqual((value[14], value[19] in "89ab"), ("7", True))


if __name__ == "__main__":
    unittest.main()
