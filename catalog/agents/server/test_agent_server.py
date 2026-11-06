"""The agent server against stand-ins for each agent's CLI (`testing/`).

The stand-ins speak each agent's real protocol (Claude Code's stream-json,
Codex's app-server JSON-RPC, ACP) and do what a message asks by keyword, so
every path runs here with no account: a turn's events, a permission request
and a question answered, the program's own tool called and answered, a turn
queued behind another, a restart that keeps the conversation and loses the
turn, a crash, starting over, and a fresh login handed to the agent. The pure
translations (a command line, a question, an answer) are tested on their
own. Run with the standard library alone: `python3 test_agent_server.py`.
"""

import base64
import contextlib
import glob
import http.client
import importlib
import io
import json
import os
import shutil
import signal
import stat
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import HERE, LOOKUP, Rig, shim  # noqa: E402
import adapter_acp  # noqa: E402
import adapter_claude  # noqa: E402
import adapter_codex  # noqa: E402
import adapters  # noqa: E402
import agent_server  # noqa: E402
import events as ev  # noqa: E402
from events import Refused  # noqa: E402


def jwt(claims: dict) -> str:
    """An unsigned JWT saying `claims`: what an expiry is read off."""
    def part(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
    return f"{part({'alg': 'none'})}.{part(claims)}.sig"


def setUpModule():
    # A made-up ACP agent: the protocol's own baseline, beside the real
    # agents' recorded lines (`test_acp_agents.py`), for this module alone.
    adapters.AGENTS["fakeacp"] = {"label": "Fake ACP", "check": ["fakeacp"], "install": {"linux": "true", "macos": "true"}, "bin": ["bin"], "installHint": "",
                                  "permissions": ["ask", "bypass"],
                                  "logins": {"apiKey": {"env": {"key": "FAKE_API_KEY"}}}}
    adapter_acp.register(adapter_acp.AcpSpec(
        "fakeacp", ["fakeacp"],
        permissions={"ask": adapter_acp.Setup(mode="default"), "bypass": adapter_acp.Setup(mode="yolo")},
    ))


def tearDownModule():
    del adapters.KINDS["fakeacp"], adapters.AGENTS["fakeacp"]


def load_tests(loader, tests, pattern):
    """Every other `test_*.py` beside this one runs with it."""
    for path in sorted(glob.glob(os.path.join(HERE, "test_*.py"))):
        name = os.path.splitext(os.path.basename(path))[0]
        if name != "test_agent_server":
            tests.addTests(loader.loadTestsFromModule(importlib.import_module(name)))
    return tests


class ClaudeTest(Rig):
    def test_a_turn_streams_its_events_and_a_permission_request_waits_for_its_answer(self):
        self.configure("claude")
        turn = self.turn("please bash")
        waiting = self.server.until(turn, ev.WAITING_QUESTION)
        question = waiting["pending"]["question"]
        self.assertEqual((question["kind"], question["permission"]["tool"]), ("permission", "Bash"))
        self.assertEqual([c["kind"] for c in question["permission"]["choices"]], [ev.ALLOW_ONCE, ev.ALLOW_ALWAYS, ev.REJECT])
        self.assertIn("date > d.txt", question["permission"]["detail"])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "always"}})
        done = self.server.until(turn, ev.DONE, after=waiting["after"])
        decision = json.loads(done["reply"])
        self.assertEqual(decision["behavior"], "allow")
        self.assertEqual(decision["updatedPermissions"], [{"type": "addRules", "rules": [{"toolName": "Bash"}]}])
        kinds = [e["type"] for e in waiting["seen"] + done["seen"]]
        self.assertEqual(kinds, [ev.TEXT, ev.TOOL_USE, ev.QUESTION, ev.TOOL_RESULT, ev.DONE])
        self.assertEqual([e["n"] for e in waiting["seen"] + done["seen"]], [1, 2, 3, 4, 5], "numbered once, in order")
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        self.assertEqual([m["role"] for m in history], ["user", "assistant", "assistant", "tool"])
        self.assertEqual(history[2]["tool_calls"][0]["name"], "Bash")

    def test_a_refusal_carries_the_note_to_the_agent(self):
        self.configure("claude")
        turn = self.turn("bash")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "deny", "note": "not today"}})
        decision = json.loads(self.server.until(turn, ev.DONE)["reply"])
        self.assertEqual(decision, {"behavior": "deny", "message": "not today"})

    def test_an_ask_reaches_weft_as_a_question_and_the_answer_reaches_claude_by_question_text(self):
        self.configure("claude")
        turn = self.turn("ask me")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual(question["kind"], "question")
        self.assertEqual(question["questions"][0]["options"][1], {"label": "Blue", "description": "cold"})
        self.assertTrue(question["questions"][0]["allowOther"])
        status, refused = self.server.call("POST", "/turns", {"answer": {"questionId": question["id"], "answers": {}}})
        self.assertEqual(status, 400)
        self.assertIn("has no answer", refused["error"])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"0": ["Blue"]}}})
        self.assertEqual(json.loads(self.server.until(turn, ev.DONE)["reply"]), {"Red or blue?": "Blue"})

    def test_a_question_reads_asked_while_the_agent_waits_and_gone_once_answered(self):
        self.configure("claude")
        turn = self.turn("ask me")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        path = f"{agent_server.QUESTION_PATH}?id={question['id']}"
        self.assertEqual(self.server.call("GET", path), (200, {"questionId": question["id"], "gone": False}))
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"0": ["Blue"]}}})
        self.server.until(turn, ev.DONE)
        status, answer = self.server.call("GET", path)
        self.assertEqual((status, answer["gone"]), (200, True))
        status, refused = self.server.call("GET", agent_server.QUESTION_PATH)
        self.assertEqual(status, 400)
        self.assertIn("names its question", refused["error"])

    def test_the_programs_tool_is_called_over_mcp_and_waits_for_its_result(self):
        self.configure("claude")
        turn = self.turn("use the tool", tools=LOOKUP)
        waiting = self.server.until(turn, ev.WAITING_TOOL)
        call = waiting["pending"]
        self.assertEqual((call["type"], call["name"], json.loads(call["arguments"])), (ev.TOOL_CALL, "lookup", {"key": "x"}))
        status, refused = self.server.call("POST", "/turns", {"toolResult": {"callId": "nope", "output": "1"}})
        self.assertEqual(status, 409)
        self.assertIn("nothing waits for tool_call 'nope'", refused["error"])
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        done = self.server.until(turn, ev.DONE, after=waiting["after"])
        reply = json.loads(done["reply"])
        self.assertIn("lookup", reply["tools"], "the model sees the plain name")
        self.assertEqual(reply["result"], {"content": [{"type": "text", "text": "42"}], "isError": False, "structuredContent": {"result": 42}})
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in waiting["seen"] + done["seen"]], "the program's tool shows once, as its call")
        args = self.logged()[0]["args"]
        self.assertIn("mcp__weft", args[args.index("--allowedTools") + 1])

    def test_a_second_message_waits_for_the_first_turn_to_end(self):
        self.configure("claude")
        first = self.turn("bash")
        second = self.turn("hello")
        question = self.server.until(first, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual(self.server.call("GET", f"/events?turn={second}&after=0")[1]["status"], ev.QUEUED)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow"}})
        self.server.until(first, ev.DONE)
        self.assertEqual(self.server.until(second, ev.DONE)["reply"], "done")
        self.assertEqual(len(self.logged()), 1, "one process held both turns")

    def test_starting_over_waits_for_the_turn_before_it_and_then_forgets(self):
        self.configure("claude")
        first = self.turn("bash")
        question = self.server.until(first, ev.WAITING_QUESTION)["pending"]["question"]
        fresh = self.turn("hello", startOver=True)
        self.assertEqual(self.server.call("GET", f"/events?turn={fresh}&after=0")[1]["status"], ev.QUEUED)
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow"}})
        self.server.until(first, ev.DONE)
        self.server.until(fresh, ev.DONE)
        starts = self.logged()
        self.assertEqual(len(starts), 2, "the agent started again for the new conversation")
        self.assertNotIn("--resume", starts[1]["args"])
        history = self.server.call("GET", f"/history?turn={fresh}")[1]["messages"]
        self.assertEqual(history[0], {"role": "user", "content": "hello"}, "only the new conversation")
        # The turn that ended just before the start over still reads its own,
        # as its node does once the next turn has begun.
        status, before = self.server.call("GET", f"/history?turn={first}")
        self.assertEqual((status, before["messages"][0]), (200, {"role": "user", "content": "bash"}))

    def test_a_restart_loses_the_turn_and_keeps_the_conversation(self):
        self.configure("claude")
        first = self.turn("bash")
        self.server.until(first, ev.WAITING_QUESTION)
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        lost = self.server.call("GET", f"/events?turn={first}&after=0")[1]
        self.assertEqual(lost["status"], ev.LOST)
        self.assertIn("send the message again", lost["error"])
        self.configure("claude")
        self.server.until(self.turn("hello"), ev.DONE)
        args = self.logged()[-1]["args"]
        self.assertEqual(args[args.index("--resume") + 1], session)

    def test_an_agent_that_dies_in_a_turn_loses_it_saying_what_it_printed(self):
        self.configure("claude")
        answer = self.server.until(self.turn("crash now"), ev.LOST)
        self.assertIn("ended in the middle of the turn (exit 3)", answer["error"])
        self.assertIn("crashing as asked", answer["error"])
        self.server.until(self.turn("hello"), ev.DONE)

    def test_an_agent_that_dies_leaving_a_helper_on_its_output_loses_the_turn_at_once(self):
        # The helper holds the agent's stdout and stderr open: weft stops
        # it once the agent has exited, so the readers reach their end.
        self.configure("claude")
        answer = self.server.until(self.turn("crash and leave a helper"), ev.LOST)
        self.assertIn("ended in the middle of the turn (exit 3)", answer["error"])
        self.assertIn("crashing as asked", answer["error"])
        self.server.until(self.turn("hello"), ev.DONE)

    def test_an_agent_that_dies_leaving_a_daemon_on_its_output_loses_the_turn_after_the_grace(self):
        # A helper in a session of its own is out of the agent's group: weft
        # cannot stop it, and tells the end once the grace is out, saying so.
        self.configure("claude")
        with unittest.mock.patch.object(adapters, "STOP_GRACE_SECS", 0.2):
            answer = self.server.until(self.turn("crash and a helper escapes"), ev.LOST)
        with open(os.path.join(self.folder, "helper.pid")) as written:
            pid = int(written.read())
        self.addCleanup(lambda: os.kill(pid, signal.SIGKILL))
        self.assertIn("ended in the middle of the turn (exit 3)", answer["error"])
        self.assertIn("crashing as asked", answer["error"])
        self.assertIn("started outside its own process group still holds its error output", answer["error"])

    def test_starting_over_fails_the_running_turn_and_forgets_the_conversation(self):
        self.configure("claude")
        turn = self.turn("slow")
        self.server.until(turn, ev.WORKING)
        self.assertEqual(self.server.post("/action", {"action": agent_server.START_OVER, "payload": {}}), {"result": {"startedOver": True}})
        failed = self.server.until(turn, ev.FAILED)
        self.assertIn("started over from the agent's card", failed["error"])
        status, kept = self.server.call("GET", f"/history?turn={turn}")
        self.assertEqual((status, kept["messages"][0]), (200, {"role": "user", "content": "slow"}),
                         "the conversation before the last start over is kept")
        self.assertIsNone(self.server.conversation.conversation_id())
        self.server.until(self.turn("hello"), ev.DONE)
        self.assertNotIn("--resume", self.logged()[-1]["args"])
        # Kept on disk, for a restarted server.
        self.server.close()
        self.server = self.start()
        self.assertEqual(self.server.call("GET", f"/history?turn={turn}")[0], 200)
        self.server.post("/reset", {})
        status, gone = self.server.call("GET", f"/history?turn={turn}")
        self.assertEqual(status, 404)
        self.assertIn(f"holds no message of turn {turn}: it was started over twice since", gone["error"])

    def test_two_tool_calls_at_once_are_answered_one_after_the_other(self):
        self.configure("claude")
        turn = self.turn("twice", tools=LOOKUP)
        first = self.server.until(turn, ev.WAITING_TOOL)
        conv = self.server.conversation
        with conv.lock:
            self.assertTrue(conv.lock.wait_for(lambda: len(conv.state.get("waitingOn", [])) == 2, 20), "the second call never came")
        second_id = [i for k, i in self.server.conversation.state["waitingOn"] if i != first["pending"]["callId"]][0]
        status, refused = self.server.call("POST", "/turns", {"toolResult": {"callId": second_id, "output": "x"}})
        self.assertEqual(status, 409)
        self.assertIn(f"it waits on tool_call '{first['pending']['callId']}' first", refused["error"])
        key = json.loads(first["pending"]["arguments"])["key"]
        self.server.post("/turns", {"toolResult": {"callId": first["pending"]["callId"], "output": f"out-{key}"}})
        second = self.server.until(turn, ev.WAITING_TOOL, after=first["pending"]["n"])
        self.assertEqual(second["pending"]["callId"], second_id, "answering the first shows the next")
        self.assertEqual(self.server.conversation.state["waitingOn"], [[ev.TOOL_CALL, second_id]])
        key = json.loads(second["pending"]["arguments"])["key"]
        self.server.post("/turns", {"toolResult": {"callId": second_id, "output": f"out-{key}"}})
        self.assertEqual(json.loads(self.server.until(turn, ev.DONE)["reply"]), ["out-0", "out-1"])

    def test_firings_reading_two_calls_at_once_see_every_event_once(self):
        # As AskAgent does: each firing starts from the `after` `/turns`
        # answered, streams what it reads, and ends on a call or the reply.
        self.configure("claude")
        answer = self.server.post("/turns", {"message": "twice", "tools": LOOKUP})
        turn, streamed = answer["turn"], []
        while True:
            after = answer["after"]
            while True:
                read = self.server.call("GET", f"/events?turn={turn}&after={after}&wait=5")[1]
                streamed += [e["n"] for e in read["events"] if e["n"] > after]
                after = max([after, *(e["n"] for e in read["events"])])
                if read["status"] == ev.DONE or read["pending"] is not None:
                    break
            if read["status"] == ev.DONE:
                break
            key = json.loads(read["pending"]["arguments"])["key"]
            answer = self.server.post("/turns", {"toolResult": {"callId": read["pending"]["callId"], "output": f"out-{key}"}})
        self.assertEqual(json.loads(read["reply"]), ["out-0", "out-1"])
        every = [e["n"] for e in self.server.conversation.events if e["turn"] == turn]
        self.assertEqual(streamed, every, "every event streamed exactly once, in order")

    def test_the_history_of_a_turn_is_what_it_was_when_that_turn_ended(self):
        self.configure("claude")
        first, second = self.turn("hello"), self.turn("again")
        self.server.until(first, ev.DONE)
        self.server.until(second, ev.DONE)
        history = self.server.call("GET", f"/history?turn={first}")[1]["messages"]
        self.assertEqual(history, [{"role": "user", "content": "hello"}, {"role": "assistant", "content": "on it: hello"}])
        self.assertEqual(len(self.server.call("GET", f"/history?turn={second}")[1]["messages"]), 4)
        status, refused = self.server.call("GET", "/history")
        self.assertEqual((status, "a history read names its turn" in refused["error"]), (400, True))
        status, refused = self.server.call("GET", "/history?turn=nope")
        self.assertEqual(status, 404)
        running = self.turn("slow")
        self.server.until(running, ev.WORKING)
        status, refused = self.server.call("GET", f"/history?turn={running}")
        self.assertEqual((status, refused["error"]), (409, f"turn {running} has not ended yet; read its history once it has"))

    def test_no_answer_is_written_while_the_conversation_is_held(self):
        self.configure("claude")
        conv, held = self.server.conversation, []
        send = agent_server.Handler._send

        def watched(handler, status, body):
            held.append(conv.lock._is_owned())
            send(handler, status, body)

        with unittest.mock.patch.object(agent_server.Handler, "_send", watched):
            for path in ("/health", "/live", "/events?turn=nope"):
                self.server.call("GET", path)
        self.assertEqual(held, [False] * 3)

    def test_a_post_that_is_not_json_is_refused(self):
        self.configure("claude")
        host, port = self.server.http.server_address
        for content_type in ("text/plain", "application/x-www-form-urlencoded", None):
            # http.client, which adds no Content-Type of its own (urllib does).
            connection = http.client.HTTPConnection(host, port, timeout=10)
            self.addCleanup(connection.close)
            headers = {"Content-Type": content_type} if content_type else {}
            connection.request("POST", "/turns", body=b'{"message": "hi"}', headers=headers)
            answer = connection.getresponse()
            self.assertEqual(answer.status, 415, content_type)
            self.assertEqual(json.loads(answer.read())["error"], "this server takes a JSON body (Content-Type: application/json), "
                             f"and got {content_type or 'no Content-Type'}")
        self.assertEqual(self.server.conversation.turns, {})
        request = urllib.request.Request(self.server.url + "/configure", method="POST",
                                         data=json.dumps({"agent": "claude", "folder": self.folder}).encode(),
                                         headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(request, timeout=10) as answer:
            self.assertEqual(answer.status, 200, "a charset is still JSON")

    def test_a_turn_before_any_configuration_is_refused(self):
        status, answer = self.server.call("POST", "/turns", {"message": "hi", "tools": []})
        self.assertEqual(status, 409)
        self.assertIn("never configured", answer["error"])

    def test_the_card_shows_the_status_and_a_start_over_button(self):
        self.configure("claude")
        items = self.server.call("GET", "/live")[1]["items"]
        self.assertEqual([i["label"] for i in items], ["Agent", "Status", "Conversation"])
        self.assertEqual(items[0]["data"], "Claude Code")
        self.assertEqual(items[2]["action"]["actionKind"], agent_server.START_OVER)


class ClaudeCommandTest(unittest.TestCase):
    def config(self, **more):
        return {"agent": "claude", "folder": "/w", "permission": "ask", "options": {}, **more}

    def test_ask_is_the_manual_mode_with_every_prompt_sent_here(self):
        cmd = adapter_claude.command(self.config(), None, [])
        self.assertEqual(cmd[cmd.index("--permission-mode") + 1], "manual")
        self.assertEqual(cmd[cmd.index("--permission-prompt-tool") + 1], "stdio")
        self.assertNotIn("--bare", cmd)
        self.assertNotIn("--resume", cmd)

    def test_bypass_is_the_mode_and_the_flag_that_allows_it(self):
        cmd = adapter_claude.command(self.config(permission="bypass"), None, [])
        self.assertEqual(cmd[cmd.index("--permission-mode") + 1], "bypassPermissions")
        self.assertIn("--allow-dangerously-skip-permissions", cmd)

    def test_every_option_becomes_its_flag(self):
        options = {"effort": "high", "appendSystemPrompt": "be brief", "addDirs": ["/a", "/b"], "allowedTools": ["Read"],
                   "disallowedTools": ["WebFetch"], "fallbackModel": "sonnet", "chrome": True, "remoteControl": "build",
                   "agents": {"reviewer": {"description": "d", "prompt": "p"}}}
        cmd = adapter_claude.command(self.config(model="opus", streamBy="token", options=options), "s1", [])
        flags = " ".join(cmd)
        for expected in ("--model opus", "--effort high", "--append-system-prompt be brief", "--add-dir /a --add-dir /b",
                         "--allowedTools Read", "--disallowedTools WebFetch", "--fallback-model sonnet", "--chrome",
                         "--remote-control build", "--include-partial-messages", "--resume s1"):
            self.assertIn(expected, flags)
        self.assertEqual(json.loads(cmd[cmd.index("--agents") + 1]), options["agents"])

    def test_an_unknown_permission_setting_is_refused(self):
        with self.assertRaises(Refused):
            adapter_claude.command(self.config(permission="sometimes"), None, [])

    def test_every_program_tool_name_joins_the_persons_disallowed_tools_and_the_server_is_allowed(self):
        tools = [{"name": "WebSearch"}, {"name": "lookup"}, {"name": "Read"}]
        cmd = adapter_claude.command(self.config(options={"disallowedTools": ["Read", "Bash"], "allowedTools": ["Edit"]}), None, tools)
        self.assertEqual(cmd[cmd.index("--disallowedTools") + 1], "Read,Bash,WebSearch,lookup")
        self.assertEqual(cmd[cmd.index("--allowedTools") + 1], "Edit,mcp__weft")
        self.assertNotIn("--mcp-config", adapter_claude.command(self.config(), None, []), "no tools, no server")

    def test_exactly_one_login_reaches_claude(self):
        env = {"ANTHROPIC_API_KEY": "old", "CLAUDE_CODE_OAUTH_TOKEN": "old"}
        adapter_claude.credential_env(env, {"way": "subscription", "values": {"token": "t"}})
        wait = {"MCP_TOOL_TIMEOUT": "2147483647", "CLAUDE_AGENT_SDK_MCP_NO_PREFIX": "1", "DISABLE_AUTOUPDATER": "1"}
        self.assertEqual(env, {"CLAUDE_CODE_OAUTH_TOKEN": "t", **wait})
        adapter_claude.credential_env(env, {"way": "apiKey", "values": {"key": "k"}})
        self.assertEqual(env, {"ANTHROPIC_API_KEY": "k", **wait})
        adapter_claude.credential_env(env, None)
        self.assertEqual(env, wait, "the machine's own login: neither variable")


class CodexTest(Rig):
    def test_an_approval_a_question_and_the_programs_tool_each_wait_for_weft(self):
        self.configure("codex")
        turn = self.turn("run it", tools=LOOKUP)
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual((question["kind"], question["permission"]["tool"]), ("permission", "shell"))
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "acceptForSession"}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "acceptForSession")

        turn = self.turn("ask")
        question = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual(question["questions"][0]["id"], "colour")
        self.server.post("/turns", {"answer": {"questionId": question["id"], "answers": {"colour": ["purple"]}}})
        self.assertEqual(json.loads(self.server.until(turn, ev.DONE)["reply"]), {"answers": {"colour": {"answers": ["purple"]}}})

        turn = self.turn("tool", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        handed = self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42", "isError": True}})
        self.assertEqual(handed, {"turn": turn, "after": call["n"]}, "the firing reads on after the call it answers")
        self.assertEqual(json.loads(self.server.until(turn, ev.DONE)["reply"]),
                         {"success": False, "contentItems": [{"type": "inputText", "text": "42"}]})
        sent = [m for m in self.logged() if m.get("method") == "thread/start"][0]["params"]
        self.assertEqual(sent["model"], "big", "the default the login lists, never the machine's config")
        self.assertEqual(sent["approvalPolicy"], "untrusted")
        self.assertEqual(sent["dynamicTools"][0]["name"], "lookup")

    def test_a_program_tool_named_like_a_built_in_turns_it_off_and_replaces_it(self):
        self.configure("codex")
        tools = [{"name": "web_search", "description": "the program's", "parameters": {"type": "object", "properties": {}}}]
        turn = self.turn("search", tools=tools)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual(call["name"], "web_search")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program searched"}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "the program searched")
        config = [m for m in self.logged() if m.get("method") == "thread/start"][0]["params"]["config"]
        self.assertEqual(config, {"features.tool_registry.error_on_tool_collisions": True, "web_search": "disabled"})
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        self.assertEqual([c["name"] for m in history for c in m.get("tool_calls") or []], ["web_search"], "written once")

    def test_a_built_in_codex_cannot_give_up_fails_the_turn_loudly(self):
        self.configure("codex")
        tools = [{"name": "exec_command", "parameters": {"type": "object", "properties": {}}}]
        failed = self.server.until(self.turn("hello", tools=tools), ev.FAILED)
        self.assertIn("duplicate tool: functions.exec_command", failed["error"])

    def test_a_name_codex_reserves_is_refused_saying_so(self):
        self.configure("codex")
        failed = self.server.until(self.turn("hello", tools=[{"name": "mcp__x"}]), ev.FAILED)
        self.assertIn("dynamic tool name is reserved: mcp__x", failed["error"])

    def test_a_failed_turn_says_why(self):
        self.configure("codex")
        answer = self.server.until(self.turn("fail please"), ev.FAILED)
        self.assertIn("the model refused", answer["error"])

    def chatgpt(self, token: str) -> dict:
        """A ChatGPT login as the node hands it: the token and account weft holds."""
        return {"way": "subscription", "values": {"token": token, "accountId": "acct-1"}}

    def asks(self, turn: str) -> list:
        """The asks for a fresh login the turn made so far, by reason."""
        events = self.server.call("GET", f"/events?turn={turn}&after=0&wait=0")[1]["events"]
        return [e["reason"] for e in events if e["type"] == ev.LOGIN]

    def until_asked(self, turn: str) -> list:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            asked = self.asks(turn)
            if asked:
                return asked
            time.sleep(0.02)
        raise AssertionError(f"turn {turn} never asked for a login")

    def test_a_chatgpt_login_reaches_codex_as_its_token_and_no_file(self):
        self.configure("codex", credentials=self.chatgpt("tok-1"))
        self.assertEqual(self.server.until(self.turn("whoami"), ev.DONE)["reply"], "tok-1")
        login = [m for m in self.logged() if m.get("method") == "account/login/start"][0]["params"]
        self.assertEqual(login, {"type": "chatgptAuthTokens", "accessToken": "tok-1", "chatgptAccountId": "acct-1", "chatgptPlanType": None})
        self.assertNotIn("auth.json", os.listdir(os.path.join(self.home, "codex-home")), "Codex holds it in memory alone")

    def test_a_refused_token_asks_weft_and_the_login_weft_renewed_answers_codex(self):
        self.configure("codex", credentials=self.chatgpt("tok-1"))
        turn = self.turn("expired")
        self.assertEqual(self.until_asked(turn), ["unauthorized"])
        # The node hands what the connection holds first: Codex holds it,
        # so the node renews, and the renewed login answers Codex's refresh.
        self.assertEqual(self.server.post("/login", {"credentials": self.chatgpt("tok-1")}), {"taken": False})
        self.assertEqual(self.server.post("/login", {"credentials": self.chatgpt("tok-2")}), {"taken": True})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "tok-2")
        self.assertEqual(self.asks(turn), ["unauthorized"], "asked once")
        # A process started again (another configuration) starts on it.
        self.configure("codex", credentials=self.chatgpt("tok-2"), model="small")
        self.assertEqual(self.server.until(self.turn("whoami"), ev.DONE)["reply"], "tok-2")

    def test_a_configure_that_changes_only_the_login_hands_it_to_the_running_codex(self):
        self.configure("codex", credentials=self.chatgpt("tok-1"))
        self.server.until(self.turn("whoami"), ev.DONE)
        # Another node's renewal reached the connection, and this run's
        # configure hands it: Codex takes it with no restart.
        self.configure("codex", credentials=self.chatgpt("tok-2"))
        self.assertEqual(self.server.until(self.turn("whoami"), ev.DONE)["reply"], "tok-2")
        self.assertEqual(len([m for m in self.logged() if m.get("method") == "initialize"]), 1, "the same process")

    def test_a_token_about_to_die_is_asked_for_before_codex_is_refused(self):
        dying = jwt({"exp": int(time.time()) + 60})
        self.configure("codex", credentials=self.chatgpt(dying))
        turn = self.turn("whoami")
        self.server.until(turn, ev.DONE)
        self.assertEqual(self.asks(turn), ["expiring"])
        self.assertEqual(self.server.post("/login", {"credentials": self.chatgpt("tok-2")}), {"taken": True})
        logins = [m["params"]["accessToken"] for m in self.logged() if m.get("method") == "account/login/start"]
        self.assertEqual(logins, [dying, "tok-2"], "logged in again while it runs")
        self.assertEqual(self.server.until(self.turn("whoami"), ev.DONE)["reply"], "tok-2")

    def test_a_fresh_login_that_dies_soon_is_not_asked_for_again(self):
        # weft's freshest login already dies within the lead: asking before
        # it dies would only bring it back, and ask again.
        self.configure("codex", credentials=self.chatgpt(jwt({"exp": int(time.time()) + 864000})))
        self.server.until(self.turn("whoami"), ev.DONE)
        self.configure("codex", credentials=self.chatgpt(jwt({"exp": int(time.time()) + 60})))
        turn = self.turn("whoami")
        self.server.until(turn, ev.DONE)
        self.assertEqual(self.asks(turn), [])

    def test_a_login_weft_could_not_renew_fails_the_turn_with_why(self):
        self.configure("codex", credentials=self.chatgpt("tok-1"))
        turn = self.turn("expired")
        self.until_asked(turn)
        self.server.post("/login", {"error": "Codex needs a fresh login, and weft could not renew its connection: reconnect it"})
        failed = self.server.until(turn, ev.FAILED)
        self.assertIn("weft could not renew its connection: reconnect it", failed["error"])

    def test_a_login_handed_to_an_agent_that_takes_none_while_it_runs_is_refused(self):
        self.configure("codex", credentials={"way": "apiKey", "values": {"key": "sk"}})
        self.server.until(self.turn("hello"), ev.DONE)
        status, refused = self.server.call("POST", "/login", {"credentials": {"way": "apiKey", "values": {"key": "sk-2"}}})
        self.assertEqual((status, "takes no login while it runs" in refused["error"]), (409, True))
        status, refused = self.server.call("POST", "/login", {"credentials": self.chatgpt("tok-1")})
        self.assertEqual((status, "configured with the login way 'apiKey'" in refused["error"]), (409, True))

    def test_a_tool_codex_gave_up_on_stops_its_wait_and_a_late_result_reads_the_rest_of_the_turn(self):
        self.configure("codex")
        turn = self.turn("abandon", tools=LOOKUP)
        done = self.server.until(turn, ev.DONE)
        call, gave_up = done["seen"][0], done["seen"][1]
        self.assertEqual((call["type"], call["callId"]), (ev.TOOL_CALL, "exec-tool"))
        self.assertEqual((gave_up["type"], gave_up["id"], gave_up["isError"]), (ev.TOOL_RESULT, "exec-tool", True))
        self.assertIn("the agent stopped waiting for this call", gave_up["output"])
        # A restarted server no longer has the turn, and says so.
        with self.assertRaises(Refused) as refused:
            agent_server.Conversation(self.home).deliver(ev.TOOL_CALL, "exec-tool", {"output": "late"})
        self.assertIn("the agent stopped waiting for it, and the agent server restarted since", str(refused.exception))
        # The late result is not handed to the agent: its firing reads the
        # rest of the turn from just after the call.
        late = self.server.post("/turns", {"toolResult": {"callId": "exec-tool", "output": "late"}})
        self.assertEqual(late, {"turn": turn, "after": call["n"]})

    def test_a_tool_answered_with_an_error_ends_failed_and_the_turn_goes_on(self):
        # Codex ends a call answered `success: false` with a failed item, the
        # same item it gives up with: an answered call is never given up on.
        self.configure("codex")
        turn = self.turn("tool", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "no such key", "isError": True}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(json.loads(done["reply"])["success"], False)
        self.assertNotIn("the agent stopped waiting", json.dumps(done["seen"]))

    def test_codex_giving_up_on_a_call_weft_does_not_wait_on_fails_the_turn_naming_both(self):
        self.configure("codex")
        failed = self.server.until(self.turn("strays", tools=LOOKUP), ev.FAILED)
        self.assertIn("the agent gave up on call 'another-call', and the turn waits on 'exec-tool'", failed["error"])

    def test_a_tool_call_with_no_call_id_fails_the_turn(self):
        self.configure("codex")
        failed = self.server.until(self.turn("nocallid", tools=LOOKUP), ev.FAILED)
        self.assertIn("Codex called the program's tool 'lookup' with no callId", failed["error"])

    def test_a_turn_weft_failed_on_its_side_stops_the_agent_and_the_next_turn_resumes(self):
        self.configure("codex")
        self.server.until(self.turn("strays", tools=LOOKUP), ev.FAILED)
        self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "done", "nothing of the failed turn carries")
        sent = [m["method"] for m in self.logged() if m.get("method") in ("initialize", "thread/start", "thread/resume")]
        self.assertEqual(sent, ["initialize", "thread/start", "initialize", "thread/resume"])

    def test_a_handshake_that_fails_stops_the_agent_it_started(self):
        config = {"agent": "codex", "folder": self.folder, "credentials": {"way": "apiKey", "values": {"key": ""}}}
        self.configure("codex")
        adapter = adapters.KINDS["codex"](self.server.conversation, config, [])
        with self.assertRaises(Refused) as refused:
            adapter.ensure_started()
        self.assertIn("the connection's 'key' is empty", str(refused.exception))
        self.assertIsNotNone(adapter.proc.poll(), "stopped, so the next turn starts another and handshakes")
        self.assertTrue(adapter.retired, "never started again: the next turn builds a new adapter")
        with self.assertRaises(Refused) as refused:
            adapter.ensure_started()
        self.assertIn("was stopped for good", str(refused.exception))
        self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "done")
        self.assertEqual([m["method"] for m in self.logged() if m.get("method") == "initialize"], ["initialize"] * 2)

    def test_a_handshake_that_fails_and_a_stop_that_fails_beside_it_are_both_said(self):
        self.configure("codex", credentials={"way": "apiKey", "values": {"key": ""}})
        with unittest.mock.patch.object(adapter_codex.Codex, "stop_rest", side_effect=OSError(5, "Input/output error")):
            failed = self.server.until(self.turn("hello"), ev.FAILED)
            self.assertIn("the connection's 'key' is empty", failed["error"])
            self.assertIn("Stopping Codex then failed too: the agent server could not stop Codex: OSError", failed["error"])
            status, refused = self.server.call("POST", "/turns", {"message": "again"})
            self.assertEqual(status, 503)
            self.assertIn("could not stop Codex", refused["error"])
        self.configure("codex")
        self.assertEqual(self.server.until(self.turn("hello"), ev.DONE)["reply"], "done", "the configure stopped it")

    def test_an_answer_after_a_restart_or_a_start_over_says_what_became_of_its_call(self):
        home = self.home + "-gone"
        conv = agent_server.Conversation(home)
        turn = agent_server.Turn("t", "m", LOOKUP)
        conv.current, turn.status = turn, ev.WORKING
        conv.block(turn, ev.QUESTION, "q-1", {"question": {}}, lambda payload: None)
        restarted = agent_server.Conversation(home)
        with self.assertRaises(Refused) as refused:
            restarted.deliver(ev.QUESTION, "q-1", {})
        self.assertIn("the agent server restarted while the agent waited on it", str(refused.exception))
        with self.assertRaises(Refused) as refused:
            agent_server.Conversation(home).deliver(ev.QUESTION, "q-1", {})
        self.assertIn("it waits on nothing", str(refused.exception), "said once, by the life that restarted")

        conv.fail(turn, "the conversation was started over from the agent's card while this turn ran; send the message again")
        with self.assertRaises(Refused) as refused:
            conv.deliver(ev.QUESTION, "q-1", {})
        self.assertIn("its turn ended first: the conversation was started over", str(refused.exception))

    def test_inside_a_computer_the_sandbox_is_the_computer(self):
        params = adapter_codex.thread_params({"folder": "/w", "permission": "ask", "inComputer": True, "options": {"sandbox": "read_only"}}, [], "m")
        self.assertEqual(params["sandbox"], "danger-full-access")
        params = adapter_codex.thread_params({"folder": "/w", "permission": "ask", "options": {"sandbox": "read_only"}}, [], "m")
        self.assertEqual(params["sandbox"], "read-only")
        params = adapter_codex.thread_params({"folder": "/w", "permission": "bypass", "options": {}}, [], "m")
        self.assertEqual((params["sandbox"], params["approvalPolicy"]), ("danger-full-access", "never"))


WEB_SEARCH = [{"name": "WebSearch", "description": "The program's own search", "parameters": {"type": "object", "properties": {}}}]


class ClaudeToolsTest(Rig):
    """The program's tools over the sdk MCP server, by their plain names."""

    def test_the_tools_ride_an_sdk_server_the_handshake_declares_with_plain_names(self):
        self.configure("claude")
        turn = self.turn("use the tool", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        self.server.until(turn, ev.DONE)
        started = self.logged()[0]
        args = started["args"]
        self.assertEqual(json.loads(args[args.index("--mcp-config") + 1]),
                         {"mcpServers": {"weft": {"type": "sdk", "name": "weft", "alwaysLoad": True}}})
        self.assertEqual(started["env"]["CLAUDE_AGENT_SDK_MCP_NO_PREFIX"], "1")
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        calls = [c for m in history for c in m.get("tool_calls") or []]
        self.assertEqual([(c["id"], c["name"]) for c in calls], [(call["callId"], "lookup")], "written once, by the route")
        self.assertIn({"role": "tool", "tool_call_id": call["callId"], "content": "42"}, history)

    def test_a_program_tool_named_like_a_built_in_replaces_it(self):
        self.configure("claude")
        turn = self.turn("search the web", tools=WEB_SEARCH)
        waiting = self.server.until(turn, ev.WAITING_TOOL)
        call = waiting["pending"]
        self.assertEqual(call["name"], "WebSearch", "toolCall gives the plain name back")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program searched"}})
        done = self.server.until(turn, ev.DONE, after=waiting["after"])
        self.assertEqual(done["reply"], "the program searched")
        self.assertNotIn(ev.TOOL_USE, [e["type"] for e in waiting["seen"] + done["seen"]], "never shown as the agent's own")
        history = self.server.call("GET", f"/history?turn={turn}")[1]["messages"]
        calls = [(c["id"], c["name"]) for m in history for c in m.get("tool_calls") or []]
        self.assertEqual(calls, [(call["callId"], "WebSearch")], "labelled the program's: the id its route gave it")

    def test_the_agents_own_built_in_stays_its_own(self):
        self.configure("claude")
        done = self.server.until(self.turn("search the web", tools=LOOKUP), ev.DONE)
        self.assertEqual(done["reply"], "the built-in searched")
        used = [e for e in done["seen"] if e["type"] == ev.TOOL_USE]
        self.assertEqual([(e["id"], e["name"]) for e in used], [("toolu_search", "WebSearch")])


class TurnKeyTest(Rig):
    """A post repeated with its key starts nothing (a node that crashed after posting)."""

    def test_a_message_posted_twice_with_one_key_starts_one_turn_even_after_a_restart(self):
        self.configure("claude")
        first = self.server.post("/turns", {"message": "hello", "key": "run-1/ask@0"})
        again = self.server.post("/turns", {"message": "hello", "key": "run-1/ask@0"})
        self.assertEqual(again, first)
        self.server.until(first["turn"], ev.DONE)
        other = self.server.post("/turns", {"message": "hello", "key": "run-1/ask@1"})
        self.assertNotEqual(other["turn"], first["turn"], "another key is another turn")
        self.server.until(other["turn"], ev.DONE)
        sent = [m for m in self.server.conversation.state["history"] if m["role"] == "user"]
        self.assertEqual(len(sent), 2, "the agent was prompted once per key")
        # Kept on disk: a restarted server still answers the turn it started.
        self.server.close()
        self.server = self.start()
        self.configure("claude")
        self.assertEqual(self.server.post("/turns", {"message": "hello", "key": "run-1/ask@1"}), other)
        self.assertEqual(self.server.call("GET", f"/events?turn={other['turn']}&after=0")[1]["status"], ev.LOST)

    def test_a_tool_result_posted_twice_with_one_key_is_handed_over_once(self):
        self.configure("codex")
        turn = self.turn("tool", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        result = {"toolResult": {"callId": call["callId"], "output": "42"}, "key": "run-1/ask@1"}
        first = self.server.post("/turns", result)
        self.assertEqual(self.server.post("/turns", result), first)
        self.assertEqual(json.loads(self.server.until(turn, ev.DONE)["reply"])["contentItems"][0]["text"], "42")

    def test_a_key_that_is_not_a_string_is_refused(self):
        self.configure("claude")
        status, refused = self.server.call("POST", "/turns", {"message": "hello", "key": 7})
        self.assertEqual(status, 400)
        self.assertIn("key is a non-empty string", refused["error"])


class TrimTest(Rig):
    """The events of a turn that ended and was read whole go once a later turn is posted."""

    def test_an_ended_turn_its_node_read_whole_is_dropped_and_reading_it_again_says_so(self):
        self.configure("claude")
        first = self.server.post("/turns", {"message": "hello", "key": "k-first"})["turn"]
        self.server.until(first, ev.DONE)
        self.server.until(self.turn("again"), ev.DONE)
        self.assertEqual(self.server.call("GET", f"/events?turn={first}&after=0")[0], 200,
                         "its end was handed out, and its node has not shown it holds it")
        # AskAgent reads the history once it saved the end: that is the node holding it all.
        self.assertEqual(self.server.call("GET", f"/history?turn={first}")[0], 200)
        third = self.turn("once more")
        status, gone = self.server.call("GET", f"/events?turn={first}&after=0")
        self.assertEqual(status, 410)
        self.assertIn(f"the events of turn {first} are gone", gone["error"])
        self.assertEqual(self.server.call("GET", f"/history?turn={first}")[0], 200, "the history is kept")
        self.assertNotIn("k-first", self.server.conversation.state["turnKeys"], "its key goes with it")
        self.server.until(third, ev.DONE)

    def test_a_turn_read_from_its_last_event_or_ended_lost_and_handed_out_is_dropped(self):
        self.configure("claude")
        done = self.turn("hello")
        last = self.server.until(done, ev.DONE)["after"]
        self.server.call("GET", f"/events?turn={done}&after={last}")
        lost = self.turn("crash")
        self.server.until(lost, ev.LOST)
        self.turn("again")
        for turn in (done, lost):
            self.assertEqual(self.server.call("GET", f"/events?turn={turn}&after=0")[0], 410, turn)

    def test_a_running_turn_and_an_unread_one_are_kept(self):
        self.configure("claude")
        unread = self.turn("hello")
        conv = self.server.conversation
        with conv.lock:
            self.assertTrue(conv.lock.wait_for(lambda: conv.turns[unread].status == ev.DONE, 20))
        running = self.turn("slow")
        self.server.until(running, ev.WORKING)
        self.turn("again")
        self.assertEqual(self.server.until(unread, ev.DONE)["reply"], "done", "never read, so kept")
        self.assertEqual(self.server.call("GET", f"/events?turn={running}&after=0")[1]["status"], ev.WORKING)


class McpRouteTest(Rig):
    """weft's MCP server as a plugin calls it: plain JSON-RPC posts, no session."""

    def post(self, message):
        request = urllib.request.Request(self.server.url + agent_server.MCP_PATH, data=json.dumps(message).encode(),
                                         method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as answer:
            body = answer.read()
            return answer.status, json.loads(body) if body else None

    def test_a_plain_tools_call_with_no_initialize_waits_for_weft(self):
        self.configure("claude")
        turn = self.turn("slow", tools=LOOKUP)
        self.server.until(turn, ev.WORKING)
        answers = []
        caller = threading.Thread(target=lambda: answers.append(self.post(
            {"jsonrpc": "2.0", "id": "p-1", "method": "tools/call", "params": {"name": "lookup", "arguments": {"key": "y"}}})))
        caller.start()
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual((call["name"], json.loads(call["arguments"])), ("lookup", {"key": "y"}))
        self.assertTrue(caller.is_alive(), "held open until weft answers")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "from weft"}})
        caller.join(20)
        self.assertEqual(answers, [(200, {"jsonrpc": "2.0", "id": "p-1",
                                          "result": {"content": [{"type": "text", "text": "from weft"}], "isError": False}})])
        history = self.server.conversation.state["history"]
        self.assertIn("lookup", [c["name"] for m in history for c in m.get("tool_calls") or []], "the route labels it")
        self.server.conversation.adapter.send({"type": "interrupt"})
        self.server.until(turn, ev.DONE)

    def test_a_name_that_is_not_the_programs_is_an_error_result(self):
        self.configure("claude")
        turn = self.turn("slow", tools=LOOKUP)
        self.server.until(turn, ev.WORKING)
        status, answer = self.post({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "mcp__weft__lookup"}})
        self.assertEqual((status, answer["result"]["isError"]), (200, True))
        self.assertIn("no tool 'mcp__weft__lookup'", answer["result"]["content"][0]["text"])
        self.server.conversation.adapter.send({"type": "interrupt"})
        self.server.until(turn, ev.DONE)

    def test_every_method_it_does_not_serve_is_method_not_found_and_a_notification_gets_nothing(self):
        for method in ("server/discover", "resources/list", "prompts/list", "completion/complete"):
            status, answer = self.post({"jsonrpc": "2.0", "id": 9, "method": method, "params": {}})
            self.assertEqual((status, answer["error"]["code"]), (200, -32601), method)
            self.assertNotIn("result", answer, f"{method}: an error, never an empty result")
        self.assertEqual(self.post({"jsonrpc": "2.0", "method": "notifications/initialized"}), (202, None))
        self.assertEqual(self.post({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})[1]["result"], {"tools": []})


def submitted(conv, message, tools, start_over, key=None):
    """The turn `conv.submit` queued."""
    return conv.turns[conv.submit(message, tools, start_over, key)["turn"]]


def call_fields(call_id: str) -> dict:
    """The fields of a `tool_call` event of the program's `lookup`."""
    return {"callId": call_id, "name": "lookup", "arguments": "{}"}


class ConversationTest(unittest.TestCase):
    """The server's own bookkeeping, with no agent: a turn is made current by hand."""

    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)

    def running(self, conv=None):
        conv = conv or agent_server.Conversation(self.home)
        turn = agent_server.Turn("t", "m", LOOKUP)
        conv.turns[turn.id], conv.current, turn.status = turn, turn, ev.WORKING
        return conv, turn

    def test_a_notice_shows_on_the_card_until_the_turns_next_event(self):
        conv, turn = self.running()
        status = lambda: next(i["data"] for i in conv.live_items() if i["label"] == "Status")
        conv.notice(turn, "retrying a refused request")
        self.assertEqual(status(), "working (retrying a refused request)")
        conv.emit(turn, ev.TEXT, text="hi")
        self.assertEqual(status(), "working")
        conv.finish(turn, "hi")
        conv.notice(turn, "late")
        self.assertIsNone(turn.notice, "a turn that ended takes no notice")

    def test_a_read_that_sees_the_turn_ended_carries_its_last_event(self):
        conv, turn = self.running()
        answers = []
        reader = threading.Thread(target=lambda: answers.append(conv.read(turn.id, 0, 10)))
        reader.start()
        conv.emit(turn, ev.TEXT, text="hi")
        conv.finish(turn, "hi")
        reader.join()
        answer = answers[0]
        if answer["status"] == ev.DONE:
            self.assertEqual([e["type"] for e in answer["events"]], [ev.TEXT, ev.DONE])
        self.assertEqual(conv.read(turn.id, 0, 0)["events"][-1]["type"], ev.DONE)

    def test_the_state_file_is_readable_by_its_user_alone(self):
        temp = os.path.join(self.home, "state.json.weft-tmp")
        with open(temp, "w") as f:
            f.write("left by an older write")
        os.chmod(temp, 0o644)
        conv = agent_server.Conversation(self.home)
        conv.save_state()
        self.assertEqual(stat.S_IMODE(os.stat(os.path.join(self.home, "state.json")).st_mode), 0o600)

    def test_every_call_waited_on_is_explained_after_a_restart(self):
        conv, turn = self.running()
        conv.block(turn, ev.TOOL_CALL, "c-1", call_fields("c-1"), lambda payload: None)
        conv.block(turn, ev.TOOL_CALL, "c-2", call_fields("c-2"), lambda payload: None)
        self.assertEqual((turn.status, turn.pending.id), (ev.WAITING_TOOL, "c-1"))
        restarted = agent_server.Conversation(self.home)
        for call in ("c-1", "c-2"):
            with self.assertRaises(Refused) as refused:
                restarted.deliver(ev.TOOL_CALL, call, {})
            self.assertIn("the agent server restarted while the agent waited on it", str(refused.exception))

    def test_a_refused_answer_never_reopens_a_turn_that_ended_meanwhile(self):
        conv, turn = self.running()

        def answer(payload):
            conv.fail(turn, "the agent died", status=ev.LOST)
            raise Refused("the agent is no longer running")

        conv.block(turn, ev.QUESTION, "q-1", {}, answer)
        with self.assertRaises(Refused):
            conv.deliver(ev.QUESTION, "q-1", {})
        self.assertEqual((turn.status, turn.waiting), (ev.LOST, []))

    def test_a_refused_answer_leaves_the_call_waiting(self):
        conv, turn = self.running()

        def answer(payload):
            raise Refused("not a choice it offered", 400)

        conv.block(turn, ev.QUESTION, "q-1", {}, answer)
        with self.assertRaises(Refused):
            conv.deliver(ev.QUESTION, "q-1", {})
        self.assertEqual((turn.status, turn.pending.id, turn.pending.answering), (ev.WAITING_QUESTION, "q-1", False))

    def test_an_answer_the_adapter_cannot_hand_over_fails_the_turn(self):
        conv, turn = self.running()

        def answer(payload):
            raise BrokenPipeError("the pipe is gone")

        conv.block(turn, ev.TOOL_CALL, "c-1", call_fields("c-1"), answer)
        with self.assertRaises(Refused) as refused:
            conv.deliver(ev.TOOL_CALL, "c-1", {})
        self.assertEqual(refused.exception.status, 500)
        self.assertEqual(turn.status, ev.FAILED)
        self.assertIn("could not hand the answer to tool_call 'c-1' to the agent: BrokenPipeError: the pipe is gone", turn.error)
        self.assertIsNone(conv.current, "the queue goes on")

    def test_a_reset_forgets_a_restart_owed_to_a_configuration_change(self):
        conv, _ = self.running()
        conv.restart_after_turn = True
        conv.reset("the conversation was started over")
        self.assertFalse(conv.restart_after_turn)

    def test_a_call_after_its_turn_ended_is_refused_so_the_agent_is_answered(self):
        conv, turn = self.running()
        conv.finish(turn, "done")
        with self.assertRaises(Refused) as refused:
            conv.block(turn, ev.QUESTION, "q-1", {}, lambda payload: None)
        self.assertIn("came after its turn ended (done)", str(refused.exception))
        # Each adapter answers the agent's request with that refusal.
        conv = agent_server.Conversation(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, conv.home, True)
        adapter = adapter_codex.Codex(conv, {"agent": "codex", "folder": conv.home}, [])
        sent = []
        adapter.send = sent.append
        adapter.turn = turn
        turn.status = ev.WORKING
        conv.finish(turn, "done")
        adapter.block(turn, ev.QUESTION, "codex-7", {}, lambda answer: None,
                      lambda why: adapter.send({"id": 7, "error": {"code": -32000, "message": why}}))
        self.assertEqual(sent[0]["id"], 7)
        self.assertIn("came after its turn ended", sent[0]["error"]["message"])

    def test_a_failed_end_of_a_call_weft_answered_is_never_a_give_up(self):
        conv, turn = self.running()
        conv.block(turn, ev.TOOL_CALL, "c-1", call_fields("c-1"), lambda payload: conv.stop_waiting(turn, "c-1"))
        conv.deliver(ev.TOOL_CALL, "c-1", {"output": "bad", "isError": True})
        conv.stop_waiting(turn, "c-1")
        self.assertEqual((turn.status, conv.given_up), (ev.WORKING, {}), "ended failed while being answered, and after")

    def test_a_new_adapter_is_built_only_once_the_old_one_has_stopped(self):
        conv = agent_server.Conversation(self.home)
        happened, release, stopping = [], threading.Event(), threading.Event()

        class Old:
            label = "Old"

            def retire(self, on_its_own=False):
                happened.append("stopping")
                stopping.set()
                release.wait(20)
                happened.append("stopped")

        class New:
            def __init__(self, conversation, config, tools):
                # Only this test's: a runner a test before left behind may
                # build this patched kind too.
                self.conv = conversation
                if conversation is conv:
                    happened.append("built")

            def ensure_started(self):
                pass

            def start_turn(self, turn):
                self.conv.finish(turn, "ok")

            def serves(self, tools):
                return True

        with unittest.mock.patch.dict(adapters.KINDS, {"codex": New}):
            conv.config, conv.adapter = {"agent": "codex", "folder": self.home}, Old()
            configuring = threading.Thread(target=conv.configure, args=({"agent": "codex", "folder": self.home, "permission": "bypass"},))
            configuring.start()
            self.assertTrue(stopping.wait(20))
            turn = submitted(conv, "hi", [], False)
            with conv.lock:
                self.assertTrue(conv.lock.wait_for(lambda: conv.current is turn, 20))
            release.set()
            configuring.join()
            with conv.lock:
                self.assertTrue(conv.lock.wait_for(lambda: turn.status == ev.DONE, 20))
        self.assertEqual(happened, ["stopping", "stopped", "built"])

    def test_a_question_the_agent_gave_up_on_reads_gone_and_its_dropped_answer_reads_the_rest_of_the_turn(self):
        conv, turn = self.running()
        conv.block(turn, ev.QUESTION, "q-1", {"question": {"id": "q-1"}}, lambda payload: None)
        self.assertEqual(conv.question("q-1"), {"questionId": "q-1", "gone": False})
        with self.assertRaises(Refused) as refused:
            conv.deliver(ev.QUESTION, "q-1", {"questionId": "q-1", "dropped": True})
        self.assertIn("yet the agent still waits on it", str(refused.exception))
        self.assertEqual(len(turn.waiting), 1, "a refused dropped answer leaves the question waiting")
        conv.stop_waiting(turn, "q-1")
        gone = conv.question("q-1")
        self.assertTrue(gone["gone"])
        self.assertIn("went on with its turn without it", gone["why"])
        asked = turn.events[-2]["n"]
        self.assertEqual(conv.deliver(ev.QUESTION, "q-1", {"questionId": "q-1", "dropped": True}),
                         {"turn": turn.id, "after": asked}, "the answer reads the turn on from the question")

    def test_a_question_whose_turn_ended_or_that_was_never_asked_reads_gone_saying_why(self):
        conv, turn = self.running()
        conv.block(turn, ev.QUESTION, "q-1", {"question": {"id": "q-1"}}, lambda payload: None)
        conv.fail(turn, "the agent crashed")
        gone = conv.question("q-1")
        self.assertTrue(gone["gone"])
        self.assertIn("the agent crashed", gone["why"])
        with self.assertRaises(Refused) as refused:
            conv.deliver(ev.QUESTION, "q-1", {"questionId": "q-1", "dropped": True})
        self.assertIn("the agent crashed", str(refused.exception), "the answer is refused for the same reason")
        self.assertEqual(conv.question("q-9"), {"questionId": "q-9", "gone": True, "why": "the agent server knows no question 'q-9'"})

    def test_a_give_up_during_an_answer_that_fails_is_applied_once_it_has(self):
        conv, turn = self.running()

        def answer(payload):
            conv.stop_waiting(turn, "c-1")
            raise Refused("the agent is no longer reading", 409)

        conv.block(turn, ev.TOOL_CALL, "c-1", call_fields("c-1"), answer)
        with self.assertRaises(Refused):
            conv.deliver(ev.TOOL_CALL, "c-1", {"output": "x"})
        self.assertEqual((turn.waiting, list(conv.given_up)), ([], [(ev.TOOL_CALL, "c-1")]),
                         "the answer never reached the agent, so the agent gave up on the call")
        self.assertEqual(turn.status, ev.WORKING)

    def test_two_start_overs_in_a_row_keep_the_conversation_before_them(self):
        conv, turn = self.running()
        conv.remember(turn, {"role": "user", "content": "m"})
        conv.finish(turn, "done")
        with conv.lock:
            # A configuration of another agent, then a queued start over.
            conv._forget_conversation()
            conv._forget_conversation()
        self.assertEqual(conv.history(turn.id), [{"role": "user", "content": "m"}])

    def test_a_stop_that_fails_refuses_turns_until_a_configure_stops_it(self):
        conv = agent_server.Conversation(self.home)
        attempts = []

        class Stuck:
            label = "Stuck"

            def serves(self, tools):
                return False

            def retire(self, on_its_own=False):
                attempts.append(True)
                if len(attempts) == 1:
                    raise PermissionError(1, "Operation not permitted")

        class New:
            def __init__(self, conversation, config, tools):
                # Only this test's counts: a runner a test before left behind
                # may build this patched kind too.
                self.conv = conversation

            def ensure_started(self):
                pass

            def start_turn(self, turn):
                self.conv.finish(turn, "ok")

            def serves(self, tools):
                return True

        config = {"agent": "codex", "folder": self.home}
        with unittest.mock.patch.dict(adapters.KINDS, {"codex": New}):
            conv.config, conv.adapter = config, Stuck()
            first = submitted(conv, "hi", [], False)
            with conv.lock:
                self.assertTrue(conv.lock.wait_for(lambda: first.status in ev.ENDED, 20))
            self.assertEqual(first.status, ev.FAILED)
            self.assertIn("could not stop Stuck: PermissionError", first.error)
            with self.assertRaises(Refused) as refused:
                submitted(conv, "again", [], False)
            self.assertEqual(refused.exception.status, 503)
            self.assertTrue(str(refused.exception).startswith(first.error), "the failed stop is what refuses it")
            conv.configure(config)
            self.assertEqual(len(attempts), 2, "the configure stopped it")
            second = submitted(conv, "again", [], False)
            with conv.lock:
                self.assertTrue(conv.lock.wait_for(lambda: second.status in ev.ENDED, 20), "the runner survived the failed stop")
            self.assertEqual(second.status, ev.DONE)

    def test_a_failure_to_write_how_a_turn_ended_never_reaches_its_ender(self):
        conv, turn = self.running()
        with unittest.mock.patch.object(conv, "save_state", side_effect=OSError(28, "No space left on device")):
            conv.finish(turn, "done")
        self.assertEqual((turn.status, conv.current, list(conv.trouble)), (ev.DONE, None, [agent_server.TROUBLE_ERROR]))
        self.assertIn("could not write down how turn t ended: OSError", conv.trouble[agent_server.TROUBLE_ERROR])
        conv._recover()
        self.assertEqual(conv.trouble, {}, "a configure or a reset is the person saying it is fixed")

    def test_an_adapter_its_reader_put_down_meanwhile_is_replaced_and_the_turn_runs(self):
        conv = agent_server.Conversation(self.home)
        built = []

        class Agent:
            label = "Agent"

            def __init__(self, conversation, config, tools):
                self.conv = conversation
                if conversation is conv:
                    built.append(self)

            def ensure_started(self):
                if self.conv is conv and len(built) == 1:
                    raise adapters.Retired("put down by its reader as the turn took it", on_its_own)

            def retire(self, on_its_own=False):
                pass

            def start_turn(self, turn):
                self.conv.finish(turn, "ok")

            def serves(self, tools):
                return True

        for on_its_own, status, starts in ((True, ev.DONE, 2), (False, ev.FAILED, 1)):
            built.clear()
            with self.subTest(on_its_own=on_its_own), unittest.mock.patch.dict(adapters.KINDS, {"codex": Agent}):
                conv.config, conv.adapter = {"agent": "codex", "folder": self.home}, None
                turn = submitted(conv, "hi", [], False)
                with conv.lock:
                    self.assertTrue(conv.lock.wait_for(lambda: turn.status in ev.ENDED, 20))
                # One retired by a reset or a configure: no new agent is
                # prompted for its turn.
                self.assertEqual((turn.status, len(built)), (status, starts))

    def test_a_trouble_is_cleared_only_by_the_recovery_of_its_kind(self):
        conv = agent_server.Conversation(self.home)
        conv.trouble = {agent_server.TROUBLE_ERROR: "something else broke", agent_server.TROUBLE_STOP: "a stop failed"}
        conv._clear_trouble(agent_server.TROUBLE_STOP)
        self.assertEqual(conv.trouble, {agent_server.TROUBLE_ERROR: "something else broke"}, "a stop fixes no other error")

    def test_a_recovery_retries_a_failed_stop(self):
        conv = agent_server.Conversation(self.home)
        tries = []

        class Stuck:
            label = "Stuck"

            def retire(self, on_its_own=False):
                tries.append(True)
                if len(tries) == 1:
                    raise PermissionError(1, "Operation not permitted")

        stuck = Stuck()
        with conv.lock:
            conv.retiring.add(stuck)
        conv._retire(stuck)
        with self.assertRaises(Refused) as refused:
            conv._refuse_if_troubled()
        self.assertIn("could not stop Stuck", str(refused.exception))
        conv._recover()
        self.assertEqual((len(tries), conv.trouble, conv.unstopped), (2, {}, None))

    def test_an_ended_turn_takes_no_event_and_no_history(self):
        conv, turn = self.running()
        conv.finish(turn, "done")
        events, history = len(conv.events), list(conv.state["history"])
        self.assertIsNone(conv.emit(turn, ev.TEXT, text="late"))
        conv.remember(turn, {"role": "assistant", "content": "late"})
        self.assertEqual((len(conv.events), conv.state["history"]), (events, history))

    def test_a_call_given_up_that_the_turn_does_not_wait_on_is_refused_naming_both(self):
        conv, turn = self.running()
        conv.block(turn, ev.TOOL_CALL, "c-1", call_fields("c-1"), lambda payload: None)
        with self.assertRaises(Refused) as refused:
            conv.stop_waiting(turn, "c-9")
        self.assertIn("the agent gave up on call 'c-9', and the turn waits on 'c-1'", str(refused.exception))


class ReaderTest(unittest.TestCase):
    """The base adapter's reader of the agent's output, on a stand-in process."""

    class Proc:
        def __init__(self, lines):
            self.stdout = io.StringIO("".join(json.dumps(line) + "\n" for line in lines))
            self.stdin = io.StringIO()
            self.pid = None
            # Its stdout and stderr already read to the end.
            self.stdout_read, self.stderr_read = threading.Event(), threading.Event()
            self.stderr_read.set()
            self.ended_told = False

        def wait(self, timeout=None):
            return 0

        def poll(self):
            return 0

    def adapter(self):
        conv = agent_server.Conversation(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, conv.home, True)
        adapter = adapter_claude.ClaudeCode(conv, {"agent": "claude", "folder": conv.home}, [])
        adapter.read, adapter.stops = [], []
        adapter.on_message = adapter.read.append
        stop = adapter.stop
        adapter.stop = lambda: (adapter.stops.append(True), stop())
        return adapter

    def test_lines_of_a_process_already_replaced_are_dropped(self):
        adapter = self.adapter()
        old = self.Proc([{"type": "result"}])
        adapter.proc = self.Proc([])
        adapter._read_out(old, {})
        self.assertEqual((adapter.read, adapter.stops), ([], []), "nothing of the old process reaches the new one's turn")

    def test_an_agent_that_exits_on_its_own_stops_the_rest_of_its_adapter(self):
        adapter = self.adapter()
        adapter.proc = self.Proc([{"type": "system"}])
        adapter._read_out(adapter.proc, adapter.waiting)
        self.assertEqual((adapter.read, adapter.stops), ([{"type": "system"}], [True]))

    def test_a_line_is_handled_whole_before_its_process_can_be_replaced(self):
        # A start swaps the process in under `life`, which the reader holds
        # from its check of a line's process to the end of handling it.
        adapter = self.adapter()
        adapter.proc = self.Proc([{"type": "system"}])
        swapped, during = threading.Event(), []

        def swap():
            with adapter.life:
                adapter.proc = self.Proc([])
                swapped.set()

        def on_message(message):
            threading.Thread(target=swap).start()
            during.append(swapped.wait(0.3))

        adapter.on_message = on_message
        adapter._read_out(adapter.proc, {})
        self.assertTrue(swapped.wait(5))
        self.assertEqual(during, [False], "the process was swapped while one of its lines was being handled")

    def test_an_agent_that_exits_on_its_own_and_cannot_be_stopped_is_recorded(self):
        adapter = self.adapter()
        conv = adapter.conv

        def failing():
            raise ProcessLookupError("gone, and its gateway's record unreadable")

        adapter.stop = failing
        adapter.proc = self.Proc([])
        adapter._read_out(adapter.proc, {})
        self.assertEqual((conv.unstopped, sorted(conv.trouble)), (adapter, [agent_server.TROUBLE_STOP]))
        with self.assertRaises(Refused) as refused:
            conv._refuse_if_troubled()
        self.assertIn("could not stop Claude Code", str(refused.exception))

    def test_a_line_read_as_its_process_starts_stopping_is_dropped(self):
        adapter = self.adapter()
        adapter.proc = self.Proc([{"type": "system"}])
        adapter.life.acquire()
        reader = threading.Thread(target=adapter._read_out, args=(adapter.proc, {}))
        reader.start()
        # The reader has the line, and waits on `life`, which the stop holds.
        adapter.stopping = True
        adapter.life.release()
        reader.join(5)
        self.assertEqual(adapter.read, [], "a stopping process's line became the ended turn's")

    def test_a_stop_waits_out_the_grace_without_holding_its_lock(self):
        adapter = self.adapter()
        taken = []

        class Running(self.Proc):
            def poll(self):
                return None

            def wait(self, timeout=None):
                # The reader's look at a line, from another thread, meanwhile.
                other = threading.Thread(target=lambda: taken.append(adapter.life.acquire(timeout=5) and adapter.life.release() is None))
                other.start()
                other.join()
                return 0

        adapter.proc = Running([])
        adapter.proc.pid = 4242
        with unittest.mock.patch.object(adapters.os, "killpg") as killpg:
            agent_server.adapters.Adapter.stop(adapter)
        self.assertEqual(killpg.call_args_list, [unittest.mock.call(4242, adapters.signal.SIGTERM)])
        self.assertEqual(taken, [True], "the reader could not take the lock while the stop waited")

    def test_a_process_replaced_as_it_exits_leaves_the_new_one_and_its_requests_alone(self):
        # The next turn starts a new process between the old one's exit and
        # its reader's end: the reader fails only the old one's requests.
        adapter = self.adapter()
        old, new = self.Proc([]), self.Proc([])
        old_box, new_box = {"done": threading.Event()}, {"done": threading.Event()}

        def replaced(timeout=None):
            adapter.proc, adapter.waiting = new, {1: new_box}
            return 0

        old.wait = replaced
        adapter.proc = old
        adapter._read_out(old, {1: old_box})
        self.assertIn("exited (exit 0) before answering", old_box["error"]["message"])
        self.assertEqual((new_box["done"].is_set(), adapter.waiting, adapter.stops), (False, {1: new_box}, []))


    def test_an_exit_is_said_with_what_the_agent_printed_as_it_died(self):
        # The stderr reader has not reached the end of the pipe yet when the
        # exit is read: the message waits for it, and holds its last line.
        adapter = self.adapter()
        proc = self.Proc([])

        class Reading:
            def wait(self, timeout=None):
                adapter.stderr.append("dying words")
                return True

        proc.stderr_read = Reading()
        adapter.proc = proc
        box = {"done": threading.Event()}
        adapter._read_out(proc, {1: box})
        self.assertIn("exited (exit 0) before answering: dying words", box["error"]["message"])


class ServerTest(unittest.TestCase):
    def test_a_client_that_hangs_up_is_no_error_and_anything_else_is_logged(self):
        server = agent_server.Server(("127.0.0.1", 0), agent_server.Handler)
        self.addCleanup(server.server_close)
        for error, logged in ((ConnectionResetError("reset"), False), (BrokenPipeError("pipe"), False), (ValueError("bad"), True)):
            caught = io.StringIO()
            try:
                raise error
            except Exception:  # noqa: BLE001
                with contextlib.redirect_stderr(caught):
                    server.handle_error(None, ("127.0.0.1", 1))
            self.assertEqual(type(error).__name__ in caught.getvalue(), logged, error)


class AcpTest(Rig):
    def test_a_permission_round_trip_and_a_diff(self):
        self.configure("fakeacp", streamBy="token")
        turn = self.turn("edit the notes")
        waiting = self.server.until(turn, ev.WAITING_QUESTION)
        question = waiting["pending"]["question"]
        self.assertEqual([c["kind"] for c in question["permission"]["choices"]], [ev.ALLOW_ONCE, ev.ALLOW_ALWAYS, ev.REJECT])
        self.assertIn("/w/notes.md", question["permission"]["detail"])
        self.server.post("/turns", {"answer": {"questionId": question["id"], "choice": "allow-once"}})
        done = self.server.until(turn, ev.DONE, after=waiting["after"])
        kinds = [e["type"] for e in waiting["seen"] + done["seen"]]
        self.assertEqual(kinds, [ev.TEXT_DELTA, ev.TEXT_DELTA, ev.TEXT, ev.TOOL_USE, ev.QUESTION, ev.TOOL_RESULT, ev.FILE_CHANGE,
                                 ev.TEXT_DELTA, ev.TEXT, ev.DONE])
        self.assertEqual(done["reply"], "on it\n\npicked allow-once")
        modes = [m for m in self.logged() if m.get("method") == "session/set_mode"]
        self.assertEqual(modes[0]["params"]["modeId"], "default")

    def test_the_programs_tool_reaches_weft_and_a_restart_loads_the_session(self):
        self.configure("fakeacp")
        turn = self.turn("tool", tools=LOOKUP)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "on it42")
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.configure("fakeacp")
        turn = self.turn("hello")
        answer = self.server.until(turn, ev.DONE)
        self.assertEqual([e["type"] for e in answer["seen"]], [ev.TEXT, ev.DONE], "the replayed conversation is not this turn's")
        loads = [m for m in self.logged() if m.get("method") == "session/load"]
        self.assertEqual(loads[0]["params"]["sessionId"], session)

    def test_a_key_login_reaches_the_agent_as_its_variable(self):
        adapter = adapters.KINDS["fakeacp"](self.server.conversation, {"agent": "fakeacp", "folder": self.folder,
                                             "credentials": {"way": "apiKey", "values": {"key": "k1"}}}, [])
        self.assertEqual(adapter.env()["FAKE_API_KEY"], "k1")


class AnswerTest(unittest.TestCase):
    def test_a_multi_select_takes_several_and_a_single_one(self):
        q = ev.questions("q", [ev.asked("a", "", "Which?", [{"label": "x", "description": ""}, {"label": "y", "description": ""}], True, False),
                               ev.asked("b", "", "One?", [{"label": "x", "description": ""}], False, False)])
        self.assertEqual(ev.answers_for({"answers": {"a": ["x", "y"], "b": "x"}}, q), {"a": ["x", "y"], "b": ["x"]})
        with self.assertRaises(Refused):
            ev.answers_for({"answers": {"a": ["x"], "b": ["x", "x"]}}, q)
        with self.assertRaises(Refused):
            ev.answers_for({"answers": {"a": ["z"], "b": ["x"]}}, q)

    def test_a_choice_that_was_not_offered_is_refused(self):
        q = ev.permission("p", "shell", "t", "d", {}, [ev.choice("allow", "Allow", ev.ALLOW_ONCE)])
        with self.assertRaises(Refused):
            ev.chosen({"choice": "always"}, q)


class ProgramTest(Rig):
    """The node's `program`: the person's own build of the agent, started
    in place of the one on PATH, with the same command line."""

    def own_build(self, name: str, script: str) -> tuple:
        folder = os.path.join(self.root, "own")
        os.makedirs(folder, exist_ok=True)
        log = os.path.join(self.root, "own.log")
        shim(folder, name, script, {"FAKE_LOG": log})
        return os.path.join(folder, name), log

    def test_claude_runs_the_persons_own_build(self):
        program, log = self.own_build("my-claude", "fake_claude.py")
        self.configure("claude", program=program)
        self.server.until(self.turn("hello"), ev.DONE)
        with open(log) as f:
            started = [json.loads(line) for line in f]
        self.assertIn("--permission-prompt-tool", started[0]["args"])
        self.assertFalse(os.path.exists(self.log), "the claude on PATH never ran")

    def test_an_acp_agent_runs_the_persons_own_build(self):
        program, log = self.own_build("my-fakeacp", "fake_acp.py")
        self.configure("fakeacp", program=program)
        self.server.until(self.turn("hello"), ev.DONE)
        with open(log) as f:
            self.assertTrue(any(json.loads(line).get("method") == "initialize" for line in f))
        self.assertFalse(os.path.exists(self.log), "the fakeacp on PATH never ran")

    def test_a_program_that_is_not_there_is_refused_naming_it(self):
        missing = os.path.join(self.root, "nowhere", "claude")
        self.configure("claude", program=missing)
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn(f"the node's `program` is {missing}, and nothing that runs is there", failed["error"])

    def test_an_agent_not_on_path_is_refused_naming_it(self):
        os.environ["PATH"] = os.path.join(self.root, "empty")
        self.configure("claude")
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("`claude` is not installed where the agent server runs", failed["error"])


if __name__ == "__main__":
    unittest.main()
