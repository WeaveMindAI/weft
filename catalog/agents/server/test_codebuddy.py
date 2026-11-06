"""CodeBuddy Code's program tools: the MCP server weft hands it, its tools
under their plain names (`acp_codebuddy.codebuddy_tools`), and how a call
of one is told from a call of CodeBuddy's own.

The turns, settings and logins are in `test_acp_agents.py`, played from
`testing/recorded/codebuddy.json`.

Run with the others: `python3 -m unittest test_codebuddy`.
"""

import os
import shutil
import sys
import tempfile
import threading
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

import rig  # noqa: E402,F401 (the server's modules on the path, its logs quiet)
import acp_codebuddy  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402

TOOLS = [{"name": "WebSearch"}, {"name": "lookup"}]


def adapter(tools):
    return types.SimpleNamespace(tools=tools, program_tools=adapters.ProgramTools(tools))


class Server(unittest.TestCase):
    def test_the_server_names_every_program_tool_a_bare_one(self):
        adapters.SERVER_URL = "http://127.0.0.1:4100"
        setup = acp_codebuddy.codebuddy_tools(adapter(TOOLS))
        self.assertEqual(setup.session, {"mcpServers": [{
            "type": "http", "name": "weft", "url": "http://127.0.0.1:4100/mcp", "headers": [],
            "_meta": {"transparent_proxy": True, "builtin_tools": ["WebSearch", "lookup"]}}]})
        self.assertEqual(setup.args, [], "no built-in is turned off: the program's tool of its name takes its place")


class Recognition(unittest.TestCase):
    def test_a_call_is_named_by_its_tool_name_in_meta_never_its_title(self):
        read = adapters.KINDS["codebuddy"].spec.program_tool.read
        names = {"Read", "lookup"}
        # Real `tool_call`s of 2.179.3: a program tool named `Read` is retitled
        # as CodeBuddy's own once its arguments are in.
        self.assertEqual(read({"title": "Read File", "_meta": {"codebuddy.ai/toolName": "Read"}}, names), "Read")
        self.assertEqual(read({"title": "lookup", "_meta": {"codebuddy.ai/toolName": "lookup"}}, names), "lookup")
        self.assertIsNone(read({"title": "Terminal", "_meta": {"codebuddy.ai/toolName": "Bash"}}, names))

    def test_a_program_tool_named_like_the_ask_tool_asks_an_ordinary_permission(self):
        asked = []
        stand_in = types.SimpleNamespace(program_tools=adapters.ProgramTools([{"name": acp_codebuddy.ASK_TOOL}]))
        call = {"toolCallId": "c1", "rawInput": {"q": "x"}, "_meta": {"codebuddy.ai/toolName": acp_codebuddy.ASK_TOOL}}
        original = acp_codebuddy.acp.answer_permission
        acp_codebuddy.acp.answer_permission = lambda adapter, turn, rid, params: asked.append(params["toolCall"]["title"])
        try:
            acp_codebuddy.codebuddy_permission(stand_in, "t", 1, {"toolCall": call, "options": []})
        finally:
            acp_codebuddy.acp.answer_permission = original
        self.assertEqual(asked, [acp_codebuddy.ASK_TOOL])


class Turn:
    def __init__(self):
        self.status = ev.WORKING
        self.events = []


class Conversation:
    """What `_update` and the check read and write of a conversation."""

    def __init__(self, home):
        self.home = home
        self.lock = threading.RLock()
        self.failed = []

    def emit(self, turn, kind, **fields):
        turn.events.append({"type": kind, **fields})

    def remember(self, turn, message):
        pass

    def fail(self, turn, message, status=None):
        self.failed.append(message)
        turn.status = ev.FAILED


class PlainNamesCheck(unittest.TestCase):
    """The plain names of the program's tools rest on keys CodeBuddy does not
    document: a CodeBuddy that stops reading them fails the turn, saying so."""

    def run_updates(self, tools, updates, routed=()):
        adapters.SERVER_URL = "http://127.0.0.1:4100"
        home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, home, True)
        conv = Conversation(home)
        adapter = adapters.KINDS["codebuddy"](conv, {"agent": "codebuddy", "folder": home, "permission": "ask", "options": {}},
                                              [{"name": t, "description": "", "parameters": {"type": "object", "properties": {}}}
                                               for t in tools])
        turn = adapter.turn = Turn()
        for update in updates:
            if update == "route":
                turn.events.append({"type": ev.TOOL_CALL, "name": routed[0]})
                continue
            adapter._update(turn, update)
        return conv.failed

    @staticmethod
    def call(call_id, tool_name):
        return {"sessionUpdate": "tool_call", "toolCallId": call_id, "title": tool_name, "rawInput": {},
                "_meta": {"codebuddy.ai/toolName": tool_name}}

    @staticmethod
    def done(call_id):
        return {"sessionUpdate": "tool_call_update", "toolCallId": call_id, "status": "completed",
                "content": [{"type": "content", "content": {"type": "text", "text": "out"}}]}

    def test_a_program_tool_shown_under_its_prefixed_name_fails_the_turn(self):
        failed = self.run_updates(["lookup"], [self.call("a", "mcp__weft__lookup")])
        self.assertEqual(len(failed), 1)
        self.assertIn("showed the program's tool `lookup` as `mcp__weft__lookup`", failed[0])
        self.assertIn(f"codebuddy-code@{acp_codebuddy.NAMES_CHECKED_ON}", failed[0])

    def test_its_own_built_in_run_under_a_program_tools_name_fails_the_turn(self):
        failed = self.run_updates(["WebSearch"], [self.call("a", "WebSearch"), self.done("a")])
        self.assertEqual(len(failed), 1)
        self.assertIn("a call of `WebSearch` ended that never reached the program", failed[0])

    def test_a_call_that_reached_the_program_goes_on(self):
        updates = [self.call("a", "WebSearch"), "route", self.done("a"), self.call("b", "WebSearch"), "route", self.done("b")]
        self.assertEqual(self.run_updates(["WebSearch"], updates, routed=["WebSearch"]), [])

    def test_a_second_call_the_program_never_got_fails_even_after_one_that_reached_it(self):
        updates = [self.call("a", "WebSearch"), "route", self.done("a"), self.call("b", "WebSearch"), self.done("b")]
        self.assertEqual(len(self.run_updates(["WebSearch"], updates, routed=["WebSearch"])), 1)

    def test_its_own_tools_and_another_servers_go_on(self):
        updates = [self.call("a", "Bash"), self.done("a"), self.call("b", "mcp__other__lookup"), self.done("b"),
                   self.call("c", "mcp__weft__notours"), self.done("c")]
        self.assertEqual(self.run_updates(["lookup"], updates), [])


if __name__ == "__main__":
    unittest.main()
