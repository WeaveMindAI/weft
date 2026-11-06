"""Kilo Code over its own server (`kilo serve`).

What the adapter hands Kilo for a configuration (the permission rules, weft's
plugin with the program's tools, the logins), read straight off its
functions; then whole turns through the agent server against
`testing/fake_kilo.py`, which replays what Kilo Code CLI 7.8.18 really sent:
the program's tools coming back by their plain names (one replacing a
built-in) and kept in the history as the program's, a question, the
permissions, the modes a conversation runs in, a resumed conversation, a
sub-agent, a lost network Kilo waits out by itself, and the ways a turn
fails. Last, weft's plugin
itself on Node.

Run with the others: `python3 -m unittest test_kilo`.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, shim  # noqa: E402
import adapter_kilo as kilo  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402
import js_plugin_rig  # noqa: E402
from events import Refused  # noqa: E402

# A program tool named like one of Kilo's built-ins, beside a new one.
READ = {"name": "read", "description": "The program's own reader", "parameters": {"type": "object", "properties": {}}}
TOOLS = [READ, *LOOKUP]


class WhatIsSent(js_plugin_rig.AdapterCase):
    kind = "kilo"

    def env(self, adapter) -> dict:
        adapter.prepare()
        env = {"KILO_PURE": "1", "KILO_CONFIG_CONTENT": "{}", "KILO_SERVER_PASSWORD": "the-persons-own",
               "XDG_CONFIG_HOME": "/theirs", "KILO_CLIENT": "cli"}
        adapter.credential_env(env)
        return env

    def test_each_permission_setting_is_a_rule_set_kilo_merges_last(self):
        rules = {p: json.loads(self.env(self.adapter([], permission=p))["KILO_PERMISSION"]) for p in kilo.PERMISSIONS}
        self.assertEqual(rules["ask"], {"bash": "ask", "edit": "ask", "webfetch": "ask", "websearch": "ask", "question": "allow"})
        self.assertEqual(rules["accept_edits"]["edit"], "allow")
        # An agent of the person's own denies the question tool unless told otherwise.
        self.assertEqual((rules["plan"], rules["bypass"]), ({"question": "allow"}, {"*": "allow"}))

    def test_the_programs_tools_are_weft_plugin_and_nothing_of_the_persons_own_is_kept(self):
        adapter = self.adapter(LOOKUP)
        env = self.env(adapter)
        loader = os.path.join(self.root, kilo.LOADER)
        self.assertEqual(json.loads(env["KILO_CONFIG_CONTENT"]), {"plugin": ["file://" + loader]})
        with open(loader) as f:
            self.assertEqual(js_plugin_rig.made(f.read()), {"url": "http://127.0.0.1:4100/mcp", "tools": LOOKUP})
        self.assertNotIn("KILO_PURE", env, "it would leave weft's plugin out")
        self.assertEqual(env["XDG_CONFIG_HOME"], "/theirs", "the machine's own login reads the person's own config")
        # Its own question tool on, the terminal's own tools off.
        self.assertEqual((env["KILO_CLIENT"], env["KILO_ENABLE_QUESTION_TOOL"]), ("weft", "1"))
        self.assertEqual(env["KILO_SERVER_USERNAME"], "kilo")
        self.assertNotEqual(env["KILO_SERVER_PASSWORD"], "the-persons-own")
        self.assertGreater(len(env["KILO_SERVER_PASSWORD"]), 30)
        self.assertNotIn("KILO_CONFIG_CONTENT", self.env(self.adapter([])), "no tools, no plugin")

    def test_with_a_connection_the_loader_and_its_config_are_in_its_private_home(self):
        adapter = self.adapter(LOOKUP, credentials={"way": "kiloKey", "values": {"key": "k"}})
        env = self.env(adapter)
        self.assertEqual(json.loads(env["KILO_CONFIG_CONTENT"])["plugin"], ["file://" + os.path.join(adapter.home, kilo.LOADER)])
        self.assertEqual((env["HOME"], env["KILO_API_KEY"]), (adapter.home, "k"))
        self.assertNotIn("XDG_CONFIG_HOME", env, "Kilo would read the person's own config from it")

    def test_what_is_refused_before_anything_starts(self):
        refusals = {
            "a provider's key with no model": dict(credentials={"way": "openaiKey", "values": {"key": "k"}}),
            "plan with the program's tools": dict(tools=LOOKUP, permission="plan"),
        }
        for what, given in refusals.items():
            with self.subTest(what), self.assertRaises(Refused) as refused:
                tools = given.pop("tools", [])
                self.adapter(tools, **given)
            self.assertEqual(refused.exception.status, 400)
        with self.assertRaises(Refused) as refused:
            kilo.Kilo(self.conv, {"agent": "kilo", "folder": self.root, "model": "claude-sonnet"}, [])
        self.assertIn("`<provider>/<model>`", str(refused.exception))

    def test_a_permission_request_and_its_answers(self):
        request = {"id": "per_1", "permission": "bash", "patterns": ["echo hi"], "metadata": {"command": "echo hi"},
                   "always": ["echo *"]}
        question = kilo.permission_question("kilo-per_1", request)
        self.assertEqual((question["permission"]["title"], question["permission"]["detail"]),
                         ("Kilo Code wants to run a command", "echo hi"))
        self.assertEqual([c["id"] for c in question["permission"]["choices"]], ["once", "always", "reject"])
        self.assertEqual(kilo.permission_reply(question, {"choice": "always"}), {"reply": "always"})
        self.assertEqual(kilo.permission_reply(question, {"choice": "reject", "note": "not that"}),
                         {"reply": "reject", "message": "not that"})
        never = kilo.permission_question("kilo-per_2", {**request, "always": []})
        self.assertEqual([c["id"] for c in never["permission"]["choices"]], ["once", "reject"], "nothing to allow from now on")

    def test_several_questions_at_once_and_their_answers_in_order(self):
        request = {"questions": [{"question": "Which?", "header": "Pick", "options": [{"label": "a", "description": ""}]},
                                 {"question": "Any?", "header": "More", "multiple": True, "custom": False,
                                  "options": [{"label": "x", "description": ""}, {"label": "y", "description": ""}]}]}
        question = kilo.asked_questions("kilo-que_1", request)
        self.assertEqual([(q["id"], q["multiSelect"], q["allowOther"]) for q in question["questions"]],
                         [("q0", False, True), ("q1", True, False)])
        self.assertEqual(kilo.question_reply(question, {"answers": {"q0": ["my own"], "q1": ["x", "y"]}}),
                         {"answers": [["my own"], ["x", "y"]]})

    def test_the_files_a_tool_changed(self):
        self.assertEqual(kilo.file_changes("write", {"metadata": {"filepath": "/a", "exists": True}}), [("/a", "update")])
        self.assertEqual(kilo.file_changes("edit", {"metadata": {"filediff": {"file": "/b"}}}), [("/b", "update")])
        moved = {"metadata": {"files": [{"filePath": "/c", "type": "add"}, {"filePath": "/d", "type": "move", "movePath": "/e"}]}}
        self.assertEqual(kilo.file_changes("apply_patch", moved), [("/c", "add"), ("/d", "delete"), ("/e", "add")])


class KiloTurns(Rig):
    def setUp(self):
        super().setUp()
        shim(os.path.join(self.root, "bin"), "kilo", "fake_kilo.py")

    def received(self, path=None) -> list:
        return [m for m in self.logged() if "start" not in m and (path is None or m["path"] == path)]

    def prompts(self) -> list:
        return [m["body"] for m in self.received() if m["path"].endswith("/prompt_async")]

    def starts(self) -> list:
        return [m["start"] for m in self.logged() if "start" in m]

    def history(self, turn) -> list:
        return self.server.call("GET", f"/history?turn={turn}")[1]["messages"]

    def answer(self, turn, status, **answer):
        pending = self.server.until(turn, status)
        self.server.post("/turns", {"answer": {"questionId": pending["pending"]["question"]["id"], **answer}})
        return pending

    def test_a_program_tool_named_like_a_built_in_comes_back_by_its_plain_name_and_is_the_programs_in_the_history(self):
        self.configure("kilo")
        turn = self.turn("call:read call:grep call:lookup", tools=TOOLS)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual(call["name"], "read")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "the program's read"}})
        call = self.server.until(turn, ev.WAITING_TOOL, after=call["n"])["pending"]
        self.assertEqual(call["name"], "lookup")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "no such key", "isError": True}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(done["reply"], "read: the program's read\ngrep: built-in grep ran\nlookup failed: no such key")
        uses = [e["name"] for e in done["seen"] if e["type"] == ev.TOOL_USE]
        self.assertEqual(uses, ["grep"], "a call of the program's tool is its tool_call, never the agent's own tool use")
        # The program's calls are written by its route as they reach weft,
        # Kilo's own (`grep`) by the adapter as it reads Kilo's event stream:
        # two threads, so where `grep` lands between `read` and `lookup`
        # follows which one runs first, and Kilo itself runs the tool calls
        # of one model step at once. What holds is each channel's own order,
        # and every call before its result.
        history = self.history(turn)
        calls = [(i, c) for i, m in enumerate(history) for c in m.get("tool_calls") or []]
        self.assertEqual([c["name"] for _, c in calls if c["name"] != "grep"], ["read", "lookup"])
        self.assertEqual(sorted(c["name"] for _, c in calls), ["grep", "lookup", "read"], "each written once")
        results = {m["tool_call_id"]: (i, m["content"]) for i, m in enumerate(history) if m["role"] == "tool"}
        for i, call in calls:
            at, content = results[call["id"]]
            self.assertGreater(at, i, f"{call['name']}'s result comes after its call")
            self.assertEqual(content, {"read": "the program's read", "grep": "built-in grep ran", "lookup": "no such key"}[call["name"]])
        self.assertEqual(self.starts()[-1]["args"], ["serve", "--port", "0", "--hostname", "127.0.0.1"])

    def test_kilo_stops_when_the_agent_server_is_gone(self):
        # Its server reads nothing on its standard input: the tether's end of
        # it is what stops Kilo once the agent server dies.
        self.configure("kilo")
        self.server.until(self.turn("hello"), ev.DONE)
        adapter = self.server.conversation.adapter
        tether = adapter.proc
        self.assertEqual(adapter.command()[1:3], [adapters.TETHER, "--"])
        tether.stdin.close()
        self.assertIsNotNone(tether.wait(10), "the tether ended with Kilo")

    def test_a_question_waits_for_the_person_and_their_answer_goes_back(self):
        self.configure("kilo")
        turn = self.turn("ask")
        pending = self.server.until(turn, ev.WAITING_QUESTION)["pending"]["question"]
        self.assertEqual(pending["questions"][0]["question"], "Red or blue?")
        self.assertEqual([o["label"] for o in pending["questions"][0]["options"]], ["Red", "Blue"])
        self.server.post("/turns", {"answer": {"questionId": pending["id"], "answers": {"q0": ["purple"]}}})
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], 'ask: [["purple"]]')

    def test_a_permission_allowed_refused_with_a_note_and_refused_without_one(self):
        self.configure("kilo")
        turn = self.turn("run")
        self.answer(turn, ev.WAITING_QUESTION, choice="once")
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "run: hi")
        turn = self.turn("run")
        self.answer(turn, ev.WAITING_QUESTION, choice="reject", note="use another way")
        self.assertEqual(self.server.until(turn, ev.DONE)["reply"], "run: refused, use another way")
        turn = self.turn("run")
        self.answer(turn, ev.WAITING_QUESTION, choice="reject")
        failed = self.server.until(turn, ev.FAILED)
        self.assertIn("ended the turn when `Kilo Code wants to run a command` was refused", failed["error"])
        self.assertEqual([m["body"] for m in self.received() if m["path"].startswith("/permission/")],
                         [{"reply": "once"}, {"reply": "reject", "message": "use another way"}, {"reply": "reject"}])
        self.configure("kilo", permission="bypass")
        self.assertEqual(self.server.until(self.turn("run"), ev.DONE)["reply"], "run: hi", "nothing asks")

    def test_streaming_by_token_sends_the_pieces_and_the_whole(self):
        self.configure("kilo", streamBy="token")
        done = self.server.until(self.turn("think write"), ev.DONE)
        self.assertEqual([e["type"] for e in done["seen"]],
                         [ev.THINKING, ev.TOOL_USE, ev.TOOL_RESULT, ev.FILE_CHANGE, ev.TEXT_DELTA, ev.TEXT_DELTA, ev.TEXT, ev.DONE])
        self.assertEqual([e["path"] for e in done["seen"] if e["type"] == ev.FILE_CHANGE], ["made.txt"])
        self.configure("kilo", streamBy="message")
        done = self.server.until(self.turn("think"), ev.DONE)
        self.assertEqual([e["type"] for e in done["seen"]], [ev.THINKING, ev.TEXT, ev.DONE])
        self.assertEqual(done["reply"], "done", "the person's own message is no part of the reply")

    def test_a_restarted_server_finds_the_conversation_and_keeps_the_mode_it_began_in(self):
        os.environ["FAKE_KILO_AGENTS"] = "prompt-engineer,plan"
        self.configure("kilo")
        self.server.until(self.turn("hello"), ev.DONE)
        conversation = self.server.conversation.conversation_id()
        self.assertEqual(self.server.conversation.conversation_kept()["started"],
                         {"agent": "prompt-engineer", "model": {"providerID": "fake", "modelID": "fake-model"}})
        self.server.close()
        self.server = self.start()
        self.configure("kilo", permission="plan")
        self.server.until(self.turn("again"), ev.DONE)
        self.configure("kilo", model="fake/other-model")
        self.server.until(self.turn("and again"), ev.DONE)
        self.configure("kilo")
        self.server.until(self.turn("once more"), ev.DONE)
        self.assertEqual(self.server.conversation.conversation_id(), conversation)
        self.assertEqual(len(self.received("/session")), 1, "found again, never made anew")
        prompts = self.prompts()
        self.assertNotIn("agent", prompts[0], "a new conversation runs in the agent Kilo's config makes the default")
        self.assertEqual([p.get("agent") for p in prompts[1:]], ["plan", "prompt-engineer", "prompt-engineer"])
        self.assertEqual([p["model"]["modelID"] for p in prompts[1:]], ["fake-model", "other-model", "fake-model"])

    def test_a_conversation_kilo_no_longer_has_says_to_start_over(self):
        self.configure("kilo")
        self.server.until(self.turn("hello"), ev.DONE)
        os.remove(os.path.join(self.root, "kilo-sessions.json"))
        failed = self.server.until(self.turn("again", tools=TOOLS), ev.FAILED)
        self.assertIn("Session not found", failed["error"])
        self.assertIn("start it over from the node's card", failed["error"])

    def test_a_mode_its_config_turns_off_fails_the_turn_naming_the_ones_it_offers(self):
        os.environ["FAKE_KILO_AGENTS"] = "prompt-engineer,good-writter"
        self.configure("kilo", permission="plan")
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("has no `plan` mode here", failed["error"])
        self.assertIn("(the modes it offers: `prompt-engineer`, `good-writter`)", failed["error"])
        self.configure("kilo")
        self.server.until(self.turn("hello"), ev.DONE)
        self.server.close()
        os.environ["FAKE_KILO_AGENTS"] = "code"
        self.server = self.start()
        self.configure("kilo")
        failed = self.server.until(self.turn("again"), ev.FAILED)
        self.assertIn("has no `prompt-engineer` mode any more, the mode this conversation started in", failed["error"])

    def test_an_effort_level_is_the_models_variant_and_one_it_lacks_fails_the_turn(self):
        self.configure("kilo", options={"effort": "high"})
        self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual(self.prompts()[-1]["variant"], "high")
        self.configure("kilo", model="fake/other-model", options={"effort": "high"})
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertEqual(failed["error"], "Kilo Code's model fake/other-model has no `high` effort level (it offers: none); "
                                          "pick one of those, or leave `effort` empty")
        self.assertTrue(self.received(f"/session/{self.server.conversation.conversation_id()}/abort"))

    def test_a_sub_agents_call_of_the_programs_tool_waits_and_its_messages_are_no_reply(self):
        self.configure("kilo")
        turn = self.turn("sub:lookup", tools=TOOLS)
        call = self.server.until(turn, ev.WAITING_TOOL)["pending"]
        self.assertEqual(call["name"], "lookup")
        self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": "42"}})
        done = self.server.until(turn, ev.DONE)
        self.assertEqual(done["reply"], "sub: sub done")
        self.assertEqual([e["name"] for e in done["seen"] if e["type"] == ev.TOOL_USE], ["task"])

    def test_a_lost_network_is_kilo_s_to_wait_out_and_asks_nothing(self):
        self.configure("kilo")
        done = self.server.until(self.turn("offline"), ev.DONE)
        self.assertEqual(done["reply"], "offline: back")
        self.assertEqual([e for e in done["seen"] if e["type"] in (ev.WAITING_QUESTION, ev.QUESTION)], [])

    def test_a_retry_or_a_lost_network_is_the_card_s_notice(self):
        adapter = kilo.Kilo.__new__(kilo.Kilo)
        adapter.facts = adapters.AGENTS["kilo"]
        said = []
        adapter.conv = type("Conv", (), {"notice": lambda _self, turn, text: said.append(text)})()
        adapter._status("t", {"type": "retry", "attempt": 2, "message": "Provider is overloaded", "next": 3})
        adapter._status("t", {"type": "offline", "requestID": "que_1", "message": "Connection refused"})
        adapter._status("t", {"type": "busy"})
        self.assertEqual(said, ["Kilo Code is retrying its model (attempt 2): Provider is overloaded",
                                "Kilo Code cannot reach its model (Connection refused); it goes on by itself once the "
                                "network is back"])

    def test_an_error_it_says_fails_the_turn_and_an_exit_loses_it(self):
        self.configure("kilo")
        failed = self.server.until(self.turn("fail"), ev.FAILED)
        self.assertEqual(failed["error"], "Kilo Code could not finish the turn: Incorrect API key provided (APIError)")
        lost = self.server.until(self.turn("crash"), ev.LOST)
        self.assertIn("ended in the middle of the turn", lost["error"])

    def test_a_connections_login_is_the_only_one_it_has(self):
        os.environ["XDG_DATA_HOME"] = "/the-persons-own"
        self.configure("kilo", credentials={"way": "kiloKey", "values": {"key": "kilo-key"}})
        self.server.until(self.turn("hello"), ev.DONE)
        env = self.starts()[-1]["env"]
        self.assertEqual((env["KILO_API_KEY"], env["HOME"]), ("kilo-key", os.path.join(self.home, "kilo-home")))
        self.assertNotIn("XDG_DATA_HOME", env)

    def test_the_server_answers_only_its_own_password(self):
        self.configure("kilo")
        self.server.until(self.turn("hello"), ev.DONE)
        adapter = self.server.conversation.adapter
        adapter.password = "wrong"
        with self.assertRaises(Refused) as refused:
            adapter.api("GET", "/agent")
        self.assertIn("HTTP 401", str(refused.exception))


HARNESS = r"""
const plugin = (await import(process.argv[2])).default;
const hooks = await plugin({});
const ran = {};
for (const [name, args] of Object.entries(JSON.parse(process.argv[3]))) {
  try { ran[name] = { output: await hooks.tool[name].execute(args, { abort: undefined }) }; }
  catch (error) { ran[name] = { error: error.message }; }
}
const schemas = {};
for (const name of Object.keys(hooks.tool)) {
  const output = { description: hooks.tool[name].description, parameters: "zod", jsonSchema: { type: "object" } };
  await hooks["tool.definition"]({ toolID: name }, output);
  schemas[name] = output.jsonSchema;
}
const other = { description: "", parameters: "zod", jsonSchema: { kept: true } };
await hooks["tool.definition"]({ toolID: "bash" }, other);
console.log(JSON.stringify({ tools: Object.keys(hooks.tool), args: hooks.tool[Object.keys(hooks.tool)[0]].args, ran, schemas, other: other.jsonSchema }));
"""


@js_plugin_rig.needs_node
class KiloPlugin(js_plugin_rig.AdapterCase):
    kind = "kilo"

    def test_each_tool_keeps_its_plain_name_and_schema_and_goes_to_the_route(self):
        url, calls = js_plugin_rig.route(self)
        adapters.SERVER_URL = url.removesuffix("/mcp")
        tools = [js_plugin_rig.tool("read"), js_plugin_rig.tool("failing", required=())]
        self.adapter(tools).prepare()
        ran = js_plugin_rig.run_node(self, HARNESS, os.path.join(self.root, kilo.LOADER),
                                     json.dumps({"read": {"q": "a"}, "failing": {"q": "b"}}))
        self.assertEqual(ran["tools"], ["read", "failing"])
        self.assertEqual(ran["ran"], {"read": {"output": 'read {"q": "a"}'}, "failing": {"error": 'failing {"q": "b"}'}})
        self.assertEqual([c["params"] for c in calls], [{"name": "read", "arguments": {"q": "a"}},
                                                        {"name": "failing", "arguments": {"q": "b"}}])
        # Kilo would make every property of `args` required; the model gets the program's own schema.
        self.assertEqual(ran["schemas"], {"read": tools[0]["parameters"], "failing": tools[1]["parameters"]})
        self.assertEqual(ran["other"], {"kept": True}, "a tool of Kilo's own keeps its schema")


if __name__ == "__main__":
    unittest.main()
