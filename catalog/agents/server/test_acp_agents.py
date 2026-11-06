"""Every ACP agent against its own recorded lines (`testing/recorded/`).

One test class per recording, made from its data, so an agent is tested
by writing down what it really said and what weft must see of it:

  turns     each turn's steps (what the stand-in plays), the program's
            tools it runs with (`tools`: true for `lookup`, or their names),
            and `expect`: the events weft must see, in order, the questions
            and tool calls it is asked on the way (`blocks`, each answered
            as given), what the agent must have received back (`agentGot`),
            and how the turn ends (`reply`, `status`, `error`).
  settings  a configuration (`given`: permission, model, options, login)
            and what it must do to the agent (`expect`: flags, variables,
            requests, files in its private home). Every permission setting
            and login way in agents.json, and every option the spec takes,
            must be in one of them.
  resume    the agent picked up after a restart, its replay never counted
            as the next turn's.
  given     what every configuration above starts from (a login, for an
            agent that runs on nothing else), when there is one.

Run with the others: `python3 test_agent_server.py`.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

from rig import LOOKUP, Rig, recordings  # noqa: E402
import adapter_acp  # noqa: E402
import adapters  # noqa: E402
import events as ev  # noqa: E402

# A replay's silence is the stand-in's, which writes the whole replay at
# once: its last line comes microseconds after the load's answer.
adapter_acp.REPLAY_QUIET_SECS = 0.1

STATUSES = {"done": ev.DONE, "failed": ev.FAILED, "lost": ev.LOST}

# The ACP agents with no recording yet, and why: each one named here is
# tested by nothing below, so a new agent without one fails instead.
NOT_RECORDED = {}


def turn_tools(turn: dict) -> list:
    """The program's tools a recorded turn runs with: `tools` true for
    `lookup` alone, or the names of each (a built-in's name, for a turn where
    the program's tool of that name took the built-in's place)."""
    tools = turn.get("tools")
    if tools is True:
        return LOOKUP
    return [next((t for t in LOOKUP if t["name"] == name), {"name": name, "description": f"The program's {name}",
                                                             "parameters": {"type": "object", "properties": {}}})
            for name in tools or []]


def contains(expected, actual) -> bool:
    """Whether `actual` holds everything `expected` says (dicts by subset)."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(k in actual and contains(v, actual[k]) for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(expected) == len(actual) and all(contains(e, a) for e, a in zip(expected, actual))
    return expected == actual


def sequence_in(part: list, whole: list) -> bool:
    return any(whole[i:i + len(part)] == part for i in range(len(whole) - len(part) + 1))


class AcpAgent(Rig):
    kind = None
    recording = None

    def given(self, given: dict) -> None:
        """A configuration: the recording's own `given` (what every turn of an
        agent needs, such as a login), then this one's."""
        merged = {**(self.recording.get("given") or {}), **given}
        self.configure(self.kind, **{k: v for k, v in merged.items() if k != "tools"})

    def received(self) -> list:
        """What the stand-in read from weft, after its start lines."""
        return [m for m in self.logged() if "start" not in m]

    def starts(self) -> list:
        return [m["start"] for m in self.logged() if "start" in m]

    def private_home(self) -> str:
        return os.path.join(self.home, f"{self.kind}-home")

    def play(self, name: str) -> None:
        turn = self.recording["turns"][name]
        expect = turn["expect"]
        self.given(turn.get("given") or {})
        tid = self.turn(name, tools=turn_tools(turn))
        seen, after = [], 0
        for block in expect.get("blocks") or []:
            if "question" in block:
                waiting = self.server.until(tid, ev.WAITING_QUESTION, after=after)
                question = waiting["pending"]["question"]
                self.assertTrue(contains(block["question"], question), f"{name}: asked {json.dumps(question)}")
                self.server.post("/turns", {"answer": {"questionId": question["id"], **block["answer"]}})
            else:
                waiting = self.server.until(tid, ev.WAITING_TOOL, after=after)
                call = waiting["pending"]
                self.assertEqual(call["name"], block["tool"])
                self.server.post("/turns", {"toolResult": {"callId": call["callId"], "output": block["result"]}})
            seen += waiting["seen"]
            after = waiting["after"]
        ended = self.server.until(tid, STATUSES[expect.get("status", "done")], after=after)
        seen += ended["seen"]
        self.assert_events([e["type"] for e in seen], expect["events"], f"{name}: saw {json.dumps(seen)}")
        if "reply" in expect:
            self.assertEqual(ended["reply"], expect["reply"])
        if "error" in expect:
            self.assertIn(expect["error"], ended["error"])
        answers = [m for m in self.received() if "method" not in m and "toolResult" not in m]
        results = [m["toolResult"]["content"][0]["text"] for m in self.received() if "toolResult" in m]
        tool_blocks = [b for b in expect.get("blocks") or [] if "tool" in b]
        for block in expect.get("blocks") or []:
            if "agentGot" in block:
                self.assertTrue(any(contains(block["agentGot"], a.get("result", a.get("error"))) for a in answers),
                                f"{name}: the agent got {json.dumps(answers)}")
            if "tool" in block and adapters.KINDS[self.kind].spec.program_tool is not None:
                # Kept in the history under the tool's bare name, as every adapter keeps it.
                with self.server.conversation.lock:
                    names = [c["name"] for m in self.server.conversation.state["history"] for c in m.get("tool_calls") or []]
                self.assertIn(block["tool"], names, f"{name}: the history has {names}")
            if "tool" in block:
                # Over MCP, in the order they were called.
                self.assertEqual(results[tool_blocks.index(block)], block["result"])

    def assert_events(self, seen: list, expected: list, why: str) -> None:
        """The turn's events, in the order the agent's stream guarantees.

        An agent calls the program's tools over weft's route (HTTP, not its
        stdout), so it reaches weft on two paths read by two
        threads: its stream (everything but `tool_call`) and the route (each
        `tool_call`). Each path keeps its own order, and nothing orders one
        against the other: the stream lines the agent wrote just before a
        call can still be unread when the call arrives (Hermes writes them
        through its gateway daemon's viewer, `hermes acp`, while its plugin
        posts from the daemon itself). What is guaranteed is causal: the
        agent writes what follows a call only once weft has answered it, so
        a `tool_call` never comes after a stream event recorded after it."""
        self.assertEqual([e for e in seen if e != ev.TOOL_CALL], [e for e in expected if e != ev.TOOL_CALL], why)
        self.assertEqual(seen.count(ev.TOOL_CALL), expected.count(ev.TOOL_CALL), why)

        def stream_before_each_call(events):
            """For each `tool_call`, how many stream events came before it."""
            return [i - events[:i].count(ev.TOOL_CALL) for i, e in enumerate(events) if e == ev.TOOL_CALL]

        for got, recorded in zip(stream_before_each_call(seen), stream_before_each_call(expected)):
            self.assertLessEqual(got, recorded, why)

    def check_setting(self, setting: dict) -> None:
        self.given(setting.get("given") or {})
        ended = self.server.until(self.turn("hello"), ev.DONE, ev.FAILED)
        expect = setting["expect"]
        if "error" in expect:
            self.assertIn(expect["error"], ended["error"])
            return
        self.assertEqual(ended["status"], ev.DONE, ended["error"])
        start = self.starts()[-1]
        for part in expect.get("args") or []:
            self.assertTrue(sequence_in(part, start["args"]), f"{part} not in {start['args']}")
        for name, value in (expect.get("env") or {}).items():
            self.assertEqual(start["env"].get(name), value.replace("$HOME", self.private_home()), name)
        for name in expect.get("unset") or []:
            self.assertNotIn(name, start["env"])
        for wanted in expect.get("requests") or []:
            self.assertTrue(any(contains(wanted, m) for m in self.received()),
                            f"no {json.dumps(wanted)} in {json.dumps(self.received())[:3000]}")
        for path, texts in (expect.get("files") or {}).items():
            with open(os.path.join(self.private_home(), path), encoding="utf-8") as f:
                content = f.read()
            for text in texts if isinstance(texts, list) else [texts]:
                self.assertIn(text, content, path)

    def test_every_setting_is_tested(self):
        settings = [s.get("given") or {} for s in self.recording.get("settings") or []]
        facts = adapters.AGENTS[self.kind]
        spec = adapters.KINDS[self.kind].spec
        self.assertEqual(sorted({s.get("permission", "ask") for s in settings}), sorted(facts["permissions"]))
        self.assertEqual(sorted({(s.get("credentials") or {}).get("way") for s in settings if s.get("credentials")}),
                         sorted(facts["logins"]))
        options = {name for s in settings for name in (s.get("options") or {})}
        self.assertEqual(sorted(options), sorted(spec.options))
        if spec.model is not None:
            self.assertTrue(any(s.get("model") for s in settings), "the model setting is tested")

    def test_what_moves_its_folders_is_taken_out_only_in_a_private_home(self):
        spec = adapters.KINDS[self.kind].spec
        if not spec.home_unset:
            return
        os.environ.update({name: f"/mine/{name}" for name in spec.home_unset})
        login = next(s["given"] for s in self.recording["settings"]
                     if (s.get("given") or {}).get("credentials") and "error" not in s["expect"])
        # An agent that runs only on a connection's login always has a private
        # home: its machine's own login is refused before anything starts.
        cases = ([] if spec.machine_login else [({"credentials": None}, True)]) + [(login, False)]
        for given, kept in cases:
            with self.subTest(credentials=given.get("credentials")):
                self.given(given)
                ended = self.server.until(self.turn("hello"), ev.DONE, ev.FAILED)
                self.assertEqual(ended["status"], ev.DONE, ended["error"])
                env = self.starts()[-1]["env"]
                for name in spec.home_unset:
                    self.assertEqual(env.get(name), f"/mine/{name}" if kept else None, name)

    def test_a_call_of_the_programs_tool_is_told_apart(self):
        spec = adapters.KINDS[self.kind].spec
        if "tool" in self.recording["turns"]:
            self.assertIsNotNone(spec.program_tool, "it shows a call of the program's tool: say how it names it")

    def test_an_option_it_does_not_take_is_refused(self):
        self.given({"options": {"noSuchOption": "x"}})
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("has no option noSuchOption", failed["error"])

    def test_a_restart_picks_the_conversation_up_again_and_its_replay_is_no_turns(self):
        spec = adapters.KINDS[self.kind].spec
        self.given({})
        self.server.until(self.turn("hello"), ev.DONE)
        session = self.server.conversation.conversation_id()
        self.server.close()
        self.server = self.start()
        self.given({})
        again = self.server.until(self.turn("hello"), ev.DONE)
        self.assertEqual([e["type"] for e in again["seen"]], self.recording["turns"]["hello"]["expect"]["events"])
        if spec.resume is None:
            self.assertEqual([m for m in self.received() if m.get("method") in ("session/load", "session/resume")], [])
            return
        picked = [m for m in self.received() if m.get("method") == spec.resume]
        self.assertEqual(picked[0]["params"]["sessionId"], session)


class FormReading(unittest.TestCase):
    """The one reader of `elicitation/create` forms, under each `Form`."""

    FIELDS = {"pick": {"type": "string", "title": "Pick", "enum": ["a", "b"]},
              "pick_custom": {"type": "string", "title": "Pick (other)"},
              "sure": {"type": "boolean", "title": "Sure?"}}

    def read(self, form, answers, fields=None):
        question, result_of = adapter_acp.form_question("q", {"message": "Asked", "requestedSchema": {"properties": fields or self.FIELDS}}, form)
        return question, result_of({"answers": answers})

    def test_enum_and_boolean_read_under_every_form(self):
        for form in (adapter_acp.Form(), adapter_acp.Form(message="header", own_answer=adapter_acp.Sibling("__other")),
                     adapter_acp.Form(own_answer=adapter_acp.Sibling("_custom")), adapter_acp.Form(own_answer=adapter_acp.ANY_TEXT)):
            with self.subTest(message=form.message, own_answer=form.own_answer):
                question, result = self.read(form, {"pick": ["b"], "sure": ["Yes"], "pick_custom": ["b"]})
                asked = {q["id"]: q for q in question["questions"]}
                self.assertEqual([o["label"] for o in asked["pick"]["options"]], ["a", "b"])
                self.assertEqual([o["label"] for o in asked["sure"]["options"]], ["Yes", "No"])
                self.assertEqual(result["content"]["pick"], "b")
                self.assertIs(result["content"]["sure"], True)

    def test_a_typed_answer_goes_in_its_sibling_field(self):
        form = adapter_acp.Form(own_answer=adapter_acp.Sibling("_custom"))
        question, result = self.read(form, {"pick": ["c"], "sure": ["No"]})
        self.assertEqual([q["id"] for q in question["questions"]], ["pick", "sure"])
        self.assertTrue(question["questions"][0]["allowOther"])
        self.assertEqual(result["content"], {"pick_custom": "c", "sure": False})

    def test_a_marked_sibling_for_no_question_is_refused(self):
        form = adapter_acp.Form(own_answer=adapter_acp.Sibling("_custom", marker="own"))
        fields = {"lost_custom": {"type": "string", "_meta": {"own": True}}}
        with self.assertRaises(adapter_acp.Refused):
            self.read(form, {}, fields)


class ProgramToolReading(unittest.TestCase):
    """`ProgramTool` reads the bare name where an agent writes it, and a name
    counts only when it is exactly one of the program's."""

    def test_a_name_in_the_set_is_read_and_any_other_is_not(self):
        tool = adapter_acp.ProgramTool(r"mcp__{server}__(?P<name>.+)")
        self.assertEqual(tool.read({"title": "mcp__weft__lookup"}, {"lookup"}), "lookup")
        self.assertIsNone(tool.read({"title": "mcp__weft__other"}, {"lookup"}))
        self.assertIsNone(tool.read({"title": "mcp__elsewhere__lookup"}, {"lookup"}))
        self.assertIsNone(tool.read({"title": "lookup"}, {"lookup"}))

    def test_it_reads_where_the_agent_names_the_route(self):
        goose = adapters.KINDS["goose"].spec.program_tool
        update = {"title": "weft: lookup", "_meta": {"goose": {"toolCall": {"toolName": "weft__lookup", "extensionName": "weft"}}}}
        self.assertEqual(goose.read(update, {"lookup"}), "lookup")
        self.assertIsNone(goose.read({"title": "weft: lookup", "_meta": {"goose": {"toolCall": {"toolName": "shell"}}}}, {"lookup"}))


class Remembered:
    """A conversation that keeps what an adapter writes into the history."""

    def __init__(self, home):
        self.home = home
        self.history = []

    def remember(self, turn, message):
        self.history.append(message)

    def emit(self, turn, kind, **fields):
        return None


class TheHistoryLeavesTheProgramsCallsToTheRoute(unittest.TestCase):
    """An ACP agent's echo of a call of the program's tools is not written by
    the adapter (the program's route writes it, under its plain name); a call
    of the agent's own tools is, even when its title is a program tool's name."""

    def history(self, kind, updates):
        home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, home, True)
        conv = Remembered(home)
        adapter = adapters.KINDS[kind](conv, {"agent": kind, "folder": home, "permission": "ask", "options": {}}, LOOKUP)
        for update in updates:
            adapter._update(object(), update)
        return conv.history

    def call(self, call_id, title, **more):
        return [{"sessionUpdate": "tool_call", "toolCallId": call_id, "title": title, "rawInput": {}, **more},
                {"sessionUpdate": "tool_call_update", "toolCallId": call_id, "status": "completed",
                 "content": [{"type": "content", "content": {"type": "text", "text": "out"}}]}]

    def test_the_programs_call_is_left_out_and_the_agents_own_is_kept(self):
        history = self.history("minimax", self.call("a", "mcp__weft__lookup") + self.call("b", "read"))
        self.assertEqual([c["name"] for m in history for c in m.get("tool_calls") or []], ["read"])
        self.assertEqual([m["tool_call_id"] for m in history if m["role"] == "tool"], ["b"])

    def test_a_title_alone_proves_nothing(self):
        # Gemini titles the program's call `lookup (weft MCP Server)`: its own
        # call titled `lookup` is its own.
        history = self.history("gemini", self.call("a", "lookup"))
        self.assertEqual([c["name"] for m in history for c in m.get("tool_calls") or []], ["lookup"])

    def test_where_the_agent_says_the_route_it_is_read_there(self):
        routed = self.call("a", "weft: lookup", _meta={"goose": {"toolCall": {"toolName": "weft__lookup", "extensionName": "weft"}}})
        own = self.call("b", "weft: lookup", _meta={"goose": {"toolCall": {"toolName": "shell"}}})
        history = self.history("goose", routed + own)
        self.assertEqual([c["id"] for m in history for c in m.get("tool_calls") or []], ["b"])


class ARepeatedCallIsOneCall(unittest.TestCase):
    """A second `tool_call` with an id already shown (CodeBuddy sends one
    once the arguments are complete) updates that call: it is shown and
    written once."""

    def test_it_is_written_once(self):
        home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, home, True)
        conv = Remembered(home)
        emitted = []
        conv.emit = lambda turn, kind, **fields: emitted.append(kind)
        adapter = adapters.KINDS["minimax"](conv, {"agent": "minimax", "folder": home, "permission": "ask", "options": {}}, LOOKUP)
        for update in ({"sessionUpdate": "tool_call", "toolCallId": "a", "title": "read", "rawInput": {}},
                       {"sessionUpdate": "tool_call", "toolCallId": "a", "title": "read", "rawInput": {"path": "x"}}):
            adapter._update(object(), update)
        self.assertEqual(emitted, [ev.TOOL_USE])
        self.assertEqual(len([m for m in conv.history if m.get("tool_calls")]), 1)


class FilesOnTheMachinesOwnLogin(Rig):
    """With no private home (the machine's own login), the files a spec
    writes before each start go into a folder of weft's own."""

    def test_qwens_tools_are_written_in_wefts_folder(self):
        self.configure("qwen")
        self.server.until(self.turn("hello", tools=LOOKUP), ev.DONE)
        folder = os.path.join(self.home, "qwen-files", "weft-tools")
        start = [m["start"] for m in self.logged() if "start" in m][-1]
        self.assertEqual(start["env"]["QWEN_CODE_SYSTEM_SETTINGS_PATH"], os.path.join(folder, "settings.json"))
        with open(os.path.join(folder, "tools.json"), encoding="utf-8") as f:
            self.assertEqual([t["name"] for t in json.load(f)["tools"]], ["lookup"])
        self.assertTrue(os.access(os.path.join(folder, "call"), os.X_OK))
        self.assertEqual(os.stat(os.path.join(folder, "tools.json")).st_mode & 0o777, 0o600)

    def test_dshs_patch_is_named_on_its_command_line(self):
        self.configure("dsh")
        self.server.until(self.turn("hello", tools=LOOKUP), ev.DONE)
        start = [m["start"] for m in self.logged() if "start" in m][-1]
        patch = start["args"][start["args"].index("--patch") + 1]
        self.assertEqual(patch, os.path.join(self.home, "dsh-files", "weft-tools", "weft.patch.yml"))
        with open(patch, encoding="utf-8") as f:
            self.assertEqual(json.load(f)[0]["insert"][0]["config"]["url"], adapters.mcp_url())


class HermesRefusesWithoutItsPlugin(Rig):
    """The session is checked once made (`AcpSpec.session_check`): a Hermes
    that did not load weft's plugin fails the turn before its first prompt."""

    def test_no_prompt_is_sent(self):
        import acp_hermes  # noqa: PLC0415
        # Where the stand-in writes nothing: the plugin never said it loaded.
        self.addCleanup(setattr, acp_hermes, "PLUGIN_LOADED", acp_hermes.PLUGIN_LOADED)
        acp_hermes.PLUGIN_LOADED = "plugins/weft/never.json"
        self.given_hermes()
        failed = self.server.until(self.turn("tool", tools=LOOKUP), ev.FAILED)
        self.assertIn("did not load weft's plugin", failed["error"])
        self.assertNotIn("session/prompt", [m.get("method") for m in self.logged()])

    def given_hermes(self):
        self.configure("hermes", **recordings()["hermes"]["given"])


class ProgramToolsListing(unittest.TestCase):
    def test_the_listing_holds_the_route_and_each_tool(self):
        import program_tools  # noqa: PLC0415
        listed = json.loads(program_tools.listing("http://route", LOOKUP))
        self.assertEqual(listed["url"], "http://route")
        self.assertEqual([t["name"] for t in listed["tools"]], ["lookup"])


class DshKeepsItsPermission(Rig):
    def test_a_conversation_picked_up_under_another_permission_is_refused(self):
        self.configure("dsh", permission="ask")
        self.server.until(self.turn("hello"), ev.DONE)
        self.server.close()
        self.server = self.start()
        self.configure("dsh", permission="read_only")
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("keeps the permission setting a conversation started with (ask)", failed["error"])
        self.assertNotIn("session/resume", [m.get("method") for m in self.logged()])

    def test_a_conversation_kept_before_its_permission_was_kept_says_to_start_over(self):
        self.configure("dsh", permission="ask")
        self.server.until(self.turn("hello"), ev.DONE)
        self.server.close()
        self.server = self.start()
        conv = self.server.conversation
        with conv.lock:
            del conv.state["kept"]["started"]["permission"]
        self.configure("dsh", permission="ask")
        failed = self.server.until(self.turn("hello"), ev.FAILED)
        self.assertIn("began before weft kept the settings", failed["error"])
        self.assertIn("start the conversation over", failed["error"])


class EveryAcpAgentIsRecorded(unittest.TestCase):
    def test_every_acp_kind_has_a_recording_or_says_why_not(self):
        acp_kinds = {kind for kind, adapter in adapters.KINDS.items() if issubclass(adapter, adapter_acp.Acp)}
        recorded = set(recordings())
        self.assertEqual(sorted(acp_kinds - recorded), sorted(NOT_RECORDED), "an ACP agent with no recording")
        self.assertEqual(sorted(recorded & set(NOT_RECORDED)), [], "recorded now: take it out of NOT_RECORDED")


def turn_test(name: str):
    return lambda self: self.play(name)


def setting_test(setting: dict):
    return lambda self: self.check_setting(setting)


for kind, recording in recordings().items():
    methods = {"kind": kind, "recording": recording}
    for name in recording["turns"]:
        methods[f"test_turn_{name}"] = turn_test(name)
    for index, setting in enumerate(recording.get("settings") or []):
        methods[f"test_setting_{index}_{setting['name']}"] = setting_test(setting)
    globals()[f"Acp_{kind}_Test"] = type(f"Acp_{kind}_Test", (AcpAgent,), methods)

del AcpAgent


if __name__ == "__main__":
    unittest.main()
