"""What holds across every adapter: agents.json, the adapters and the weft
nodes (`catalog/agents/<node>/`) agree on each agent, and
`catalog/agents/agent.rs` ships exactly the files the server runs on.

Run with the others: `python3 test_agent_server.py`.
"""

import glob
import json
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import adapters  # noqa: E402
from events import Refused  # noqa: E402

NODES = os.path.dirname(HERE)
AGENT_RS = os.path.join(NODES, "agent.rs")
# What is in the folder for the tests alone: never shipped.
TEST_ONLY = ("testing", "__pycache__")

# A login value an adapter hands the agent itself: `login_value("key")`, or
# by a constant of its module (`login_value(LICENCE_VALUE)`).
LOGIN_VALUE = re.compile(r"login_value\(\s*(?:\"([^\"]+)\"|([A-Z_]+))\s*\)")
# A login value read around `login_value`, from the connection itself.
RAW_VALUES = re.compile(r"credentials[^\n]*values|\[\"values\"\]")


def stored_values(way: dict) -> list:
    """The values a connection made with `way` (an access node's) hands a
    node: its fields, or for a way weft renews, the token and what the
    paste captured (the pasted document itself is not stored)."""
    renewal = way.get("renewal")
    if renewal is None:
        return [f["name"] for f in way["fields"]]
    return ["token", *(c["name"] for c in renewal["paste"].get("captures") or [])]


def shipped() -> dict:
    """`SERVER_FILES` of agent.rs: the name each file is written as -> the file it includes."""
    with open(AGENT_RS, encoding="utf-8") as f:
        source = f.read()
    start = source.index("pub const SERVER_FILES")
    listed = source[start:source.index("];", start)]
    return dict(re.findall(r"\(\"([^\"]+)\", include_str!\(\"server/([^\"]+)\"\)\)", listed))


def runtime_files() -> set:
    """Every file of the server's folder but its tests."""
    return {name for name in os.listdir(HERE)
            if name not in TEST_ONLY and not name.startswith("test_") and os.path.isfile(os.path.join(HERE, name))}


def common_inputs() -> set:
    """`COMMON_INPUTS` of agent.rs: the settings every agent node has, which are never options."""
    with open(AGENT_RS, encoding="utf-8") as f:
        source = f.read()
    start = source.index("const COMMON_INPUTS")
    return set(re.findall(r"\"([^\"]+)\"", source[start:source.index("];", start)]))


def nodes() -> dict:
    """Kind -> the folder of the agent node that runs it (its `mod.rs` names the kind as `KIND`)."""
    found = {}
    for path in glob.glob(os.path.join(NODES, "*", "mod.rs")):
        with open(path, encoding="utf-8") as f:
            kind = re.search(r"const KIND: &str = \"([a-z_]+)\"", f.read())
        if kind:
            found[kind.group(1)] = os.path.dirname(path)
    return found


def metadata(folder: str) -> dict:
    with open(os.path.join(folder, "metadata.json"), encoding="utf-8") as f:
        return json.load(f)


def module_of(kind: str):
    """The module an agent's adapter is written in: `adapter_<kind>`, or `acp_<kind>` for an ACP agent."""
    module = sys.modules.get(f"adapter_{kind}") or sys.modules.get(f"acp_{kind}")
    if module is None:
        raise AssertionError(f"no adapter_{kind}.py or acp_{kind}.py is imported for {kind!r}")
    return module


class ServerFilesTest(unittest.TestCase):
    def test_agent_rs_ships_exactly_the_files_the_server_runs_on(self):
        files = shipped()
        self.assertEqual({name: path for name, path in files.items() if name != path}, {},
                         "each file is written under its own name")
        missing = sorted(runtime_files() - set(files))
        extra = sorted(set(files) - runtime_files())
        self.assertEqual(missing, [], "files the server runs on that SERVER_FILES in agent.rs leaves out")
        self.assertEqual(extra, [], "files SERVER_FILES in agent.rs lists that are not the server's (gone, or a test's)")


class AgentsAgreeTest(unittest.TestCase):
    def test_every_kind_in_agents_json_has_an_adapter_and_every_adapter_an_entry(self):
        self.assertEqual(sorted(set(adapters.AGENTS) - set(adapters.KINDS)), [], "kinds no adapter registers")
        self.assertEqual(sorted(set(adapters.KINDS) - set(adapters.AGENTS)), [], "adapters agents.json has no entry for")

    def test_every_adapter_carries_out_exactly_the_permission_settings_agents_json_lists(self):
        for kind, adapter in adapters.KINDS.items():
            self.assertEqual(list(adapter.permissions), adapters.AGENTS[kind]["permissions"], kind)

    def test_every_install_hint_says_how_to_install_it_by_hand(self):
        for kind, facts in adapters.AGENTS.items():
            self.assertIn(adapters.BY_HAND, facts["installHint"], f"{kind}: a missing program's refusal hands on what follows it")

    def test_every_adapter_starts_the_program_its_check_runs(self):
        # The node checks `check` with the person's own `program` in place
        # of its first word, the program the adapter starts.
        for kind, adapter in adapters.KINDS.items():
            self.assertEqual(adapter.program_name, adapters.AGENTS[kind]["check"][0], kind)

    def test_every_login_value_an_adapter_hands_over_itself_is_one_agents_json_names(self):
        for kind, facts in adapters.AGENTS.items():
            module = module_of(kind)
            with open(module.__file__, encoding="utf-8") as f:
                source = f.read()
            self.assertIsNone(RAW_VALUES.search(source), f"{kind} reads the connection's values around `login_value`")
            read = {literal or getattr(module, constant) for literal, constant in LOGIN_VALUE.findall(source)}
            named = {value for way in facts["logins"].values() for value in [*way["env"], *way.get("protocol", [])]}
            self.assertEqual(sorted(read - named), [], f"{kind} reads login values agents.json does not name")
            protocol = {value for way in facts["logins"].values() for value in way.get("protocol", [])}
            self.assertEqual(sorted(protocol - read), [], f"{kind}: agents.json's `protocol` values its adapter never hands over")

    def test_every_agent_has_a_node_whose_own_settings_are_exactly_its_adapters_options(self):
        found, common = nodes(), common_inputs()
        self.assertEqual(sorted(set(adapters.AGENTS) - set(found)), [], "agents no node runs")
        for kind, folder in found.items():
            own = {i["name"] for i in metadata(folder)["inputs"]} - common
            self.assertEqual(sorted(own), sorted(adapters.KINDS[kind].option_names), f"{kind}: {os.path.basename(folder)}'s inputs")

    def test_every_access_node_offers_exactly_the_login_ways_and_values_agents_json_names(self):
        for kind, folder in nodes().items():
            access = folder + "_access"
            self.assertTrue(os.path.isdir(access), f"{kind} has no {os.path.basename(access)}")
            ways = metadata(access)["service"]["acquisition"]["ways"]
            offered = {way["id"]: sorted(stored_values(way)) for way in ways}
            named = {way_id: sorted([*way["env"], *way.get("protocol", [])])
                     for way_id, way in adapters.AGENTS[kind]["logins"].items()}
            self.assertEqual(offered, named, f"{kind}: {os.path.basename(access)}'s ways and their fields")

    def test_every_adapter_refuses_an_option_it_does_not_take_before_anything_starts(self):
        for kind, adapter in adapters.KINDS.items():
            config = {"agent": kind, "folder": "/w", "permission": adapters.AGENTS[kind]["permissions"][0],
                      "options": {"noSuchOption": 1}}
            with self.assertRaises(Refused) as refused:
                adapter(None, config, [])
            self.assertIn(f"{adapters.AGENTS[kind]['label']} has no option noSuchOption; it takes ", str(refused.exception), kind)

    def test_every_login_way_hands_over_something_and_only_the_keys_the_node_reads(self):
        # SYNC: the shape of a login way <-> catalog/agents/agent.rs (LoginFacts)
        for kind, facts in adapters.AGENTS.items():
            for way_id, way in facts["logins"].items():
                self.assertLessEqual(set(way), {"env", "protocol"}, f"{kind}.{way_id}")
                self.assertTrue(way["env"] or way.get("protocol"), f"{kind}.{way_id} hands over nothing")


class ProgramToolsTest(unittest.TestCase):
    """The one test of whether a call is the program's: its exact name, never a part of it."""

    def test_a_plain_name_is_the_programs_exactly(self):
        tools = adapters.ProgramTools([{"name": "lookup"}, {"name": "Read"}])
        self.assertEqual((tools.plain("lookup"), tools.plain("Read")), ("lookup", "Read"))
        for other in ("look", "lookup2", "mcp__weft__lookup", "read", ""):
            self.assertIsNone(tools.plain(other), other)
        self.assertIn("Read", tools)

    def test_an_agent_that_shows_its_own_spelling_is_matched_on_that_spelling_whole(self):
        tools = adapters.ProgramTools([{"name": "shell"}], lambda plain: f"mcp__weft__{plain}")
        self.assertEqual(tools.plain("mcp__weft__shell"), "shell")
        for other in ("shell", "mcp__weft__shell2", "mcp__other__shell"):
            self.assertIsNone(tools.plain(other), f"{other}: the agent's own, or another server's")


class McpResultTest(unittest.TestCase):
    def test_structured_content_is_only_ever_a_json_object_and_the_text_is_always_there(self):
        for output in ("the number is 42", "", "{not json"):
            answer = adapters.mcp_result({"output": output})
            self.assertEqual(answer["content"], [{"type": "text", "text": output}], output)
            self.assertNotIn("structuredContent", answer, output)
        # Any other JSON value, wrapped: Gemini CLI would otherwise parse the
        # text into `structuredContent` itself and fail the call on it.
        for output, value in (("42", 42), ('"text"', "text"), ("[1, 2]", [1, 2]), ("null", None)):
            answer = adapters.mcp_result({"output": output})
            self.assertEqual(answer["content"], [{"type": "text", "text": output}], output)
            self.assertEqual(answer["structuredContent"], {"result": value}, output)
        answer = adapters.mcp_result({"output": '{"n": 42}', "isError": True})
        self.assertEqual((answer["structuredContent"], answer["isError"]), ({"n": 42}, True))
        self.assertEqual(answer["content"], [{"type": "text", "text": '{"n": 42}'}])


class ReservedToolNamesTest(unittest.TestCase):
    """The names a node refuses when the program is built (`lists.reservedToolNames`)."""

    def test_claude_code_keeps_only_tool_search_and_codex_none_it_can_turn_off(self):
        import adapter_codex  # noqa: PLC0415

        def reserved(node):
            with open(os.path.join(NODES, node, "metadata.json"), encoding="utf-8") as f:
                return json.load(f)["lists"]["reservedToolNames"]

        self.assertEqual(reserved("claude_code"), ["ToolSearch"])
        codex = reserved("codex")
        self.assertEqual(set(codex) & set(adapter_codex.REPLACEABLE), set(), "a name Codex can give up is never refused")
        self.assertIn("exec_command", codex)
        self.assertTrue({"write_stdin", "create_goal", "get_goal", "update_goal", "request_plugin_install",
                         "list_available_plugins_to_install"} <= set(codex),
                        "a built-in whose only switch turns off others with it is refused")
        self.assertEqual(adapter_codex.thread_config([{"name": "view_image"}]),
                         dict(adapter_codex.COLLISIONS_FAIL, **{"features.view_image": False}), "one tool, its own switch")
        self.assertEqual(adapter_codex.thread_config([{"name": n} for n in codex]), adapter_codex.COLLISIONS_FAIL,
                         "a reserved name never switches anything off")
        muse = reserved("muse_code")
        self.assertTrue({"bash", "read_file", "write_file", "request_user_input"} <= set(muse))
        self.assertEqual(len(muse), len(set(muse)))


if __name__ == "__main__":
    unittest.main()
