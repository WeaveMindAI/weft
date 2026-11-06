"""OpenCode 2 (`opencode acp`), as an `AcpSpec`.

Its quirks, and what the spec does about each:

  - Out of the box it asks for nothing (every action is allowed but reading
    a `.env`), so the permission settings are rules handed in
    `OPENCODE_CONFIG_CONTENT`: an ordered list where the last rule that
    matches wins. That variable is read after the folder's own
    `opencode.json`, so a project that allows `shell` still asks.
  - The same binary name ships OpenCode 1, whose ACP reads none of those
    rules (it would run with everything allowed) and whose question tool
    hangs the turn. OpenCode 2 is told apart by the `additionalDirectories`
    session capability, and anything else is refused.
  - Its question tool arrives as `elicitation/create` once `initialize`
    declares `elicitation.form`: each question is a field `q<N>`, and when
    the person may type their own answer a second field `q<N>_custom`
    carries it (it wins over a pick for one choice, and is added to the
    picks for several), folded into the first as the answer the person
    types (`Form`'s `Sibling`).
  - A refused permission ends the whole turn (`stopReason: cancelled`).
  - It reads Claude Code's folder (`~/.claude`: instructions, skills,
    agents) as config of its own; `OPENCODE_DISABLE_CLAUDE_CODE` turns
    that off.
  - With a connection's login it runs in a private home: `HOME` is the
    home and the `XDG_*` folders are taken out so they follow it, so its
    config, logins, providers, MCP servers and sessions are the
    connection's alone, never the person's own `~/.config/opencode` and
    `~/.local/share/opencode`.
  - The program's tools are a plugin of weft's own (`opencode_weft.mjs`),
    a folder named in `OPENCODE_CONFIG_CONTENT`'s `plugins`. Each is added
    with no namespace and `codemode: false`, so it is on the model's own
    tool list under its plain name (an MCP server's would be reached only
    from the code its `execute` tool runs, as `tools.weft.<tool>`), and a
    user plugin's tools are added after the built-ins, so a program tool
    named like one (`read`, `shell`, ...) is the one the model calls. Its
    call shows as a `tool_call` whose `name` is the tool's. `execute` is
    the one name OpenCode keeps for itself (`reservedToolNames`).
"""

import json
import os

import adapter_acp as acp
import js_plugin
from adapter_acp import AcpSpec, Setup

# The actions a person is asked about under `ask`; everything else (reading,
# searching, skills, subagents, its question tool, the program's tools) stays
# allowed.
ASKED = ("shell", "edit", "webfetch", "websearch", "external_directory")


# The config OpenCode reads last: the permission rules, and weft's plugin.
CONFIG = "OPENCODE_CONFIG_CONTENT"


def rules(*rules_: tuple) -> dict:
    """The permission rules, as the variable OpenCode reads its config from."""
    permissions = [{"action": action, "resource": "*", "effect": effect} for action, effect in rules_]
    return {CONFIG: json.dumps({"permissions": permissions})}


PLUGIN = js_plugin.plugin_file("opencode_weft.mjs")
# The plugin folder, in weft's own folder for the agent: OpenCode loads a
# local plugin only as a folder, from its `index` file.
PLUGIN_FOLDER = "weft-opencode-plugin"


def opencode_plugin(adapter) -> Setup:
    """The program's tools: weft's plugin folder (in the private home, or the
    conversation's folder on the machine's own login), added to the config
    the permission setting hands over, the one variable both live in."""
    folder = os.path.join(adapter.home if adapter.home is not None else adapter.conv.home, PLUGIN_FOLDER)
    js_plugin.write_loader(os.path.join(folder, "index.mjs"), PLUGIN, adapter.tools)
    config = json.loads(adapter.spec.permissions[adapter.permission].env[CONFIG])
    return Setup(env={CONFIG: json.dumps({**config, "plugins": [js_plugin.uri(folder)]})})


ASK = tuple((action, "ask") for action in ASKED)
# Under `plan` its plan agent refuses edits outside its plan folder, and a
# rule of ours for `edit` would win over that refusal, so edits are left out.
PLAN = tuple((action, "ask") for action in ASKED if action != "edit")


def opencode_2(adapter):
    """OpenCode 1, which this spec cannot drive, told apart from OpenCode 2."""
    if "additionalDirectories" not in (adapter.capabilities.get("sessionCapabilities") or {}):
        return ("the `opencode` on PATH is OpenCode 1, and weft drives OpenCode 2 (OpenCode 1 would run with every "
                "action allowed whatever the permission setting says). weft installs OpenCode 2 itself only when no "
                "`opencode` is on PATH, so take this one off PATH and start the node's infra again, point the node's "
                "`program` at an OpenCode 2, or put OpenCode 2 in its place with "
                "`curl -fsSL https://opencode.ai/v2/install | bash`; then run the turn again")
    return None


acp.register(AcpSpec(
    "opencode", ["opencode", "acp"],
    permissions={
        "ask": Setup(env=rules(*ASK)),
        "accept_edits": Setup(env=rules(*ASK, ("edit", "allow"))),
        "plan": Setup(env=rules(*PLAN), config={"mode": "plan"}),
        "bypass": Setup(env=rules(("*", "allow"))),
    },
    model=acp.config_option("model", description="The model as `<provider>/<model>`, e.g. `anthropic/claude-sonnet-4-6`"),
    options={
        "effort": acp.config_option("effort", ["minimal", "low", "medium", "high", "xhigh", "max", "default"],
                                    description="How hard the model thinks, on the models that take it"),
        "addDirs": acp.Lever(acp.Lever.LIST, lambda dirs: Setup(session={"additionalDirectories": dirs}),
                             description="More folders it may read and change"),
    },
    env={"OPENCODE_DISABLE_CLAUDE_CODE": "1", "OPENCODE_DISABLE_AUTOUPDATE": "1"},
    # OpenCode finds its config, logins and sessions through these, under
    # `HOME`: in a private home they follow it, on the machine's login they stay.
    home_unset=["XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME"],
    initialize={"clientCapabilities": {"elicitation": {"form": {}}}},
    agent_refusal=opencode_2,
    form=acp.Form(own_answer=acp.Sibling("_custom")),
    reject_ends_turn=True,
    mcp=opencode_plugin,
    program_tool=acp.ProgramTool(r"(?P<name>.+)", at=("name",)),
))
